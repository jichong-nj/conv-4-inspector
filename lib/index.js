// conv-4-inspector — host half.
//
// Captures every outbound LLM request and its assembled response by listening
// on the `llm/stream` waterfall, which is dispatched with the exact
// `GenerateOptions` the adapter is about to send. Captures live in a bounded
// in-memory ring buffer and are served to the browser half over a prefix HTTP
// route registered on `ctx.webServer`.
//
// Contract notes (installed npm plugin, NOT a dynamic cordis package):
//   - top-level `name` / `inject` / `apply` exports; no `harness` global.
//   - `options` is deep-frozen for loop-built requests: read only, never mutate.
//   - the response is a `StreamChunk` stream (SSE-like deltas); we accumulate
//     the deltas and persist only the final assembled blocks.

const name = "conv-4-inspector";
const inject = ["llm", "webServer"];

/** Ring-buffer limits. */
const MAX_RECORDS = 60;
const MAX_TOTAL_BYTES = 24 * 1024 * 1024;
/** Per-string truncation budget for request/response payloads. */
const MAX_STRING = 8000;
/** Per-string budget when building list previews. */
const MAX_PREVIEW = 220;
/** Raw text is kept for only the most recent requests; a single request can carry the whole system prompt and tool catalogue. */
const MAX_RAW_RECORDS = 16;
/** Per-request ceiling for one request's raw text (request payload plus stream events). */
const MAX_RAW_BYTES = 512 * 1024;

const API_PREFIX = "/conv-inspector/api";

// ── storage ──────────────────────────────────────────────────────────────────

const records = [];
let totalBytes = 0;
let counter = 0;

function sizeOf(value) {
	try {
		return JSON.stringify(value).length;
	} catch {
		return 0;
	}
}

function evict() {
	while (records.length > MAX_RECORDS || totalBytes > MAX_TOTAL_BYTES) {
		const oldest = records.shift();
		if (oldest === undefined) break;
		totalBytes -= oldest.bytes;
	}
}

function commit(record) {
	record.bytes = sizeOf(record);
	records.push(record);
	totalBytes += record.bytes;
	evict();
}

// ── raw capture ──────────────────────────────────────────────────────────────
// Structured records stay small and long-lived; raw text is far bulkier, so it
// lives in its own short ring keyed by record id and is dropped independently.
// The client fetches it lazily, only when a section is expanded.

const raws = new Map();
const rawOrder = [];

function rawOpen(id, requestText) {
	const entry = {
		id,
		requestText,
		requestBytes: requestText.length,
		events: [],
		eventBytes: 0,
		dropped: 0,
		truncated: false,
	};
	raws.set(id, entry);
	rawOrder.push(id);
	while (rawOrder.length > MAX_RAW_RECORDS) {
		const oldest = rawOrder.shift();
		if (oldest !== id) raws.delete(oldest);
	}
	return entry;
}

/** Append one stream event while the per-request budget lasts. */
function rawPush(entry, chunk) {
	if (entry.truncated) {
		entry.dropped += 1;
		return;
	}
	const size = sizeOf(chunk) + 1;
	if (entry.requestBytes + entry.eventBytes + size > MAX_RAW_BYTES) {
		entry.truncated = true;
		entry.dropped += 1;
		return;
	}
	entry.eventBytes += size;
	entry.events.push(chunk);
}

/**
 * Faithful serialization of the object handed to the adapter. Unlike the
 * structured snapshot this keeps full strings; only the abort signal is
 * replaced, since it is a live handle rather than payload.
 */
function rawRequestText(options) {
	try {
		return JSON.stringify(options, (key, value) => (key === "signal" ? "[AbortSignal]" : value)) ?? String(options);
	} catch (error) {
		return `# request not serializable: ${String(error?.message ?? error)}`;
	}
}

// ── capture switch ───────────────────────────────────────────────────────────

/**
 * Sessions with capture switched on. Opt-in and in-memory: nothing is recorded
 * until a session is enabled, and a restart returns everything to off.
 */
const enabled = new Set();

function sessionKey(options) {
	const sessionId = options?.sessionId;
	return sessionId === undefined || sessionId === null ? "" : String(sessionId);
}

function captureEnabled(options) {
	const key = sessionKey(options);
	return key !== "" && enabled.has(key);
}

// ── payload shrinking ────────────────────────────────────────────────────────

function clip(text, limit) {
	if (typeof text !== "string") return text;
	if (text.length <= limit) return text;
	return `${text.slice(0, limit)}\n…[truncated ${text.length - limit} chars]`;
}

/**
 * Deep-copy a value into a JSON-safe shape, clipping long strings and dropping
 * functions / symbols / bigints. Depth- and breadth-bounded so a pathological
 * payload can never blow up the buffer.
 */
function shrink(value, depth = 0) {
	if (depth > 14) return "[max-depth]";
	if (value === null || value === undefined) return null;
	const kind = typeof value;
	if (kind === "string") return clip(value, MAX_STRING);
	if (kind === "number" || kind === "boolean") return value;
	if (kind === "bigint") return String(value);
	if (kind === "function" || kind === "symbol") return undefined;
	if (Array.isArray(value)) {
		const out = [];
		for (let i = 0; i < value.length && i < 400; i++) out.push(shrink(value[i], depth + 1));
		if (value.length > 400) out.push(`…[${value.length - 400} more items]`);
		return out;
	}
	const out = {};
	for (const key of Object.keys(value)) {
		if (key === "signal") continue; // AbortSignal: not serializable, not useful
		const child = shrink(value[key], depth + 1);
		if (child !== undefined) out[key] = child;
	}
	return out;
}

// ── request snapshot ─────────────────────────────────────────────────────────

function snapshotRequest(options) {
	const messages = Array.isArray(options.messages) ? options.messages : [];
	const tools = Array.isArray(options.tools) ? options.tools : [];
	return {
		provider: options.provider ?? null,
		model: options.model ?? null,
		purpose: options.purpose ?? null,
		sessionId: options.sessionId ?? null,
		reasoningEffort: options.reasoningEffort ?? null,
		temperature: options.temperature ?? null,
		maxTokens: options.maxTokens ?? null,
		stop: shrink(options.stop ?? null),
		system: typeof options.system === "string" ? clip(options.system, MAX_STRING * 4) : null,
		messageCount: messages.length,
		toolCount: tools.length,
		tools: tools.map((tool) => ({
			name: tool?.name ?? null,
			description: clip(tool?.description ?? "", 600),
			parameters: shrink(tool?.parameters ?? null),
		})),
		messages: shrink(messages),
	};
}

// ── response assembly ────────────────────────────────────────────────────────

/**
 * Accumulate `StreamChunk` deltas into the final blocks. Only the assembled
 * result is persisted — the raw delta stream is never retained.
 */
function createAssembler() {
	/** @type {Map<number, {index:number,type:string,text:string,id?:string,name?:string,arguments?:string}>} */
	const blocks = new Map();

	function slot(index, type) {
		const key = index ?? 0;
		let block = blocks.get(key);
		if (block === undefined) {
			block = { index: key, type, text: "" };
			blocks.set(key, block);
		}
		if (type === "tool-call") {
			block.type = "tool-call";
			if (block.arguments === undefined) block.arguments = "";
		}
		return block;
	}

	return {
		push(chunk) {
			if (chunk === null || typeof chunk !== "object" || typeof chunk.type !== "string") return;
			switch (chunk.type) {
				case "block-start":
					slot(chunk.index, String(chunk.blockType ?? "text"));
					break;
				case "text-delta": {
					const block = slot(chunk.index, "text");
					if (block.text.length < MAX_STRING * 4) block.text += chunk.text ?? "";
					break;
				}
				case "reasoning-delta": {
					const block = slot(chunk.index, "reasoning");
					if (block.text.length < MAX_STRING * 4) block.text += chunk.text ?? "";
					break;
				}
				case "tool-call-delta": {
					const block = slot(chunk.index, "tool-call");
					if (chunk.id) block.id = chunk.id;
					if (chunk.name) block.name = chunk.name;
					if (block.arguments.length < MAX_STRING * 4) block.arguments += chunk.argumentsDelta ?? "";
					break;
				}
				case "block-end": {
					// The provider hands the finished block here; prefer it over deltas.
					if (chunk.block !== undefined && chunk.block !== null) {
						const block = slot(chunk.index, String(chunk.block?.type ?? "text"));
						block.final = shrink(chunk.block);
					}
					break;
				}
				default:
					break;
			}
		},
		result() {
			const list = [...blocks.values()]
				.sort((a, b) => a.index - b.index)
				.map((block) => {
					const final = block.final;
					if (block.type === "tool-call") {
						return {
							type: "tool-call",
							id: block.id ?? final?.id ?? null,
							name: block.name ?? final?.name ?? null,
							// Prefer the accumulated deltas; fall back to the finished block
							// for providers that only emit `block-end`.
							arguments: clip(block.arguments || final?.arguments || "", MAX_STRING * 2),
						};
					}
					const text = block.final !== undefined ? shrink(block.final) : null;
					return {
						type: block.type === "reasoning" ? "reasoning" : "text",
						text: text?.text ?? clip(block.text, MAX_STRING * 2),
					};
				});
			return list;
		},
	};
}

// ── capture ──────────────────────────────────────────────────────────────────

function beginCapture(options) {
	counter += 1;
	const startedAt = Date.now();
	const record = {
		id: `req-${counter}`,
		seq: counter,
		startedAt,
		finishedAt: null,
		durationMs: null,
		done: false,
		error: null,
		usage: null,
		stopReason: null,
		chunkCount: 0,
		...snapshotRequest(options),
		blocks: [],
		bytes: 0,
	};
	// Non-enumerable: the structured record stays lean, the raw text lives in `raws`.
	Object.defineProperty(record, "rawRequest", { value: rawRequestText(options), enumerable: false, configurable: true });
	return record;
}

function observe(next, record) {
	const assembler = createAssembler();
	const raw = rawOpen(record.id, record.rawRequest ?? "");
	return (async function* captureStream() {
		try {
			for await (const chunk of next()) {
				record.chunkCount += 1;
				rawPush(raw, chunk);
				assembler.push(chunk);
				if (chunk?.type === "usage" && chunk.usage) record.usage = shrink(chunk.usage);
				if (chunk?.type === "finish") record.stopReason = chunk.reason ?? null;
				yield chunk;
			}
		} catch (error) {
			record.error = String(error?.message ?? error);
			throw error;
		} finally {
			record.finishedAt = Date.now();
			record.durationMs = record.finishedAt - record.startedAt;
			record.blocks = assembler.result();
			record.rawBytes = raw.requestBytes + raw.eventBytes;
			record.rawTruncated = raw.truncated;
			record.done = true;
			commit(record);
		}
	})();
}

// ── HTTP surface ─────────────────────────────────────────────────────────────

function writeJson(res, status, value) {
	const body = Buffer.from(JSON.stringify(value));
	res.writeHead(status, {
		"content-type": "application/json; charset=utf-8",
		"content-length": String(body.length),
		"cache-control": "no-store",
	});
	res.end(body);
}

function writeError(res, status, code, message) {
	writeJson(res, status, { ok: false, error: { code, message } });
}

async function readJsonBody(req) {
	const chunks = [];
	let size = 0;
	for await (const chunk of req) {
		size += chunk.length;
		if (size > 1024 * 1024) throw new Error("request body too large");
		chunks.push(chunk);
	}
	if (chunks.length === 0) return {};
	return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
}

function preview(text) {
	if (typeof text !== "string") return "";
	return text.replace(/\s+/g, " ").trim().slice(0, MAX_PREVIEW);
}

function firstText(blocks) {
	for (const block of blocks ?? []) if (block.type === "text") return block.text;
	return "";
}

/** Text of the last user-role message, reaching into its content blocks. */
function lastUserText(messages) {
	if (!Array.isArray(messages)) return "";
	for (let i = messages.length - 1; i >= 0; i--) {
		const content = messages[i]?.role === "user" ? messages[i].content : undefined;
		if (typeof content === "string") return content;
		if (Array.isArray(content)) {
			const text = content
				.filter((block) => block?.type === "text")
				.map((block) => block.text ?? "")
				.join(" ");
			if (text.trim().length > 0) return text;
		}
	}
	return "";
}

function summarize(record) {
	return {
		id: record.id,
		seq: record.seq,
		startedAt: record.startedAt,
		durationMs: record.durationMs,
		done: record.done,
		error: record.error,
		provider: record.provider,
		model: record.model,
		purpose: record.purpose,
		sessionId: record.sessionId,
		messageCount: record.messageCount,
		toolCount: record.toolCount,
		usage: record.usage,
		stopReason: record.stopReason,
		requestPreview: preview(lastUserText(record.messages)),
		responsePreview: preview(firstText(record.blocks)),
		rawAvailable: raws.has(record.id),
	};
}

const ENDPOINTS = {
	list(_ctx, payload) {
		const sessionFilter = payload?.sessionId;
		const items = records
			.filter((record) => !sessionFilter || record.sessionId === sessionFilter)
			.map(summarize)
			.reverse();
		const sessions = [...new Set(records.map((record) => record.sessionId).filter(Boolean))];
		return { items, total: records.length, sessions, enabled: [...enabled], limit: MAX_RECORDS };
	},
	capture(_ctx, payload) {
		const key = String(payload?.sessionId ?? "");
		if (payload?.action === "set") {
			if (key === "") throw new Error("缺少 sessionId");
			if (payload.enabled === true) enabled.add(key);
			else enabled.delete(key);
		}
		return { enabled: [...enabled], current: key === "" ? null : enabled.has(key) };
	},
	detail(_ctx, payload) {
		const record = records.find((item) => item.id === payload?.id);
		if (record === undefined) throw new Error(`no such capture: ${payload?.id}`);
		const { bytes, ...rest } = record;
		return rest;
	},
	raw(_ctx, payload) {
		const entry = raws.get(payload?.id);
		if (entry === undefined) {
			throw new Error(`原文未保留: ${payload?.id}（只缓存最近 ${MAX_RAW_RECORDS} 条）`);
		}
		return {
			id: entry.id,
			requestText: entry.requestText,
			responseText: JSON.stringify(entry.events) ?? "[]",
			eventCount: entry.events.length,
			dropped: entry.dropped,
			truncated: entry.truncated,
			requestBytes: entry.requestBytes,
			responseBytes: entry.eventBytes,
			limitBytes: MAX_RAW_BYTES,
		};
	},
	clear() {
		records.length = 0;
		totalBytes = 0;
		raws.clear();
		rawOrder.length = 0;
		return { cleared: true };
	},
};

function registerRoutes(ctx) {
	return ctx.webServer.register({
		kind: "prefix",
		path: API_PREFIX,
		handler: async (req, res) => {
			if (req.method !== "POST") {
				writeError(res, 405, "method-error", "method not allowed");
				return;
			}
			const pathname = new URL(req.url ?? "/", "http://dsh.internal").pathname;
			const endpoint = pathname.startsWith(`${API_PREFIX}/`) ? pathname.slice(API_PREFIX.length + 1) : undefined;
			const handler = endpoint === undefined ? undefined : ENDPOINTS[endpoint];
			if (handler === undefined) {
				writeError(res, 404, "unknown-endpoint", `unknown endpoint: ${endpoint ?? "(none)"}`);
				return;
			}
			let payload = {};
			try {
				payload = await readJsonBody(req);
			} catch (error) {
				writeError(res, 400, "bad-request", String(error?.message ?? error));
				return;
			}
			try {
				writeJson(res, 200, { ok: true, value: await handler(ctx, payload) });
			} catch (error) {
				writeError(res, 500, "handler-error", String(error?.message ?? error));
			}
		},
	});
}

// ── plugin entry ─────────────────────────────────────────────────────────────

function apply(ctx) {
	ctx.on(
		"llm/stream",
		(options, next) => (captureEnabled(options) ? observe(next, beginCapture(options)) : next()),
		{ global: true },
	);
	ctx.effect(
		() => registerRoutes(ctx),
		"conv-4-inspector: /conv-inspector/api routes",
	);
}

export { apply, inject, name };
