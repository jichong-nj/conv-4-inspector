// Offline harness: exercise the host half without cordis.
import { apply } from "./lib/index.js";

const listeners = [];
const routes = [];
const ctx = {
	on: (event, fn, options) => listeners.push({ event, fn, options }),
	effect: (fn) => fn(),
	webServer: { register: (route) => { routes.push(route); return () => {}; } },
};

apply(ctx);
console.log("listeners:", listeners.map((l) => `${l.event} ${JSON.stringify(l.options)}`).join(" | "));
console.log("routes:", routes.map((r) => `${r.kind} ${r.path}`).join(" | "));

const handler = routes[0].handler;

function fakeReq(endpoint, body) {
	const payload = Buffer.from(JSON.stringify(body ?? {}));
	const req = { method: "POST", url: `/conv-inspector/api/${endpoint}` };
	let sent = false;
	req[Symbol.asyncIterator] = () => ({
		next: async () => (sent ? { done: true, value: undefined } : ((sent = true), { done: false, value: payload })),
	});
	return req;
}

function post(endpoint, body) {
	const res = { statusCode: 0, headers: null, chunks: [] };
	res.writeHead = (status, headers) => { res.statusCode = status; res.headers = headers; };
	res.end = (buf) => res.chunks.push(buf);
	return handler(fakeReq(endpoint, body), res).then(() => JSON.parse(Buffer.concat(res.chunks).toString("utf8")));
}

// A fake provider stream: reasoning, text, a streamed tool call, usage, finish.
const chunks = [
	{ type: "block-start", index: 0, blockType: "reasoning" },
	{ type: "reasoning-delta", index: 0, text: "用户想" },
	{ type: "reasoning-delta", index: 0, text: "看请求内容。" },
	{ type: "block-end", index: 0, block: { type: "reasoning", text: "用户想看请求内容。" } },
	{ type: "block-start", index: 1, blockType: "text" },
	{ type: "text-delta", index: 1, text: "你好，" },
	{ type: "text-delta", index: 1, text: "我是助手。" },
	{ type: "block-end", index: 1, block: { type: "text", text: "你好，我是助手。" } },
	{ type: "block-start", index: 2, blockType: "tool-call" },
	{ type: "tool-call-delta", index: 2, id: "call_1", name: "bash" },
	{ type: "tool-call-delta", index: 2, argumentsDelta: '{"command":"ls' },
	{ type: "tool-call-delta", index: 2, argumentsDelta: ' -a"}' },
	{ type: "block-end", index: 2, block: { type: "tool-call", id: "call_1", name: "bash", arguments: '{"command":"ls -a"}' } },
	{ type: "usage", usage: { inputTokens: 1234, outputTokens: 87, cacheReadTokens: 900 } },
	{ type: "finish", reason: "tool-calls" },
];

const options = Object.freeze({
	provider: "halogen",
	model: "halogen-qwen3.8-flash-next",
	sessionId: "sess-abc123",
	maxTokens: 8192,
	temperature: 0.7,
	signal: { aborted: false },
	tools: [{ name: "bash", description: "Run a command", parameters: { type: "object", properties: { command: { type: "string" } } } }],
	messages: [
		{ role: "system", content: [{ type: "text", text: "You are a coding agent." }] },
		{ role: "user", content: [{ type: "text", text: "列出目录" }] },
	],
});

const waterfall = listeners.find((l) => l.event === "llm/stream").fn;

async function stream(opts) {
	const seen = [];
	for await (const chunk of waterfall(opts, () => (async function* () { for (const c of chunks) yield c; })())) {
		seen.push(chunk.type);
	}
	return seen.length;
}

const total = async () => (await post("list", {})).value.total;

// ── capture gate: off by default, per session ────────────────────────────────
console.log("capture default:", JSON.stringify((await post("capture", {})).value));
console.log("stream while off:", await stream(options), "chunks forwarded, list total:", await total());
console.log("enable:", JSON.stringify((await post("capture", { action: "set", sessionId: "sess-abc123", enabled: true })).value));
console.log("passthrough chunks:", await stream(options), "(all forwarded)");
await stream({ ...options, sessionId: "sess-other", model: "other-model" });
console.log("other session ignored, total:", await total(), "| enabled list:", JSON.stringify((await post("capture", {})).value.enabled));

const list = await post("list", {});
console.log("RAW:", JSON.stringify(list));
console.log("list:", JSON.stringify(list.value?.items.map((i) => ({
	id: i.id, purpose: i.purpose ?? "main", msgs: i.messageCount, tools: i.toolCount,
	dur: i.durationMs, stop: i.stopReason, out: i.usage?.outputTokens, resp: i.responsePreview,
}))), null, 1);
console.log("sessions:", list.value.sessions, "total:", list.value.total);

const detail = await post("detail", { id: list.value.items[0].id });
const d = detail.value;
console.log("detail blocks:", JSON.stringify(d.blocks, null, 1));
console.log("signal dropped:", !("signal" in d), "| raw deltas retained:", "chunkCount" in d ? d.chunkCount : "n/a");
console.log("messages shrunk:", JSON.stringify(d.messages));
console.log("tools:", JSON.stringify(d.tools));

const raw = await post("raw", { id: d.id });
const events = JSON.parse(raw.value.responseText);
console.log("raw request head:", raw.value.requestText.slice(0, 90));
console.log("raw signal marker:", raw.value.requestText.includes("[AbortSignal]"));
console.log("raw events:", events.length, "kinds:", [...new Set(events.map((e) => e.type))].join(","));
console.log("raw bytes req/resp:", raw.value.requestBytes, raw.value.responseBytes, "truncated:", raw.value.truncated);
console.log("raw missing:", JSON.stringify(await post("raw", { id: "req-999" })));

// per-request raw budget: a stream past the ceiling must truncate, not grow
const bigChunks = [];
for (let i = 0; i < 40000; i += 1) bigChunks.push({ type: "text-delta", index: 0, text: "x".repeat(40) });
for await (const _c of waterfall(options, () => (async function* () { for (const c of bigChunks) yield c; })())) { /* drain */ }
const bigRaw = await post("raw", { id: "req-2" });
console.log("big raw: truncated =", bigRaw.value.truncated, "kept =", bigRaw.value.eventCount,
	"dropped =", bigRaw.value.dropped, "bytes =", bigRaw.value.responseBytes, "<= limit", bigRaw.value.limitBytes);

console.log("disable:", JSON.stringify((await post("capture", { action: "set", sessionId: "sess-abc123", enabled: false })).value));
const before = await total();
await stream(options);
console.log("after disable, no new capture:", (await total()) === before, `(total ${before})`);
console.log("list exposes enabled:", JSON.stringify((await post("list", {})).value.enabled));
console.log("set without session:", JSON.stringify(await post("capture", { action: "set", enabled: true })));

console.log("404:", JSON.stringify(await post("nope", {})));
console.log("clear:", JSON.stringify(await post("clear", {})), "-> list total:", (await post("list", {})).value.total);
