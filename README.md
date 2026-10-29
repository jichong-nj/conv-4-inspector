# LLM对话监视器（conv-4-inspector）

在 DSH 对话页签栏里加一个 **「LLM对话监视器」** 视图，实时展示每一次发往大模型的**完整请求**（system、messages、工具定义、采样参数）和**拼接后的最终响应**（reasoning / text / tool-call 三段），外加耗时、token 用量、结束原因。

## 工作原理

宿主侧挂在 cordis 的 `llm/stream` 瀑布钩子上（`dsh-llm` 在真正调用 adapter 之前派发，参数就是 adapter 即将发送的 `GenerateOptions`）：

```
llm/stream(options, next)
  ├─ 快照 options        → provider / model / messages / tools / 采样参数
  └─ 包装 next()         → 逐块透传 StreamChunk，同时累加增量
                            └─ 流结束时只保留「拼好的最终 blocks」
                               原始 delta 不入库、不展示
```

捕获结果放在一个有界的内存环形缓冲（默认 60 条 / 24 MB，超出丢最旧的），浏览器侧通过 `ctx.webServer` 注册的前缀路由 `POST /conv-inspector/api/{list,detail,raw,capture,clear}` 读取。

**采集默认关闭**，按会话逐个打开：`llm/stream` 回调先看 `options.sessionId` 在不在这个开关里，不在就直接 `next()` 透传，不做任何快照。开关是进程内的 `Set`，重启后回到全关。原文另有一个独立的小环形（最近 16 条，单条 512 KB 上限），不挤占结构化列表的名额。

因为是**进程内内存缓冲**，重启 `dsh` 后历史捕获会清空；开启某个会话后，该会话范围内的调用都会被捕获（含 compaction、session-title 等 one-shot 调用，用 purpose 标签区分）；没有 `sessionId` 的请求不采集。

## 文件结构

```
conv-4-inspector/
├── package.json          # dsh.manifestVersion / bundle.patch / client.platform
├── cordis.patch.yml      # 挂载宿主侧 cordis 条目
├── build.js              # 把 src/client.js 包成 __ModuleLoader__ bundle
├── src/client.js         # 浏览器侧源码（React.createElement，无 JSX）
├── lib/index.js          # 宿主侧入口（cordis 插件）
├── lib/client.js         # 构建产物，由 dsh 以 /plugins/<id>/client.js 提供
└── test-host.mjs         # 离线自测：node test-host.mjs
```

改完 `src/client.js` 要重新构建：

```bash
node build.js     # 或 npm run build
```

## 安装

从 npm 装进 `web` profile（`dsh plugin` 会把参数转发给 pnpm）：

```bash
dsh plugin --profile web add @jichong/conv-4-inspector
```

本地开发用 `link:`（改完源码即生效，无需重装）：

```bash
dsh plugin --profile web add file:/path/to/conv-4-inspector
```

包里的 `dsh.bundle.patch` 会被自动并入配置树，**不需要**手改 profile 的 `cordis.patch.yml`。若要走手动路线，在 `~/.dsh/profiles/web/cordis.patch.yml` 追加：

```yaml
- insert:
    - id: conv-4-inspector
      name: '@jichong/conv-4-inspector'
```

新增 / 改名 bundle 会触发 profile 重组，装完重启一次 `dsh --profile web` 最稳。

验证是否装载：

```bash
dsh --profile web --dump-config | grep -A3 conv-4-inspector
curl -sS -X POST http://127.0.0.1:3080/conv-inspector/api/list -d '{}'
```

## 界面

- 顶部：**采集开关**（只作用于当前会话，绿=采集中）、捕获条数、会话筛选（多于一个会话时出现）、自动刷新（2s）/ 刷新 / 清空
- 列表每行：purpose 标签（main / compaction / session-title / streaming / error）、`provider/model`、响应预览、序号、`消息数/工具数`、输出 token
- 展开后：**概览** → **最终响应**（绿框，拼接结果）→ **请求原文 / 响应原文** → System → **请求 messages**（按 role 着色）→ **工具定义**
- 原文区默认折叠，**点开才拉取**（单条可达 512 KB，不跟着 2s 轮询走）；JSON 自动缩进排版、XML 自动缩进，可切「看原始 / 排版」，带语法着色和一键复制
- 深浅色跟随系统 `prefers-color-scheme`

## 采集开关

开关状态存在宿主进程内存里，`POST /conv-inspector/api/capture`：

```bash
# 查当前开了哪些会话
curl -sS -X POST http://127.0.0.1:3080/conv-inspector/api/capture -d '{}'
# 打开 / 关闭某个会话
curl -sS -X POST http://127.0.0.1:3080/conv-inspector/api/capture \
  -d '{"action":"set","sessionId":"session-xxxx","enabled":true}'
```

界面上就是面板左上角那个拨钮，会话 id 由 `conversation.view` 的 scope binding 注入（`register` 定义里的 `inject: (key) => ({ ciSessionId: key })`）。关闭只停止后续采集，已经抓到的记录保留，要清掉用「清空」。

## 注意

- 请求体里的长字符串按 8000 字符截断，`signal` 等不可序列化字段会丢弃；`options` 对循环构造的请求是深冻结的，本插件只读不改。
- 「原文」是 **DSH 交给 adapter 的归一化请求/事件流**，不是 provider 的实际线上字节：真正的 HTTP 请求由 `@earendil-works/pi-ai` 里的 `fetch` 发出，要抓那一层得包 `globalThis.fetch`，这里没做。响应原文是 `StreamChunk` 事件的 JSON 数组，不是 SSE 报文。
- 面板注册在 `conversation.view`，与内置的 Chat / Trajectory 视图同级并列（`id: llm-inspector`，不覆盖已有项）。
- 路由未做额外鉴权，与同 profile 的 `dsh-web-tools` 一致，依赖 webServer 自身的访问控制——只建议在 loopback 上使用。
