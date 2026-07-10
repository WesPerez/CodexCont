# CodexCont

用于 Codex / OpenAI Responses 兼容 API 的“继续思考”中间件。

本项目是一个轻量 Starlette 代理，部署在编码代理和上游 Responses 接口之间。它会检测一种已知的推理截断指纹：`usage.output_tokens_details.reasoning_tokens == 518 * n - 2`。检测到后，中间件会在后台让模型继续思考，并把多轮上游流式响应折叠成一个连贯的下游 SSE 响应。

```text
编码代理  ->  CodexCont  ->  Codex / Responses API
```

> **用 AI Agent 安装？** 把 [`INSTALL-GUIDE-AGENT/AGENT.md`](INSTALL-GUIDE-AGENT/AGENT.md) 交给你的 Agent —— 这是一份专为 AI Agent 在你机器上逐步执行而写的安装手册。

## 免责声明

本项目是对已观察到的 OpenAI Codex 推理截断机制的明确绕过。若使用本中间件的行为被视为滥用、违反服务条款、导致费用异常增加，或造成其他不良后果，均由使用者自行承担责任。

## 功能概览

- 实时向下游转发 reasoning 项，保留“正在思考”的体验。
- 在上游 terminal event 出现前，缓存暂定的最终输出（`message` 和 `function_call`）。
- 如果本轮被判定为截断，丢弃暂定输出，并携带已产生的 reasoning 打开下一轮续写请求。
- 如果本轮自然完成或触发安全上限，冲刷最终一轮的输出，并发出一个重构后的 terminal response。
- 对不符合条件的请求透明透传。

默认续写方式是隐藏的 `phase: "commentary"` assistant 消息（`"Continue thinking..."`）。也支持旧版的合成工具调用对（`tool_pair`）模式。

## 环境要求

- Python `>= 3.12`
- 推荐使用 [`uv`](https://docs.astral.sh/uv/)

运行依赖在 `pyproject.toml` 中声明：

- `httpx`
- `starlette`
- `uvicorn`

## 快速开始

```bash
uv sync
cp config.example.toml config.toml
uv run python run.py
```

`run.py` 会读取本地 `config.toml`；请先从 `config.example.toml` 复制一份，再按需调整。

示例默认服务监听 `127.0.0.1:8787`，接受以下路径的 POST 请求：

- `/v1/responses`

也可以直接使用当前虚拟环境运行：

```bash
# Windows / 本工作区 Git Bash
.venv/Scripts/python.exe run.py
```

## 将客户端指向代理

把原本的上游接口地址替换为本代理地址即可。

示例：

```text
http://127.0.0.1:8787/v1/responses
```

示例默认配置（`config.example.toml`，复制为 `config.toml` 后使用）为：

```toml
[upstream]
url = "https://chatgpt.com/backend-api/codex/responses"
mode = "header"
dynamic_allowed_origins = ["https://api.openai.com", "https://chatgpt.com"]
```

当 `mode = "header"` 时，请求头 `Responses-API-Base` 会覆盖配置中的 `url`；如果没有该请求头，则回退到配置的 Codex URL。

例如，要指向通用 Responses 兼容端点，可以发送：

```text
Responses-API-Base: https://api.openai.com/v1
```

中间件会自动追加 `/responses`；如果传入值已经以 `/responses` 结尾，则保持不变。该控制头不会被继续转发到上游。请求头指定的目标必须精确匹配 `dynamic_allowed_origins`；空列表表示禁用动态目标。解析到私网或回环地址时默认拒绝，只有显式设置 `dynamic_allow_private_ips = true` 才允许。

## 鉴权

`config.toml` 支持三种鉴权模式。示例默认值是 `passthrough`：

```toml
[auth]
mode = "passthrough"               # passthrough | inject | passthrough_then_inject
access_token = ""                  # 作为 Authorization: Bearer <access_token> 发送
chatgpt_account_id = ""            # 非空时作为 chatgpt-account-id 发送
```

模式说明：

- `passthrough`：只转发调用方提供的鉴权头，不注入配置中的凭据。
- `inject`：使用配置中的凭据设置/覆盖鉴权头。
- `passthrough_then_inject`：调用方已有鉴权头则保留；没有时才用配置中的凭据补上。

安全保护：请求使用 `Responses-API-Base` 时，配置中的 access token、account ID 和 `[upstream.headers]` 覆盖都不会应用到动态目标，只有调用方自己提供的请求头可以发送到白名单目标；目标支持时仍可使用无鉴权或非 Authorization 鉴权方式。

`[server].max_request_body_bytes` 即使在绕过 Nginx 直连服务时也会限制请求正文。反向代理层也应设置上限；示例默认值为 32 MiB。

不要提交密钥。`.gitignore` 已忽略 `rt.json` 和 `free_rt.json`；如果把 token 写入 `config.toml`，也请谨慎管理。

## 什么时候会执行续写折叠

只有同时满足以下条件时，中间件才会执行折叠逻辑：

- `[continue].enabled = true`
- 请求体是 JSON 对象
- `stream` 为真
- reasoning 没有被显式禁用（`"reasoning": false` 会关闭折叠）
- 使用 `method = "tool_pair"` 时，请求没有声明与 `[continue].continue_tool_name` 同名的真实工具

其他请求会作为普通流式请求透明透传。

## 续写逻辑

每个上游 round 的处理流程：

1. reasoning 相关事件实时转发，并重写 `sequence_number` 和 `output_index`。
2. message 和 function-call 事件作为暂定输出缓存起来。
3. 收到 terminal event 后读取 `usage.output_tokens_details.reasoning_tokens`。
4. 如果 token 数匹配 `518 * n - 2`，位于配置的 tier 窗口内，存在 encrypted reasoning content，且安全上限允许，则中间件会：
   - 丢弃本轮暂定输出；
   - 把本轮 reasoning 和续写标记追加到下一轮请求 input；
   - 打开新的上游流式 round。
5. 否则，冲刷最终缓存的输出，并发出重构后的 terminal event。

下游编码代理只会看到一个 response；隐藏轮次的细节会写入最终响应的 metadata。

## 流式超时

可以通过 `[stream].upstream_event_timeout_seconds` 限制单个上游 round 等待下一条解析后 SSE `data:` 事件的最长时间。通过 `[stream].upstream_round_timeout_seconds` 限制单个 round 的总耗时，即使上游持续发出可解析 SSE 事件也会生效：

```toml
[stream]
upstream_event_timeout_seconds = 300
upstream_round_timeout_seconds = 480
upstream_connect_timeout_seconds = 5
upstream_read_timeout_seconds = 330
upstream_write_timeout_seconds = 60
upstream_pool_timeout_seconds = 5
```

SSE comment / keepalive 不算进展。触发超时后，中间件会发出 `response.incomplete`，其中 `incomplete_details.reason` 为 `"upstream_event_timeout"` 或 `"upstream_round_timeout"`，并且不会冲刷尚未被 terminal event 确认的暂定 message / tool 输出。传输层超时还会限制连接建立、响应头/原始读取、请求上传和连接池等待；首个上游请求超时返回 `504`，其他连接错误返回 `502`。

## 请求审计日志

可以启用独立 SQLite 请求审计库，用于排查上游 `400` / schema 不兼容问题：

```toml
[request_log]
enabled = true
path = "logs/request_audit.sqlite3"
store_body = true
max_body_bytes = 0
background_body_writes = true
background_max_pending_bytes = 134217728
retention_days = 7
sqlite_busy_timeout_ms = 5000
prune_interval_seconds = 0
prune_batch_size = 0
store_forwarded_body = true
store_response_body = "errors"
max_response_body_bytes = 1048576
preview_chars = 240
```

审计库会写入请求级元数据、压缩后的原始 body、逐项拆解的 `input[i]`、`tools[i]`，以及 schema findings。它不会保存 `Authorization` / `Cookie` 等请求头。`request_input_items` 表能直接看到每个历史项的 `type`、`name`、`arguments_type`、`arguments_json_type`，例如定位 `input[83].arguments` 是 `string` 还是 `object`。

`max_body_bytes = 0` 表示完整保存请求正文；正数表示只保存对应字节数并将
更大的正文标为截断。启用后台写入后，压缩和 SQLite 正文写入进入单一有界
FIFO；队列积压时会回压，而不会静默丢失排障证据。
单个 artifact 大于队列预算时会改为同步写入，不进入队列。正文写入失败会
重试一次，并记录到 `request_audit_failures`。

`request_audit_bodies` 会按阶段保存压缩正文：`client_request_body`、
`upstream_request_body`、`upstream_response_body`、`downstream_response_body`。
默认保存实际转发给上游的请求正文；响应正文默认只保存错误响应。临时需要排查成功流式
SSE 时，把 `store_response_body` 改成 `"all"`。每次写入新审计记录时，会按
`retention_days` 清理这个审计库里的旧主记录，子表通过 SQLite 外键级联删除。
`sqlite_busy_timeout_ms` 限制审计写入等待数据库锁的时间；
`prune_interval_seconds` 和 `prune_batch_size` 用来限制请求路径上的清理频率和
单次删除量。生产环境建议使用较短的锁等待和有上限的周期性小批清理。

## 兼容性归一化

不同 Responses 兼容历史工具项对 `arguments` 的形态要求不完全一样。如果历史回放里混入旧客户端或特定上游的形态，可以打开兼容转换：

```toml
[compat]
normalize_input_arguments = true
synthesize_web_search_call_ids = true
max_output_tokens_compat = "keep"
reasoning_effort_compat = "keep"
```

开启后，仅归一化配置内的 `input[i]` 类型。默认会把 object 形态的 `function_call.arguments` 序列化成 JSON 字符串，这是 OpenAI Responses 工具调用历史的常规形态；同时会把 JSON 字符串形态的 `tool_search_call.arguments` 严格解析成 object，用于要求该内置工具历史形态的上游。开启 `synthesize_web_search_call_ids` 后，缺失的 `web_search_call.id` 会补成稳定的 `ws_...` id，因为 Responses 历史回放需要 item id。非法 JSON、非 object JSON、重复 key 对象、已经正确的值，以及无关字段都会保持原样。原始请求 bytes 仍保存在审计库，转换/跳过决策会写入 `request_compat_actions`。

`max_output_tokens` 是标准 Responses 字段。标准 Responses 上游应保持
`keep`。只有某个上游明确在 Responses 入口接受 Chat Completions 风格的
`max_tokens`，才使用 `max_output_tokens_compat = "rename_to_max_tokens"`。
如果上游两个字段都拒绝，只能使用 `"drop"`；它会同时移除 `max_output_tokens`
和旧式 `max_tokens`，这能恢复兼容，但会在转发前移除用户请求的输出上限。只有上游拒绝 `reasoning.effort = "minimal"`、但接受
`"none"` 时，才启用 `reasoning_effort_compat = "minimal_to_none"`。

## 响应 metadata

最终重构响应会包含代理相关 metadata，例如：

- `metadata.proxy_rounds`：每轮 reasoning token 数和检测出的 tier `n`。
- `metadata.proxy_billed_usage`：隐藏上游轮次的真实累计 token 用量。
- `metadata.proxy_stopped_reason`：当续写因上限或错误停止时出现。

下游可见的 `usage` 会被重构得像单个 response：输入/缓存 token 取第一轮，reasoning token 累加，最终一轮的非 reasoning 输出计入 output。

## 测试

测试套件是自包含的，不依赖 `pytest`：

```bash
uv run python tests/test_middleware.py
# 或
.venv/Scripts/python.exe tests/test_middleware.py
```

当前离线测试覆盖：

- 截断数学判断
- 增量 SSE 解析
- 基于抓包 fixture 的折叠/重写行为
- commentary 和 tool-pair 两种续写 payload
- header 透明转发
- 上游 URL 解析
- 鉴权安全保护
- EOF / 上游错误 / failed terminal / 流式超时处理
- 请求审计 SQLite 结构化记录
- `input[i].arguments` 兼容性归一化

## 项目结构

```text
middleware/
  app.py       # Starlette 应用和路由处理
  codex.py     # 截断数学和续写 payload 构造
  config.py    # config.toml 加载和 dataclass 配置
  creds.py     # 上游 header / auth 构造
  audit.py     # 独立 SQLite 请求审计
  proxy.py     # fold_stream 状态机
  sse.py       # 增量 SSE 解析和序列化
  store.py     # 可选 stateful repair 使用的内存 ID 存储

tests/
  test_middleware.py
  fixtures/

run.py         # uvicorn 入口
config.example.toml # 示例运行配置；复制为 config.toml 后本地使用
```

## 限制

- 最终答案文本会被缓存到 terminal round 证明未截断之后才发出，因此最终答案首 token 延迟可能高于普通流式请求。
- 非流式请求当前会透传，不进行折叠。
- 截断检测器是针对已观察到的 `518 * n - 2` 指纹设计的。
- 可选的 `repair_followup = "stateful"` 使用进程内内存状态；多代理实例之间不会共享。

## 致谢

感谢 [LINUX DO](https://linux.do) 社区的相关讨论，没有这些讨论，也就没有本项目。特别感谢 LINUX DO 社区的 @shinorochi 和 @dskdkj 一同明确截断机制和 GPT 的思考模型，感谢 @shinorochi 提出的基于 commentary 输入而非工具调用伪造的更好方案。
