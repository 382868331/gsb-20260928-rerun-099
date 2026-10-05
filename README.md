# lrpc：本地远程调用框架示例

这是一个从零实现的 TCP 调用框架，没有包装 gRPC、Thrift 或现成 RPC 框架。业务方法只看到 Go 的请求/响应类型，连接、分帧、请求 ID、超时和取消由框架管理。

## 分层

- `rpc/frame.go`：4 字节大端长度前缀分帧，并限制单帧大小。
- `rpc/envelope.go`：消息信封，包含请求 ID、方法名、错误、头部和业务负载。
- `rpc/codec.go`：负载编码接口；当前提供 JSON，后续可替换或组合压缩编码。
- `rpc/client.go`：单连接复用、会话等待表、普通调用、连续结果读取、优先取消帧和断线失败。
- `rpc/server.go`：普通/流式请求分发、连接隔离、逐片段 ACK 背压、并发上限、鉴权、截止时间和 panic 隔离。
- `rpc/bidi.go`：同一连接上的多条双向流，独立序号、逐段 ACK、半关闭、取消与收尾状态上报。
- `demo/` 与 `cmd/`：本机双进程演示和类型化业务定义。

## 关键语义

- 每个请求在当前连接会话内拥有独立 ID；响应只查当前会话的等待表。
- 多个请求可在同一条 TCP 连接上交错发送，响应乱序返回也不会串结果。
- `context.WithCancel` 会发送取消帧，服务端取消对应 handler 的上下文。
- `context.WithTimeout` 本地立即结束调用，并通过截止时间头部让服务端提前停止工作。
- 连接断开时，该连接上的等待调用统一返回 `ErrConnectionClosed`；后续调用自动建立新会话。
- 旧会话关闭后其等待表被清空，旧连接的迟到响应不可能匹配新调用。
- 流式请求使用独立信封 ID；客户端按 `Recv` 返回顺序投递，服务端在收到上一片段 ACK 前不会发送下一片段，因此慢读取方看到的未消费在途片段最多为 1。
- 主动取消使用独立高优先级取消帧，不排在被流控的普通写消息之后；连接断开会取消该连接上的服务端 context，客户端等待同时结束。
- 流式正常结束返回 `io.EOF`；取消返回 `context.Canceled`，连接错误返回 `ErrConnectionClosed`。
- 双向流（`Client.BidiStream` / `Server.HandleBidi`）与普通请求、单向流共享同一连接，互不影响。
- 双向流每个方向独立编号（从 1 开始）并逐段 ACK：发送方在收到上一段 ACK 前不会发出下一段，每条流每个方向最多 1 段未确认数据。`StreamCredits`（默认 32）是**整条连接共享**的缓冲额度池：所有流的在途段合计有界，慢流每条只占用 1 个额度，已确认段立即释放额度供其他流、短请求以及取消/结束通知继续使用，大流无法独占额度。
- `State()` 给出每条流的确定范围：`[1, Acked]` 已确认、`(Acked, Sent]` 已发出但结果未定（`Inflight()` 返回其数量），`Received` 为已交给本端应用的段数；连接关闭后这些计数仍然有效，不会被统一标成成功或取消。
- `Snapshot()` 返回可保存的值拷贝，按真实 `StreamID`/方法名关联流，并记录发送和已确认序号、`ConfirmedRange()`、`PendingRange()`、每段负载大小、收发半关闭状态、收尾原因以及当时连接共享额度（`Limit`/`Used`/`Available`）。快照只读取诊断事实，不发送帧、不触发重放，也不会把未知结果改成成功；`CompareStreamSnapshots(before, after)` 只计算两次快照之间新增得到 ACK 的连续区间。
- 收尾原因同时以稳定形式 `ReasonKind` 保存：正常结束为 `eof`、主动取消为 `canceled`、超时为 `timeout`、连接断开为 `connection_closed`（对端重置为 `reset`），连接快照同样记录关闭事实与原因种类。`SaveStreamSnapshots`/`LoadStreamSnapshots` 把快照写成可重读的 JSON；重读后的快照仍按原 `StreamID` 与序号查询（`FindStreamSnapshot`、`SegmentSizes`、确认/待确认区间），但只是纯数据——不能发送、不能重放，待确认区间依旧保持未知，不会变成成功。已保存的旧快照是值拷贝，不随活动连接后续变化。
- `CloseSend` 半关闭只结束本方向：对端 `Recv` 在消费完已发数据后得到 `io.EOF`，反方向及其他流、普通请求照常继续；半关闭后 `Send` 返回 `ErrStreamClosed`。双向同时半关闭时双方最终状态一致，半关闭尾段在已经报告正常结束后不会再被重置丢失。
- `Cancel` 只取消目标流：对端 handler 的 context 被取消，本端后续操作为 `context.Canceled`；取消帧不排在受流控的数据帧之后，且绝不扩散到同一连接上的其他流或普通请求。与尾段 ACK 交错时，已经被对端确认的分段仍正常完成。服务端 handler 未读完入方向数据就返回时，框架先发半关闭再发重置，仍开放的对端发送方向得到 `ErrStreamReset`，不会无声悬挂；对端若已经半关闭，则不再误发重置。
- 连接中断时，流上的 `Send`/`Recv` 返回 `ErrConnectionClosed`；`State()` 如实保留 `Acked`（对端已确认）与 `Sent>Acked`（已发出但结果不能确定）。普通请求断线时同样以 `ErrConnectionClosed` 失败，结果视为不能确定，框架绝不静默重放可能有副作用的请求；只有新调用才会走新会话。
- 坏帧或超大帧只关闭对应连接；非法大小、未知版本等在进入业务处理前被拒绝。
- `MaxPending` 限制等待调用数量，`WriteQueue` 限制连接写队列，`MaxHandlers` 限制服务端并发 handler，`StreamCredits` 限制单条连接上双向流在途分段的共享总额度。
- 鉴权通过请求头部注入，不进入业务请求结构；压缩后续可作为负载编码或内容编码层扩展。

## 运行本机演示

启动服务端：

```bash
go run ./cmd/demo-server -addr 127.0.0.1:19090 -token local-demo-token
```

另开终端运行普通流式、取消和背压演练：

```bash
go run ./cmd/demo-client -addr 127.0.0.1:19090 -token local-demo-token -phase stream
```

多流诊断快照演练会并发打开 3 条流，发送 1B、4KB、100KB 等不同大小分段，先展示待确认区间和共享额度，再展示第二次快照新增确认的 `[2,2]`，最后关闭连接并保留各流第 3 段为未知结果：

```bash
go run ./cmd/demo-client -addr 127.0.0.1:19090 -token local-demo-token -phase snapshot
```

输出会核对普通调用结果、6 个片段的顺序、`max_inflight=1`、最终 `state=eof`，并确认取消的观察流最终为 `state=canceled`。

一键脚本会启动真实 TCP 服务端，完成流式阶段后杀掉并重启服务端，再启动新客户端验证旧连接结束后的新会话往返：

```bash
./demo/run.sh
```

也可以手动模拟重连：

```bash
go run ./cmd/demo-server -addr 127.0.0.1:19091
go run ./cmd/demo-client -addr 127.0.0.1:19091 -phase stream
# 停止服务端后重新启动
go run ./cmd/demo-server -addr 127.0.0.1:19091
go run ./cmd/demo-client -addr 127.0.0.1:19091 -phase reconnect
```

## 测试

```bash
go test -race ./...
```

测试覆盖并发乱序响应、流式顺序、共享连接关联、慢读取 ACK 背压、真实上下文取消、超时、断连结束调用、重连、旧会话响应隔离、坏帧隔离、消息大小限制和等待队列限制。

双向流测试覆盖：多条流与普通请求并行且内容/序号互不串扰、不同大小分段（1B 到 100KB）的顺序与内容核对、可保存快照及两次快照间新增确认区间比较、半关闭后反方向继续工作且双方最终状态一致、交错取消只影响目标流、慢接收方窗口为 1 且不饿死短请求、断连后已确认/不能确定分段和共享额度的精确上报、普通请求不重放、服务端提前返回时的入方向重置。
