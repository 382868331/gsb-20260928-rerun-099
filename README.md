# localrpc

一个从零实现的 Go 本地远程调用框架，核心只依赖标准库，不包装现成 RPC 框架。

## 结构

- `wire.go`：帧头、消息结构和 `Codec`；当前提供 JSON 编码。
- `client.go`：请求 ID、等待表、超时/取消、连接代号和连接复用。
- `server.go`：服务分发、有界任务队列、处理协程取消、坏帧隔离。
- `example_service.go`：不绑定具体编码细节的业务 Handler 示例。
- `cmd/demo`：本机客户端/服务端双方演示。
- `localrpc_test.go`：并发、取消、断连、旧连接、消息大小、队列和坏帧测试。

## 帧协议

每个帧由 16 字节固定头和变长消息体组成：

| 偏移 | 长度 | 含义 |
| --- | --- | --- |
| 0 | 2 | 魔数 `RC` |
| 2 | 1 | 版本 |
| 3 | 1 | 类型：请求、响应或取消 |
| 4 | 4 | 消息体长度 |
| 8 | 8 | 请求 ID |

消息体当前由 `JSONCodec` 编解码。后续增加压缩、鉴权或二进制编码时，可以实现新的 `Codec` 或在消息头中增加协商层，不需要修改业务 Handler 的方法定义。

## 生命周期保证

- 请求和响应通过全局递增 ID 关联，多请求交错返回不会串结果。
- 客户端等待项同时绑定连接代号；旧连接上的迟到响应即使 ID 相同，也不会匹配新连接的调用。
- 调用超时或主动取消时，客户端发送取消帧，服务端取消该 Handler 的 `context.Context`。
- 取消单个调用不关闭底层连接，后续调用继续复用。
- 连接断开时，该连接上所有在途调用立即收到错误。
- 坏帧或超大帧只结束受影响连接，服务端继续服务其他连接。
- 客户端限制等待请求数，服务端限制任务队列和并发 Handler 数。
- 消息大小在读写两侧都受限制。

## 运行

```sh
go test -race ./...
go run ./cmd/demo
```

演示使用 `net.Pipe` 连接同一进程内的客户端和服务端。它仍然通过真实的 `net.Conn` 传输完整帧，但不依赖当前受限沙箱中被禁止的系统 socket 监听能力。

在允许监听 socket 的普通本机环境中，可把演示切换成 TCP loopback：

```go
listener, _ := net.Listen("tcp", "127.0.0.1:0")
client, _ := localrpc.Dial(context.Background(), listener.Addr().String())
```

业务调用仍使用统一形式：

```go
response, err := client.Call(ctx, "demo", "echo", requestPayload, nil)
```
