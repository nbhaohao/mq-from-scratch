# mini-mq

手搓一个单机 mini-Kafka（Go）。后端轨 B3。

从最底层的**分段 commit log** 往上盖：store（追加写字节）→ index（offset→pos，mmap）→ segment（两者配对，满则滚动）→ log（多段管理、按 offset 路由）。之后接 gRPC 服务化、topic/partition/consumer-group 语义、并发投递引擎。

## 跑

```sh
go test ./internal/log/            # 全量
go test -run TestStore ./internal/log/   # 只跑某一关
./scripts/verify.sh 1             # 跑到第 N 关并打印下一关
```

红=还没实现（函数体是 `panic("TODO sN…")`）；把它换成真实逻辑，测试变绿 = 当日达标。

## 路线

| 模块 | 主题 |
|---|---|
| m01 | 分段 commit log 存储（store/index/segment/log）|
| m02 | gRPC 服务化（protobuf + Produce/Consume + 流式）|
| m03 | MQ 语义（topic/partition、consumer group/offset、ack/重投递）|
| m04 | 并发投递引擎（长轮询、优雅关闭、背压）|
| m05 | 性能收尾（benchmark/pprof）+ ↔ Kafka/SQS write-up |
