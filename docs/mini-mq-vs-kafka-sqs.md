# mini-mq ↔ Kafka / SQS 逐概念映射（m05 s3 write-up）

> 骨架已就位（AI 生成），**内容你来填**。目标：把手搓的 mini-mq 每个机制，对到真实
> Kafka / AWS SQS 的同类机制上，说清「我做了什么、工业界怎么做、差在哪、为什么」。
> 这是本课的收口，也是面试讲「我造过 MQ」时的底稿——**用你自己 s1/s2 跑出的数字说话**。

## 1. 概念映射表

| mini-mq 里的机制 | 对应文件/函数 | Kafka | AWS SQS | 差异 & 取舍（你来写） |
|---|---|---|---|---|
| 分段 commit log（store+index，满则滚段） | `internal/log/` | 分段日志 + 稀疏索引 | 托管，不暴露 | |
| append 得 offset | `segment.Append` | offset 单调递增 | 无 offset（消息级 ID） | |
| topic → partition（key hash 路由） | `mq/broker.go partitionFor` | 分区 + key 路由 | 无分区（FIFO 有 MessageGroupId） | |
| consumer group + 位点提交 | `mq/offset.go` | `__consumer_offsets` | 无位点（删除即消费） | |
| ack / 可见性超时重投 | `mq/delivery.go` | — | Visibility Timeout / DLQ | |
| at-least-once + 幂等去重 | `mq/dedup.go` | at-least/exactly-once | at-least-once（标准队列） | |
| 长轮询 | `mq/engine.go Poll` | `fetch.max.wait.ms` | ReceiveMessage WaitTimeSeconds | |
| 优雅关闭 | `mq/engine.go Shutdown` | controlled shutdown | 托管 | |
| 背压 | `mq/engine.go Acquire` | 生产者缓冲/阻塞 | 托管限流 | |
| group commit / 批量 fsync | `log/commit.go` | `log.flush.interval.messages`（默认不每条 fsync，靠副本） | 托管 | |
| 副本复制 | ❌ 未做 | ISR 多副本 | 托管多 AZ | 归 6.824 的 Raft |

## 2. 性能基线与优化（贴你 s1/s2 的真实数字）

- **s1 基线**（`go test -bench=BenchmarkProduce -benchmem`）：ns/op = ___、B/op = ___、allocs/op = ___。
  - pprof top 热点是谁？（提示：m04 broadcast 每写一条 close+make 一个 channel。）
- **s2 group commit**（`go test -bench=BenchmarkGroupCommit -benchmem`）：
  - batch=1（每条 fsync）：___ ns/op；batch=100：___ ns/op；提速约 ___ ×。
  - pprof 里 batch=1 的头号热点是谁？（提示：File.Sync / fsync syscall。）

## 3. 一句话回答（面试口径，你来写）

1. **为什么 Kafka 默认不每条 fsync？** ___
2. **at-least-once 下消费端怎么保证不重复处理？** ___
3. **可见性超时和 ack 是怎么实现「没 ack 就重投」的？** ___
4. **mini-mq 现在最大的缺口是什么？** ___（提示：副本 → 单机宕机即丢，见 6.824）
