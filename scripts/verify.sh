#!/usr/bin/env bash
# verify.sh [stage] —— 跑到某一关（不给则全量），绿了打印下一关目标。
# m01（存储）: 1-4 → internal/log ;  m02（gRPC）: 5-7 → internal/server
set -e
cd "$(dirname "$0")/.."

case "${1:-all}" in
  1) pkg=./internal/log/;    f=TestStore;   next="s2 · index：offset→pos 映射，mmap 定长项" ;;
  2) pkg=./internal/log/;    f=TestIndex;   next="s3 · segment：store+index 配对，满则滚动" ;;
  3) pkg=./internal/log/;    f=TestSegment; next="s4 · log：多段管理，按 offset 路由" ;;
  4) pkg=./internal/log/;    f=TestLog;     next="m01 收尾 → m02 s1 gRPC Produce/Consume" ;;
  5) pkg=./internal/server/; f=TestServerProduceConsume;   next="m02 s2 · 服务端流式 ConsumeStream" ;;
  6) pkg=./internal/server/; f=TestServerConsumeStream;    next="m02 s3 · 错误语义（越界→gRPC status）+ CLI" ;;
  7) pkg=./internal/server/; f=TestServerConsumePastBoundary; next="m02 收尾 → m03 s1 topic/partition 路由" ;;
  8) pkg=./internal/mq/; f=TestTopicPartitionRouting;      next="m03 s2 · consumer group + offset 提交（存内部 log）" ;;
  9) pkg=./internal/mq/; f=TestConsumerGroupOffset;        next="m03 s3 · ack/重投递（可见性超时, SQS 同款）" ;;
  10) pkg=./internal/mq/; f=TestVisibilityTimeoutRedelivery; next="m03 s4 · at-least-once + 消费端幂等去重" ;;
  11) pkg=./internal/mq/; f=TestDedupIdempotent;           next="m03 收尾：go run ./cmd/mqdemo 一键体感 → m04 并发投递引擎⭐" ;;
  all) pkg=./...; f=""; next="全部绿 → 当前模块通关" ;;
  *) echo "用法: verify.sh [1-11]"; exit 1 ;;
esac

if [ -z "$f" ]; then
  go test "$pkg"
else
  go test -run "$f" "$pkg"
fi
echo ""
echo "✅ 绿了。下一关：$next"
