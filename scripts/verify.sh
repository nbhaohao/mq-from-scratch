#!/usr/bin/env bash
# verify.sh [stage] —— 跑到第 N 关（不给 stage 则全量），绿了打印下一关目标。
set -e
cd "$(dirname "$0")/.."

case "${1:-all}" in
  1) f=TestStore;  next="s2 · index：offset→pos 映射，mmap 定长项" ;;
  2) f=TestIndex;  next="s3 · segment：store+index 配对，满则滚动" ;;
  3) f=TestSegment;next="s4 · log：多段管理，按 offset 路由" ;;
  4) f=TestLog;    next="m01 收尾：e2e + 口试，然后 m02 gRPC" ;;
  all) f=""; next="全部绿 → m01 通关" ;;
  *) echo "用法: verify.sh [1-4]"; exit 1 ;;
esac

if [ -z "$f" ]; then
  go test ./internal/log/
else
  go test -run "$f" ./internal/log/
fi
echo ""
echo "✅ 绿了。下一关：$next"
