// 已就位（AI 生成）：m05 s1 吞吐/延迟基线基准。
// 代码本身不用你写——被测的 Produce 是 m04 已实现的热路径。s1 的活是：
//   跑它、读数字、用 pprof 看热点、把基线记下来（s2 优化后回来对比）。
//
//	go test -bench=BenchmarkProduce -benchmem ./internal/mq/
//	go test -bench=BenchmarkProduce -cpuprofile=cpu.out ./internal/mq/ && go tool pprof -top cpu.out
package mq

import (
	"testing"

	"github.com/nbhaohao/mini-mq/internal/log"
)

func benchEngine(b *testing.B) *Engine {
	l, err := log.NewLog(b.TempDir(), log.Config{})
	if err != nil {
		b.Fatal(err)
	}
	return NewEngine(l, 1024)
}

// BenchmarkProduce：单生产者顺序写的吞吐/延迟基线。
// 读三个数：ns/op（单条延迟）、B/op、allocs/op（每条分配）。
// 记下这组数——尤其 allocs/op：m04 的 broadcast 每写一条都 close+make 一个新 channel，
// 这一处 per-write 分配会在 -benchmem 和 pprof 里现形。
func BenchmarkProduce(b *testing.B) {
	e := benchEngine(b)
	msg := []byte("hello mini-mq")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.Produce(msg); err != nil {
			b.Fatal(err)
		}
	}
}
