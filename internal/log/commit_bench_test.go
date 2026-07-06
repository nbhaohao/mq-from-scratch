// 已就位（AI 生成）：m05 s2 group commit 基准 —— 每条 fsync vs 批量 fsync 的吞吐差。
// 依赖你实现的 groupCommitter.Append。跑法：
//
//	go test -bench=BenchmarkGroupCommit -benchmem ./internal/log/
//	go test -bench='BenchmarkGroupCommit/batch=1$' -cpuprofile=cpu.out ./internal/log/
//	go tool pprof -top cpu.out    # batch=1 里 syscall.Fsync/File.Sync 会名列前茅
package log

import (
	"fmt"
	"os"
	"testing"
)

func benchGroupStore(b *testing.B) *store {
	f, err := os.CreateTemp(b.TempDir(), "gcbench")
	if err != nil {
		b.Fatal(err)
	}
	s, err := newStore(f)
	if err != nil {
		b.Fatal(err)
	}
	return s
}

// BenchmarkGroupCommit：batch=1（每条 fsync，最安全最慢）对比 batch=100（group commit）。
// 两条曲线的 ns/op 差 = fsync 被摊薄带来的吞吐收益，也就是 Kafka 不每条 fsync 的理由。
func BenchmarkGroupCommit(b *testing.B) {
	for _, batch := range []int{1, 100} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			s := benchGroupStore(b)
			gc := newGroupCommitter(s, batch)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := gc.Append(write); err != nil {
					b.Fatal(err)
				}
			}
			if err := gc.Flush(); err != nil {
				b.Fatal(err)
			}
		})
	}
}
