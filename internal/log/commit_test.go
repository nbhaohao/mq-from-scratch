// 已就位（AI 生成）：m05 s2 红测试 —— group commit 的正确性 + fsync 频率。
package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func newGroupStore(t *testing.T) *store {
	f, err := os.CreateTemp(t.TempDir(), "gc")
	require.NoError(t, err)
	s, err := newStore(f)
	require.NoError(t, err)
	return s
}

// s2：batch=1 每条 fsync（最安全，syncs==条数）；batch=10 攒批 fsync（syncs 骤降）；
// 两种模式写入的数据都必须完整落盘可读——group commit 只摊薄 fsync 次数，不能丢数据。
func TestGroupCommit(t *testing.T) {
	const N = 100

	// batch=1：每条都 fsync 一次 → syncs 恰等于条数（这就是最安全但最慢的基线）
	each := newGroupStore(t)
	gc1 := newGroupCommitter(each, 1)
	for i := 0; i < N; i++ {
		_, err := gc1.Append(write)
		require.NoError(t, err)
	}
	require.NoError(t, gc1.Flush())
	require.Equal(t, N, each.syncs, "batch=1 应每条 fsync 一次")

	// batch=10：每 10 条 fsync 一次 → 10 次满批；收尾 Flush 再补 1 次 = 11，远少于 100
	batched := newGroupStore(t)
	gc10 := newGroupCommitter(batched, 10)
	var positions []uint64
	for i := 0; i < N; i++ {
		pos, err := gc10.Append(write)
		require.NoError(t, err)
		positions = append(positions, pos)
	}
	require.NoError(t, gc10.Flush())
	require.Equal(t, 10, batched.syncs, "batch=10：100 条应只 fsync 10 次（每满 10 条一次）")
	require.Less(t, batched.syncs, each.syncs, "group commit 的 fsync 次数应远少于每条 fsync")

	// 落盘正确性：重开同一文件，group commit 写入的每条都应完整可读（没丢）
	require.NoError(t, batched.Close())
	f2, err := os.OpenFile(batched.Name(), os.O_RDWR|os.O_APPEND, 0644)
	require.NoError(t, err)
	s2, err := newStore(f2)
	require.NoError(t, err)
	for i, pos := range positions {
		got, err := s2.Read(pos)
		require.NoError(t, err)
		require.Equalf(t, write, got, "第 %d 条 group commit 后应完整可读", i)
	}
}
