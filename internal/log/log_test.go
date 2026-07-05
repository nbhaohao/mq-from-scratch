package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// s4 · log：写读一条；越界报错；多段滚动后 Lowest/Highest 覆盖全程、重开能恢复。
func TestLogAppendReadRange(t *testing.T) {
	dir, err := os.MkdirTemp("", "log_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxIndexBytes = entWidth * 3 // 每段仅 3 条 → 写 5 条必滚段

	l, err := NewLog(dir, c)
	require.NoError(t, err)

	// 越界读（还没写）→ 报错，不 panic
	_, err = l.Read(0)
	require.Error(t, err)

	// 写 5 条：offset 连续 0..4，跨段读仍正确路由
	for i := uint64(0); i < 5; i++ {
		off, err := l.Append(write)
		require.NoError(t, err)
		require.Equal(t, i, off)
	}
	require.True(t, len(l.segments) > 1) // 确实滚动出了多个段
	got, err := l.Read(4)               // 读最后一条（落在后面的段）
	require.NoError(t, err)
	require.Equal(t, write, got)

	low, err := l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), low)
	high, err := l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(4), high)

	// 关闭重开：从磁盘上的多个段文件恢复，Highest 仍是 4
	require.NoError(t, l.Close())
	l2, err := NewLog(dir, c)
	require.NoError(t, err)
	high, err = l2.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(4), high)
}
