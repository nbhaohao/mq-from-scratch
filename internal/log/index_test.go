package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// s2 · index：写 offset→pos，读回；越界给 io.EOF；重开能从文件恢复最后一条。
func TestIndexWriteRead(t *testing.T) {
	f, err := os.CreateTemp("", "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	// 空索引读任意项 → 错误（io.EOF）
	_, _, err = idx.Read(-1)
	require.Error(t, err)

	entries := []struct {
		Off uint32
		Pos uint64
	}{{0, 0}, {1, 10}}
	for _, want := range entries {
		require.NoError(t, idx.Write(want.Off, want.Pos))
		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// 读超过已写项数 → io.EOF（不是 panic，不是脏数据）
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)

	// 关闭（截回真实大小），重开，从文件恢复：读 -1 应得最后一条
	require.NoError(t, idx.Close())
	f2, err := os.OpenFile(f.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)
	idx2, err := newIndex(f2, c)
	require.NoError(t, err)
	off, pos, err := idx2.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, uint64(10), pos)
}
