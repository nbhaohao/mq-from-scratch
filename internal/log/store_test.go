package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var write = []byte("hello world")

// s1 · store：定长头+payload 追加，凭 pos 读回；重开文件仍读得到（落盘了）。
func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	// 追加 3 条，记下每条的 pos；n 应含 8 字节长度头
	width := uint64(len(write)) + lenWidth
	var positions []uint64
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, width, n)
		require.Equal(t, pos+n, width*i) // 紧凑排布：本条尾 = 第 i 条的宽度累加
		positions = append(positions, pos)
	}

	// 凭 pos 逐条读回
	for _, pos := range positions {
		got, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, got)
	}

	// 关闭落盘，重开同一文件，仍读得到第一条 —— commit log 的持久性
	require.NoError(t, s.Close())
	f2, err := os.OpenFile(f.Name(), os.O_RDWR|os.O_APPEND, 0644)
	require.NoError(t, err)
	s2, err := newStore(f2)
	require.NoError(t, err)
	got, err := s2.Read(positions[0])
	require.NoError(t, err)
	require.Equal(t, write, got)
}
