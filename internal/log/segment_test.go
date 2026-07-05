package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// s3 · segment：append 得绝对 offset（从 baseOffset 起），凭 offset 读回；
// 索引写满 → IsMaxed 为真（该滚段了）。
func TestSegmentAppendReadMaxed(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3 // 只放得下 3 条索引项

	s, err := newSegment(dir, 16, c) // baseOffset 从 16 起（不从 0）
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	// 追加 3 条：offset 应为 16、17、18；每条都读得回
	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, write, got)
	}

	// 索引已满 → IsMaxed 为真
	require.True(t, s.IsMaxed())
}
