// 已就位（AI 生成）：m03 红测试。每关一个 Test 函数，方便 -run 过滤：
//
//	s1 → TestTopicPartitionRouting   s2 → TestConsumerGroupOffset
//	s3 → TestVisibilityTimeoutRedelivery   s4 → TestDedupIdempotent
package mq

import (
	"testing"
	"time"

	"github.com/nbhaohao/mini-mq/internal/log"
	"github.com/stretchr/testify/require"
)

// s1：同一个 key 必落同一 partition（顺序保证）；不同分区各自独立可读回。
func TestTopicPartitionRouting(t *testing.T) {
	b, err := NewBroker(t.TempDir())
	require.NoError(t, err)
	topic, err := b.CreateTopic("orders", 3)
	require.NoError(t, err)

	// 同 key 两条 → 必同分区
	p1, o1, err := topic.Produce([]byte("user-1"), []byte("a"))
	require.NoError(t, err)
	p2, o2, err := topic.Produce([]byte("user-1"), []byte("b"))
	require.NoError(t, err)
	require.Equal(t, p1, p2, "same key must route to same partition")
	require.NotEqual(t, o1, o2, "two records in same partition get distinct offsets")

	// 读回正确
	v1, err := topic.Consume(p1, o1)
	require.NoError(t, err)
	require.Equal(t, []byte("a"), v1)
	v2, err := topic.Consume(p2, o2)
	require.NoError(t, err)
	require.Equal(t, []byte("b"), v2)

	// 路由稳定：分区号必在 [0, numPartitions)
	require.GreaterOrEqual(t, p1, 0)
	require.Less(t, p1, topic.NumPartitions())
}

// s2：提交位点→读回；latest-wins；没提交过的 group 返回 found=false。
func TestConsumerGroupOffset(t *testing.T) {
	b, err := NewBroker(t.TempDir())
	require.NoError(t, err)

	// 没提交过 → found=false
	_, found, err := b.FetchOffset("g1", "orders", 0)
	require.NoError(t, err)
	require.False(t, found)

	// 提交 5，读回 5
	require.NoError(t, b.CommitOffset("g1", "orders", 0, 5))
	off, found, err := b.FetchOffset("g1", "orders", 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(5), off)

	// 再提交 9（同 group/分区）→ latest-wins
	require.NoError(t, b.CommitOffset("g1", "orders", 0, 9))
	off, _, err = b.FetchOffset("g1", "orders", 0)
	require.NoError(t, err)
	require.Equal(t, uint64(9), off)

	// 不同 group 互不干扰
	_, found, err = b.FetchOffset("g2", "orders", 0)
	require.NoError(t, err)
	require.False(t, found)
}

// s3：领取→不 ack→超时后同一条被重投；ack 后不再投。用注入的时间戳，无真实 sleep。
func TestVisibilityTimeoutRedelivery(t *testing.T) {
	l, err := log.NewLog(t.TempDir(), log.Config{})
	require.NoError(t, err)
	for _, v := range [][]byte{[]byte("m0"), []byte("m1"), []byte("m2")} {
		_, err := l.Append(v)
		require.NoError(t, err)
	}
	d := NewDeliverer(l, 3, 30*time.Second)
	t0 := time.Unix(1000, 0)

	// 连领两条 → offset 0、1（各自进入飞行）
	off, val, ok, err := d.Receive(t0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), off)
	require.Equal(t, []byte("m0"), val)
	off, _, ok, _ = d.Receive(t0)
	require.True(t, ok)
	require.Equal(t, uint64(1), off)

	// 都没 ack、还没超时 → 只剩 offset 2 可投
	off, _, ok, _ = d.Receive(t0)
	require.True(t, ok)
	require.Equal(t, uint64(2), off)

	// 仍在可见性窗口内 → 无可投
	_, _, ok, _ = d.Receive(t0.Add(10 * time.Second))
	require.False(t, ok)

	// 超时后 offset 0 重投（at-least-once 的来源）
	off, _, ok, _ = d.Receive(t0.Add(31 * time.Second))
	require.True(t, ok)
	require.Equal(t, uint64(0), off)

	// ack 掉 0，再超时也不会重投 0（下一条超时的是 1）
	d.Ack(0)
	off, _, ok, _ = d.Receive(t0.Add(31 * time.Second))
	require.True(t, ok)
	require.Equal(t, uint64(1), off)
}

// s4：首见 false、再见 true；不同 id 互不影响。
func TestDedupIdempotent(t *testing.T) {
	d := NewDedup()
	require.False(t, d.Seen("msg-a"))
	require.True(t, d.Seen("msg-a"))  // 重复
	require.False(t, d.Seen("msg-b")) // 新 id
	require.True(t, d.Seen("msg-b"))
}
