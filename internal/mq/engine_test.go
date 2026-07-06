// 已就位（AI 生成）：m04 红测试。每关一个 Test 函数，方便 -run 过滤：
//
//	s1 → TestLongPoll   s2 → TestConcurrentProduce（跑 -race）
//	s3 → TestGracefulShutdown   s4 → TestBackpressure
//
// 四个测试各自独立：只依赖已就位的 plumbing（NewEngine/writeLocked/Spawn/Release），
// 不互相调用别关的「你来实现」函数，所以任一关可单独 -run 跑绿。
package mq

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nbhaohao/mini-mq/internal/log"
	"github.com/stretchr/testify/require"
)

func newTestEngine(t *testing.T, maxInflight int) *Engine {
	l, err := log.NewLog(t.TempDir(), log.Config{})
	require.NoError(t, err)
	return NewEngine(l, maxInflight)
}

// s1：没消息时 Poll 阻塞到超时(不空转)；生产者写入后，阻塞中的 Poll 立刻被唤醒拿到消息。
func TestLongPoll(t *testing.T) {
	e := newTestEngine(t, 8)

	// 当前无消息 → 在 timeout 内返回 ok=false，且确实等到了超时（不是立刻返回）
	start := time.Now()
	_, ok, err := e.Poll(0, 50*time.Millisecond)
	require.NoError(t, err)
	require.False(t, ok)
	require.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond, "无消息应等到超时而非立刻返回")

	// 阻塞一个 Poll，20ms 后另一 goroutine 写入 → Poll 应被唤醒并拿到消息
	go func() {
		time.Sleep(20 * time.Millisecond)
		e.mu.Lock()
		_, _ = e.writeLocked([]byte("hello"))
		e.mu.Unlock()
	}()
	v, ok, err := e.Poll(0, time.Second)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("hello"), v)
}

// s2：50 个 goroutine 并发 Produce → 全部成功、高水位=总条数、每个 offset 都能读回。
// 用 go test -race 跑：Produce 若不保护共享状态，这里会报数据竞争。
func TestConcurrentProduce(t *testing.T) {
	e := newTestEngine(t, 4)
	const N = 50
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.Produce([]byte("x"))
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	require.Equal(t, uint64(N), e.n, "并发写入后高水位应等于总条数（无丢更新）")
	// 每个 offset 都读得回（offset 唯一、无覆盖/丢失）——直接读 log，不经别关的函数
	for off := uint64(0); off < N; off++ {
		v, err := e.log.Read(off)
		require.NoError(t, err)
		require.Equal(t, []byte("x"), v)
	}
}

// s3：Shutdown 广播 quit 并等所有 worker 退出；返回后 3 个 worker 都已结束；重复调用幂等不 panic。
func TestGracefulShutdown(t *testing.T) {
	e := newTestEngine(t, 4)
	var exited int32
	for i := 0; i < 3; i++ {
		e.Spawn(func(quit <-chan struct{}) {
			for {
				select {
				case <-quit:
					atomic.AddInt32(&exited, 1)
					return
				case <-time.After(time.Millisecond):
				}
			}
		})
	}
	require.Equal(t, int32(0), atomic.LoadInt32(&exited), "还没关，worker 都应在跑")

	e.Shutdown()
	require.Equal(t, int32(3), atomic.LoadInt32(&exited), "Shutdown 返回后 3 个 worker 都应已退出")

	require.NotPanics(t, func() { e.Shutdown() }, "重复 Shutdown 应幂等、不 panic")
}

// s4：有界令牌桶做背压——占满后非阻塞 Acquire 被拒(限流)；Release 后又可占；阻塞 Acquire 满时挂起、有空位才返回。
func TestBackpressure(t *testing.T) {
	e := newTestEngine(t, 2) // 容量 2

	require.True(t, e.Acquire(false))  // 占 1
	require.True(t, e.Acquire(false))  // 占 2 → 满
	require.False(t, e.Acquire(false)) // 满 → 非阻塞拒绝(限流)

	e.Release()                       // 腾一个
	require.True(t, e.Acquire(false)) // 又能占 → 再次满

	// 阻塞版：满时应挂起，直到别处 Release 才返回
	done := make(chan struct{})
	go func() {
		e.Acquire(true) // 满 → 阻塞在这
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("占满时阻塞版 Acquire 不应立刻返回")
	case <-time.After(30 * time.Millisecond):
	}

	e.Release() // 腾位 → 阻塞的 Acquire 应返回
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Release 后阻塞的 Acquire 应返回")
	}
}
