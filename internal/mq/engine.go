package mq

import (
	"sync"
	"time"

	"github.com/nbhaohao/mini-mq/internal/log"
)

// Engine：把 m01 的单个 partition log 包成一个「并发投递引擎」。
// 多个 goroutine 同时往里写(生产者)，多个 goroutine 长轮询地读(消费者)，
// 还要能优雅关停、在下游跟不上时对上游施加背压。本模块所有并发原语都挂在它身上：
//   - notify：s1 长轮询的唤醒广播（有新消息就叫醒所有等待者）
//   - mu：s2 保护共享状态(n / log / notify)，让并发生产者安全
//   - quit + wg + once：s3 优雅关闭（广播「该停了」+ 等所有 worker 退出，且幂等）
//   - slots：s4 有界令牌桶，满则背压/限流
type Engine struct {
	mu     sync.Mutex
	log    *log.Log
	n      uint64        // 已写入条数 = 高水位；下一条写在 offset n
	notify chan struct{} // s1：每次写入 close+swap = 广播「有新消息」

	quit chan struct{}  // s3：close 它 = 广播「该停了」
	once sync.Once      // s3：保证 quit 只 close 一次（关闭要幂等）
	wg   sync.WaitGroup // s3：等所有 Spawn 出去的 worker 退出

	slots chan struct{} // s4：有界缓冲当令牌桶，占满 = 背压
}

// NewEngine：已就位（AI 生成）。maxInflight = s4 令牌桶容量（同时在途消息上限）。
func NewEngine(l *log.Log, maxInflight int) *Engine {
	return &Engine{
		log:    l,
		notify: make(chan struct{}),
		quit:   make(chan struct{}),
		slots:  make(chan struct{}, maxInflight),
	}
}

// broadcast：已就位（AI 生成）。0904 的「关闭即广播」模式——
// 关掉当前 notify，所有 select 在它上面的等待者会同时收到零值而醒来；
// 再换一个新的 channel 给下一批等待者。**必须在持有 e.mu 时调用。**
func (e *Engine) broadcast() {
	close(e.notify)
	e.notify = make(chan struct{})
}

// writeLocked：已就位（AI 生成）。内部追加：写 log、抬高水位、广播有新消息。
// 名字带 Locked = **假设调用方已持有 e.mu**，自己不加锁（s2 的 Produce 负责持锁）。
// s1 测试里单条灌数据也走它（测试自己夹 e.mu.Lock/Unlock）。
func (e *Engine) writeLocked(v []byte) (uint64, error) {
	off, err := e.log.Append(v)
	if err != nil {
		return 0, err
	}
	e.n++
	e.broadcast()
	return off, nil
}

// Poll 你来实现（读 offset from 的消息；没有就阻塞，直到有新消息被唤醒、或 timeout 到点）：
// 长轮询 = 不让消费者空转轮询("有没有新消息？没有。有没有？没有…")狂烧 CPU。
// 没消息时挂起在 notify 上睡觉，生产者写入时 broadcast 把你叫醒，你再回头读。
//
//	1 deadline := time.After(timeout)   // 循环外算一次：多次假唤醒也不该刷新总超时
//	2 for {
//	3   e.mu.Lock()
//	4   if from < e.n {                 // 有了：读出来返回（读也在锁内，和写互斥）
//	5      v, err := e.log.Read(from); e.mu.Unlock(); return v, true, err
//	6   }
//	7   ch := e.notify                  // 没有：在锁内抓当前 notify 快照
//	8   e.mu.Unlock()
//	9   select {
//	10    case <-ch:                    // 被生产者广播叫醒 → 回到循环重读
//	11    case <-deadline:              // 超时 → 空手而归(ok=false)，不是错误
//	12       return nil, false, nil
//	13  }
//	14 }
//
// 为什么第 7 步要在锁里抓 ch：抓完解锁再 select，是为了别持锁睡觉(否则生产者拿不到锁、
// 永远无法 broadcast，死锁)。而 close 换 channel 也在锁里，所以你抓到的要么是即将被关的、
// 要么是新的——不会漏掉唤醒。
func (e *Engine) Poll(from uint64, timeout time.Duration) (value []byte, ok bool, err error) {
	deadline := time.After(timeout)

	for {
		e.mu.Lock()
		if from < e.n {
			v, err := e.log.Read(from)
			e.mu.Unlock()
			return v, true, err
		}
		ch := e.notify
		e.mu.Unlock()

		select {
		case <-ch:
		case <-deadline:
			return nil, false, nil
		}
	}
}

// Produce 你来实现（并发安全地写入一条消息，返回它的 offset）：
// 多个生产者 goroutine 会同时调它。共享状态 e.n / e.log / e.notify 不加保护地并发读写
// = 数据竞争(go test -race 会红)：offset 算错、log 写坏、n 丢更新。
// 最直接的解法：一把互斥锁串行化写入，锁内调 writeLocked。
//
//	1 e.mu.Lock()
//	2 defer e.mu.Unlock()
//	3 return e.writeLocked(v)
//
// 取舍(concepts 会展开)：Mutex 简单但所有生产者抢一把锁、写路径串行；
// 另一路子是「每 partition 一个专职 writer goroutine + channel 收活」，靠「只有一个 goroutine 碰 log」
// 免锁——Kafka/多数 LSM 引擎走后者。本课先用 Mutex 把正确性拿到手。
func (e *Engine) Produce(v []byte) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.writeLocked(v)
}

// Spawn：已就位（AI 生成）。派一个受管的 worker goroutine：登记到 wg，
// 把 quit 只读地交给它，退出时自动 wg.Done。worker 自己 select quit 决定何时收工。
func (e *Engine) Spawn(worker func(quit <-chan struct{})) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		worker(e.quit)
	}()
}

// Shutdown 你来实现（优雅关闭：广播「该停了」+ 阻塞等所有 worker 真正退出）：
// 这就是补 db 课 0904 欠下的并发债。关键三点：
//
//   - close(quit) 是「一对多广播」：一次 close，所有 select 在 quit 上的 worker 同时收到零值。
//     （不能用「发 N 个值」——你不知道有几个 worker，close 天然广播给全部。）
//
//   - 用 once 保证只 close 一次：重复 close 已关闭的 channel 会 panic，关闭必须幂等。
//
//   - wg.Wait() 把「发了关闭信号」升级成「确认都停了」：返回后没有 worker 还在跑，可以安全释放资源。
//
//     1 e.once.Do(func() { close(e.quit) })
//     2 e.wg.Wait()
func (e *Engine) Shutdown() {
	e.once.Do(func() { close(e.quit) })
	e.wg.Wait()
}

// Release：已就位（AI 生成）。归还一个令牌（消费/ack 完一条就调它腾出容量）。
func (e *Engine) Release() { <-e.slots }

// Acquire 你来实现（背压闸门：占一个在途名额，占满时按 block 决定阻塞还是拒绝）：
// slots 是容量 maxInflight 的有界 channel，塞进一个空结构体 = 占一个名额，塞满 = 已达在途上限。
//
//   - block=true：`e.slots <- struct{}{}` 满时会**阻塞**生产者，直到别处 Release 腾位——这就是背压
//     （下游慢，就让上游等，而不是无限堆积到 OOM）。
//
//   - block=false：select + default 试一下，满就立刻 return false = **限流/丢弃**(load shedding)，不等。
//
//     1 if block { e.slots <- struct{}{}; return true }
//     2 select {
//     3   case e.slots <- struct{}{}: return true
//     4   default: return false        // 满 → 非阻塞拒绝
//     5 }
func (e *Engine) Acquire(block bool) bool {
	if block {
		e.slots <- struct{}{}
		return true
	}
	select {
	case e.slots <- struct{}{}:
		return true
	default:
		return false
	}
}
