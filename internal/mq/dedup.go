package mq

import "sync"

// Dedup：消费端幂等去重器。at-least-once 意味着同一条消息可能被投递多次
// （s3 里超时重投、或消费者处理完还没 ack 就崩了都会导致重复）。
// 系统层面「恰好一次」代价极高，工程上的标准解法是：投递保证 at-least-once，
// 消费端靠业务上的幂等键(messageID / 订单号 / 请求ID)自己去重，达成「效果上恰好一次」。
type Dedup struct {
	mu   sync.Mutex
	seen map[string]bool
}

// NewDedup：已就位（AI 生成）。
func NewDedup() *Dedup {
	return &Dedup{seen: map[string]bool{}}
}

// Seen 你来实现（第一次见某 id 返回 false 并记下；再见同一 id 返回 true）：
// 消费者拿到消息先问 Seen(id)：true 就跳过（已处理过），false 就正常处理。
// 「检查+记录」必须在一把锁里原子完成，否则两个 goroutine 可能都判 false 而重复处理。
//
//	1 加锁 / defer 解锁
//	2 if d.seen[id] { return true }   // 见过 → 是重复
//	3 d.seen[id] = true               // 首见 → 记下
//	4 return false
func (d *Dedup) Seen(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.seen[id] {
		return true
	}
	d.seen[id] = true
	return false
}
