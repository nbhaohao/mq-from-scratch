package mq

import (
	"sync"
	"time"

	"github.com/nbhaohao/mini-mq/internal/log"
)

// Deliverer：SQS 风格的投递跟踪器，和 s2 的 Kafka 位点是「两种不同范式」的对照。
//   - Kafka 位点(s2)：一个单调的高水位「读到第 N 条」，顺序提交、不能有空洞。
//   - SQS(本关)：每条消息单独发放 + 单独 ack，可乱序、可有空洞；发出去若超时没 ack 就重投。
//
// 本关在一个 partition log 上实现 SQS 语义：Receive 领一条（附一个可见性截止时间），
// Ack 确认删除；到点没 ack 的，下次 Receive 会把它再发一遍——这就是 at-least-once 的来源。
type Deliverer struct {
	mu         sync.Mutex
	part       *log.Log
	high       uint64               // 已投递范围上界（本课等于已写入条数，测试里注入）
	visibility time.Duration        // 可见性超时：发出后多久没 ack 就重投
	acked      map[uint64]bool      // 已确认（永久删除，不再投）
	inflight   map[uint64]time.Time // offset -> 可见性截止时间（在飞行中、暂时不可再投）
}

// NewDeliverer：已就位（AI 生成）。high = 该分区消息条数，visibility = 可见性超时。
func NewDeliverer(part *log.Log, high uint64, visibility time.Duration) *Deliverer {
	return &Deliverer{
		part:       part,
		high:       high,
		visibility: visibility,
		acked:      map[uint64]bool{},
		inflight:   map[uint64]time.Time{},
	}
}

// Receive 你来实现（领取下一条「可投」的消息，并把它标记为飞行中）：
// 一条 offset「可投」的条件：没被 ack，且（从没发过 或 上次发出去已超过可见性截止时间）。
// 从小到大找到第一条可投的，读出来、记下它的新截止时间(now+visibility)，返回。
// ok=false 表示当前没有可投的（都 ack 了或都还在飞行中未超时）。
//
//	1 加锁 / defer 解锁
//	2 for off := uint64(0); off < d.high; off++ {
//	     if d.acked[off] { continue }                          // 已确认，跳过
//	     deadline, flying := d.inflight[off]
//	     if flying && now.Before(deadline) { continue }        // 还在飞行且没超时，暂不重投
//	     // 命中：这条可投
//	     value, err := d.part.Read(off)；err != nil return 0,nil,false,err
//	     d.inflight[off] = now.Add(d.visibility)               // 记新的可见性截止
//	     return off, value, true, nil
//	  }
//	3 return 0, nil, false, nil                                // 没有可投的
func (d *Deliverer) Receive(now time.Time) (offset uint64, value []byte, ok bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for off := uint64(0); off < d.high; off++ {
		if d.acked[off] {
			continue
		}
		deadline, flying := d.inflight[off]
		if flying && now.Before(deadline) {
			continue
		}

		value, err := d.part.Read(off)
		if err != nil {
			return 0, nil, false, err
		}
		d.inflight[off] = now.Add(d.visibility)
		return off, value, true, nil
	}

	return 0, nil, false, nil
}

// Ack 你来实现（确认某条已处理完，永久删除、不再重投）：
//
//	加锁；d.acked[offset] = true；delete(d.inflight, offset)
func (d *Deliverer) Ack(offset uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.acked[offset] = true
	delete(d.inflight, offset)
}
