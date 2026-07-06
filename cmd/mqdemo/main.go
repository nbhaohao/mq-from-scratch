// 已就位（AI 生成）：m03 语义层的一键体感 demo。跑 `go run ./cmd/mqdemo`，
// 一口气演示四关的效果:分区路由 / 位点提交 / 可见性超时重投 / 消费端去重。
// 纯本地内存 + 临时目录，不用 key、不起服务。四关都实现后才能完整跑通。
package main

import (
	"fmt"
	"os"
	"time"

	mqlog "github.com/nbhaohao/mini-mq/internal/log"
	"github.com/nbhaohao/mini-mq/internal/mq"
)

func main() {
	dir, _ := os.MkdirTemp("", "mqdemo")
	defer os.RemoveAll(dir)
	b, err := mq.NewBroker(dir)
	must(err)

	fmt.Println("=== s1 分区路由:同 key 必落同分区 ===")
	topic, err := b.CreateTopic("orders", 3)
	must(err)
	for _, kv := range [][2]string{{"user-A", "下单1"}, {"user-C", "下单2"}, {"user-A", "下单3"}} {
		p, off, err := topic.Produce([]byte(kv[0]), []byte(kv[1]))
		must(err)
		fmt.Printf("  key=%-7s → partition=%d offset=%d\n", kv[0], p, off)
	}
	fmt.Println("  (user-A 两条落同一分区→同 key 有序;user-C 落到别的分区→不同 key 分散)")

	fmt.Println("\n=== s2 位点提交:consumer group 读到哪了(存进内部 log) ===")
	_, found, _ := b.FetchOffset("billing", "orders", 0)
	fmt.Printf("  billing 初次 FetchOffset → found=%v (没提交过, 从 0 读起)\n", found)
	must(b.CommitOffset("billing", "orders", 0, 2))
	must(b.CommitOffset("billing", "orders", 0, 5)) // 再提交, latest-wins
	off, _, _ := b.FetchOffset("billing", "orders", 0)
	fmt.Printf("  提交 2 再提交 5 后 FetchOffset → %d (latest-wins)\n", off)

	fmt.Println("\n=== s3 可见性超时:领了不 ack, 超时后重投(at-least-once) ===")
	qdir := dir + "/q"
	must(os.MkdirAll(qdir, 0o755))
	part, err := mqlog.NewLog(qdir, mqlog.Config{})
	must(err)
	for _, m := range []string{"job-0", "job-1"} {
		_, err := part.Append([]byte(m))
		must(err)
	}
	d := mq.NewDeliverer(part, 2, 30*time.Second)
	t0 := time.Unix(1000, 0)
	o, v, _, _ := d.Receive(t0)
	fmt.Printf("  T=0s  领取 → offset=%d value=%q (未 ack, 可见性截止 30s)\n", o, v)
	o, v, _, _ = d.Receive(t0.Add(1 * time.Second))
	fmt.Printf("  T=1s  领取 → offset=%d value=%q (未 ack, 可见性截止 31s)\n", o, v)
	_, _, ok, _ := d.Receive(t0.Add(10 * time.Second))
	fmt.Printf("  T=10s 再领 → ok=%v (两条都在飞行且没超时, 无可投)\n", ok)
	o, v, ok, _ = d.Receive(t0.Add(31 * time.Second))
	fmt.Printf("  T=31s 再领 → offset=%d value=%q ok=%v (offset0 超时! 被重投)\n", o, v, ok)
	d.Ack(0)
	fmt.Println("  Ack(0) 之后 offset0 永久确认, 再不会被重投")

	fmt.Println("\n=== s4 消费端幂等:at-least-once 会重复, 靠 messageID 去重 ===")
	dedup := mq.NewDedup()
	for _, id := range []string{"evt-1", "evt-1", "evt-2"} { // evt-1 重复投递
		if dedup.Seen(id) {
			fmt.Printf("  收到 %s → 重复, 跳过\n", id)
		} else {
			fmt.Printf("  收到 %s → 首次, 处理\n", id)
		}
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		os.Exit(1)
	}
}
