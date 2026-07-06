package mq

import "encoding/json"

// offsetEntry：一条「提交位点」记录。consumer group 读到哪了，就存这么一条。
// 已就位（AI 生成）：这是内部编码细节，不是本关重点。
type offsetEntry struct {
	Group     string `json:"g"`
	Topic     string `json:"t"`
	Partition int    `json:"p"`
	Offset    uint64 `json:"o"`
}

// encodeOffset / decodeOffset：已就位（AI 生成）。用 JSON 把 entry 编解码成字节。
// 生产级会用更紧凑的二进制 + compaction，本课重点不在编码。
func encodeOffset(e offsetEntry) ([]byte, error) { return json.Marshal(e) }
func decodeOffset(b []byte) (offsetEntry, error) {
	var e offsetEntry
	err := json.Unmarshal(b, &e)
	return e, err
}

// CommitOffset 你来实现（把「group 在 topic 的 partition 上读到了 offset」这条位点持久化）：
// 关键认知：位点提交不是改一个可变的 KV，而是往一个内部 log「再追加一条记录」。
// 这正是 Kafka 的做法——__consumer_offsets 就是个普通 topic，提交位点 = 往它写消息。
//
//	1 e := offsetEntry{Group: group, Topic: topic, Partition: partition, Offset: offset}
//	2 data, err := encodeOffset(e)；err != nil 直接 return err
//	3 _, err = b.offsets.Append(data)
//	4 return err
func (b *Broker) CommitOffset(group, topic string, partition int, offset uint64) error {
	e := offsetEntry{Group: group, Topic: topic, Partition: partition, Offset: offset}
	data, err := encodeOffset(e)
	if err != nil {
		return err
	}
	_, err = b.offsets.Append(data)
	return err
}

// FetchOffset 你来实现（读回某 group 在某 topic/partition 上「最后一次」提交的位点）：
// 位点都堆在同一个 log 里（各 group/分区混着）。要的是「最新一条匹配的」——
// 从头扫到尾，匹配 group+topic+partition 的记录里，后写的覆盖先写的（latest-wins）。
// found=false 表示这个 group 从没提交过（消费者据此从 0 开始）。
//
//	1 low, _ := b.offsets.LowestOffset()；high, _ := b.offsets.HighestOffset()
//	2 for off := low; off <= high; off++ {
//	     data, err := b.offsets.Read(off)
//	     if err != nil { break }                 // 空 log 或读到尾，停
//	     e, err := decodeOffset(data)；err != nil 跳过
//	     if e.Group==group && e.Topic==topic && e.Partition==partition {
//	         result = e.Offset; found = true      // 不 break，继续找更晚的
//	     }
//	  }
//	3 return result, found, nil
//
// ponytail: 全表扫描 O(n)，n 小无所谓；Kafka 靠日志压缩(compaction)只留每 key 最新一条来控规模。
func (b *Broker) FetchOffset(group, topic string, partition int) (offset uint64, found bool, err error) {
	low, _ := b.offsets.LowestOffset()
	high, _ := b.offsets.HighestOffset()

	for off := low; off <= high; off++ {
		data, err := b.offsets.Read(off)
		if err != nil {
			break
		}
		e, err := decodeOffset(data)
		if err != nil {
			continue
		}
		if e.Group == group && e.Topic == topic && e.Partition == partition {
			offset = e.Offset
			found = true
		}
	}

	return offset, found, nil
}
