package mq

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"

	"github.com/nbhaohao/mini-mq/internal/log"
)

// Topic：一个逻辑消息流，底下切成 N 个 partition。每个 partition 就是 m01 的一个 *log.Log。
// 分区是 Kafka 横向扩展与并行消费的基础：不同分区可放不同机器、被不同消费者并行读。
// 代价：全局顺序没了，只保证「同一 partition 内有序」——所以按 key 路由让相关消息落同一分区。
type Topic struct {
	Name       string
	partitions []*log.Log
}

// Broker：持有多个 topic 的顶层门面（单机版）。dir 下每个 topic 一个子目录，
// 子目录里每个 partition 一个 <name>-<i> 的 log 目录。offsets 是 s2 用的内部 log。
type Broker struct {
	dir     string
	topics  map[string]*Topic
	offsets *log.Log // s2：consumer group 的提交位点存这（类比 Kafka __consumer_offsets）
}

// NewBroker：已就位（AI 生成）。plumbing——建 broker，顺带开一个内部 offsets log（s2 才用）。
func NewBroker(dir string) (*Broker, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	offDir := filepath.Join(dir, "__consumer_offsets")
	if err := os.MkdirAll(offDir, 0o755); err != nil {
		return nil, err
	}
	off, err := log.NewLog(offDir, log.Config{})
	if err != nil {
		return nil, err
	}
	return &Broker{dir: dir, topics: map[string]*Topic{}, offsets: off}, nil
}

// CreateTopic：已就位（AI 生成）。plumbing——为 topic 建 numPartitions 个独立 log 目录。
func (b *Broker) CreateTopic(name string, numPartitions int) (*Topic, error) {
	if numPartitions < 1 {
		return nil, fmt.Errorf("numPartitions must be >= 1")
	}
	t := &Topic{Name: name, partitions: make([]*log.Log, numPartitions)}
	for i := 0; i < numPartitions; i++ {
		pdir := filepath.Join(b.dir, name, fmt.Sprintf("partition-%d", i))
		if err := os.MkdirAll(pdir, 0o755); err != nil {
			return nil, err
		}
		l, err := log.NewLog(pdir, log.Config{})
		if err != nil {
			return nil, err
		}
		t.partitions[i] = l
	}
	b.topics[name] = t
	return t, nil
}

// NumPartitions：已就位（AI 生成）。
func (t *Topic) NumPartitions() int { return len(t.partitions) }

// partitionFor 你来实现（把一个 key 稳定地映射到某个 partition 编号）：
// MQ 的路由核心。要求：① 同一个 key 每次都算到同一个分区（否则同 key 消息乱序）；
// ② key 大致均匀散布到各分区。用 hash(key) % 分区数 就同时满足这两点。
//
//	0 实现时 import "hash/fnv"
//	1 h := fnv.New32a()
//	2 h.Write(key)
//	3 return int(h.Sum32() % uint32(len(t.partitions)))
//
// 注意：空 key（len==0）也能算（hash 出固定值，全落同一分区）——真 Kafka 空 key 走轮询，本课简化为按 hash。
func (t *Topic) partitionFor(key []byte) int {
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(len(t.partitions)))
}

// Produce 你来实现（按 key 路由到某分区，把 value 追加进那个分区的 log）：
// 返回落在哪个分区、拿到的分区内 offset。这就是「往 topic 写一条消息」。
//
//	1 p := t.partitionFor(key)
//	2 off, err := t.partitions[p].Append(value)
//	3 return p, off, err
func (t *Topic) Produce(key, value []byte) (partition int, offset uint64, err error) {
	p := t.partitionFor(key)
	off, err := t.partitions[p].Append(value)
	return p, off, err
}

// Consume：已就位（AI 生成）。从指定分区按 offset 读一条（薄封装，路由已由 Produce 决定）。
func (t *Topic) Consume(partition int, offset uint64) ([]byte, error) {
	if partition < 0 || partition >= len(t.partitions) {
		return nil, fmt.Errorf("partition %d out of range", partition)
	}
	return t.partitions[partition].Read(offset)
}
