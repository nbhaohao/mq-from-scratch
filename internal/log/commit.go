package log

// groupCommitter：group commit / 批量 fsync 的最小实现（m05 s2）。
// commit log 每写一条就 fsync 最安全，但 fsync 是最贵的磁盘同步 syscall——每条一次，吞吐被它锁死。
// 攒够一批记录再 fsync 一次，用「一小段时间窗口内的持久性风险」换「吞吐暴涨」：
// 这正是 Kafka 默认不每条 fsync（log.flush.interval.messages 默认很大、靠副本兜底持久性）、
// 以及多数数据库 WAL「group commit」的同一个取舍。副本兜底的那部分归 6.824。
type groupCommitter struct {
	s       *store
	batch   int // 攒够几条 fsync 一次；1 = 每条都 fsync（最安全也最慢）
	pending int // 距上次 fsync 已攒的条数
}

// newGroupCommitter：已就位（AI 生成）。batch<1 归一到 1（退化成每条 fsync）。
func newGroupCommitter(s *store, batch int) *groupCommitter {
	if batch < 1 {
		batch = 1
	}
	return &groupCommitter{s: s, batch: batch}
}

// Append 你来实现（写一条并做 group commit：攒够 batch 条才 fsync 一次，返回这条的 pos）：
// 每条都要落进 store 缓冲，但昂贵的 fsync 攒够 batch 条才做一次。
//
//	1 _, pos, err := g.s.Append(p)   // 先把这条写进 store（进 bufio / OS 页缓存，还没落盘）
//	2 若 err != nil：return 0, err
//	3 g.pending++
//	4 若 g.pending >= g.batch {       // 攒够一批了
//	5     若 err := g.s.Sync(); err != nil { return 0, err }  // 批量 fsync 一次（真落盘）
//	6     g.pending = 0               // 清零，重新攒下一批
//	7 }
//	8 return pos, nil
func (g *groupCommitter) Append(p []byte) (pos uint64, err error) {
	panic("TODO: s2 · 实现 group commit（攒够 batch 条才 fsync 一次）")
}

// Flush：已就位（AI 生成）。收尾冲刷——把最后不满一批、还压在缓冲里的残余 fsync 落盘。
// 不调它，最后 <batch 条可能只在缓冲/页缓存里，没真正持久化。
func (g *groupCommitter) Flush() error {
	if g.pending == 0 {
		return nil // 上一批刚好 fsync 过，没有残余，不必再来一次
	}
	g.pending = 0
	return g.s.Sync()
}
