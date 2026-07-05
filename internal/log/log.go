package log

import (
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Log：多个 segment 的门面（这是包里唯一对外的类型，首字母大写）。
// 写只写「活跃段」，满了滚新段；读时按 offset 路由到覆盖它的那个段。
// 这就是一个单机 commit log——m03 的 consumer offset 也会存进一个 Log，吃自己的狗粮。
type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

// NewLog：已就位（AI 生成）。plumbing——填默认阈值后 setup。
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{Dir: dir, Config: c}
	return l, l.setup()
}

// setup：已就位（AI 生成）。plumbing——扫目录里的 <baseOffset>.store/.index，
// 按 baseOffset 排序恢复所有段；空目录则建首段。重启后 log 靠它复活。
func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool { return baseOffsets[i] < baseOffsets[j] })
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		i++ // 每个 baseOffset 有 .store 和 .index 两个文件，跳过重复
	}
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

// newSegment：已就位（AI 生成）。plumbing——建段并设为活跃段。
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

// Append 你来实现（写活跃段；写完若满了就滚一个新段）：
// 全局写锁串行化。返回这条记录的绝对 offset。
//
//	1 加写锁 l.mu.Lock() / defer Unlock()
//	2 写活跃段：off, err := l.activeSegment.Append(p)
//	3 若活跃段 IsMaxed()：l.newSegment(off + 1) 开新段（下一条写新段）
//	4 return off, err
func (l *Log) Append(p []byte) (uint64, error) {
	panic("TODO s4: Log.Append —— 写活跃段；若 IsMaxed 则 newSegment(off+1) 滚新段")
}

// Read 你来实现（按 offset 路由到覆盖它的段，再读）：
// 读锁（多读并发）。offset 落在 [baseOffset, nextOffset) 的段才是它的家。
//
//	1 加读锁 l.mu.RLock() / defer RUnlock()
//	2 线性找段：遍历 l.segments，找 baseOffset <= off < nextOffset 的那个
//	3 没找到（s==nil 或 off >= s.nextOffset）→ return nil, fmt.Errorf("offset out of range: %d", off)
//	4 return s.Read(off)
func (l *Log) Read(off uint64) ([]byte, error) {
	panic("TODO s4: Log.Read —— 线性找 baseOffset<=off<nextOffset 的段；找不到报错（记得 import fmt）")
}

// LowestOffset 你来实现（整个 log 最老的可读 offset = 第一个段的 baseOffset）：
// 保留策略删老段后，它会往前走。消费者据它知道「从哪还能读起」。
//
//	读锁；return l.segments[0].baseOffset, nil
func (l *Log) LowestOffset() (uint64, error) {
	panic("TODO s4: Log.LowestOffset —— 第一个段的 baseOffset")
}

// HighestOffset 你来实现（最新已写入的 offset = 最后一个段 nextOffset - 1）：
//
//	读锁；off := 最后段.nextOffset；若 off==0 return 0；否则 return off-1
func (l *Log) HighestOffset() (uint64, error) {
	panic("TODO s4: Log.HighestOffset —— 最后一个段 nextOffset-1（空则 0）")
}

// Close：已就位（AI 生成）。plumbing——挨个关段。
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}
