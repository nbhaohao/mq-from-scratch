package log

import (
	"fmt"
	"os"
	"path"
)

// segment：一个 store + 一个 index 配对，是 log 分段的最小单位。
// store 存记录字节流，index 存 offset→pos。段满就整体滚动出新段。
// m01 里记录就是裸 []byte；m02 接 protobuf 后才变成结构化 Record。
type segment struct {
	store      *store
	index      *index
	baseOffset uint64 // 本段第一条记录的绝对 offset
	nextOffset uint64 // 下一条记录将拿到的绝对 offset
	config     Config
}

// newSegment：已就位（AI 生成）。plumbing——按 baseOffset 开 <base>.store 和 <base>.index 两个文件，
// 再从已存在的 index 尾项恢复 nextOffset（段可能是重启后续写的）。
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{baseOffset: baseOffset, config: c}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE, 0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	// 空 index → nextOffset 就是 baseOffset；否则 = base + 最后相对 offset + 1
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append 你来实现（把一条记录写进段：store 落字节 + index 记 offset→pos）：
// 分配当前 nextOffset 给这条记录，返回它。这是「append 得 offset」这条 MQ 铁律的落点。
//
//	1 cur := s.nextOffset（本条记录拿到的绝对 offset，做返回值）
//	2 写 store：_, pos, err := s.store.Append(p)（拿到落在 store 里的 pos）
//	3 写 index：s.index.Write(uint32(s.nextOffset - s.baseOffset), pos)
//	   —— 注意存的是「相对 offset」（相对本段 base），省空间且滚动后仍从 0 算
//	4 s.nextOffset++（下一条往后排）
//	5 return cur, nil
func (s *segment) Append(p []byte) (offset uint64, err error) {
	panic("TODO s3: segment.Append —— store 落字节拿 pos，index 记「相对 offset→pos」，nextOffset++")
}

// Read 你来实现（凭绝对 offset 取回记录：先查 index 拿 pos，再去 store 读）：
//
//	1 查 index：_, pos, err := s.index.Read(int64(off - s.baseOffset))
//	   —— 外面给的是绝对 offset，index 里存的是相对，先减 baseOffset
//	2 去 store 读：s.store.Read(pos)
//	3 return 读到的 []byte
func (s *segment) Read(off uint64) ([]byte, error) {
	panic("TODO s3: segment.Read —— 绝对 offset 减 baseOffset 查 index 拿 pos，再去 store 读")
}

// IsMaxed 你来实现（本段是否该滚动了——store 或 index 任一撑满即满）：
// log 在每次 Append 后问它，返回 true 就开新段。索引项定长，通常 index 先满。
//
//	return s.store.size >= MaxStoreBytes || s.index.size >= MaxIndexBytes
func (s *segment) IsMaxed() bool {
	panic("TODO s3: segment.IsMaxed —— store 或 index 任一超过阈值即返回 true")
}

// Close：已就位（AI 生成）。plumbing——先关 index 再关 store。
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	return s.store.Close()
}

// Remove：已就位（AI 生成）。plumbing——关掉再删两个文件（Truncate 保留给 log 用）。
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	return os.Remove(s.store.Name())
}
