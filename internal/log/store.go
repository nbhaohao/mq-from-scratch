package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// enc：所有定长头统一大端序，读写两端必须一致，否则 offset/长度全乱。
var enc = binary.BigEndian

// lenWidth：每条记录前的「长度头」占 8 字节（uint64）。
const lenWidth = 8

// store：一条 append-only 文件。写走 bufio 缓冲，读前先 flush。
// 这就是 commit log 的最底层——你在 db 课 0104 写过的 WAL fsync，是同一个东西。
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore：已就位（AI 生成）。构造器/plumbing——包住一个 *os.File，
// 记下当前大小（可能是已存在文件，续写用）。
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	return &store{
		File: f,
		size: uint64(fi.Size()),
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append 你来实现（这是 commit log 的心脏：定长头 + payload 追加写）：
// 追加一条记录 p，返回写入字节数 n、写入起始位置 pos。pos 之后要交给 index 记住。
//
//	1 加锁（并发写同一文件要串行）
//	2 pos = 当前 size（本条记录的落点，先存下来做返回值）
//	3 先写 8 字节大端「长度头」= len(p)（binary.Write(s.buf, enc, uint64(len(p)))）
//	4 再写 payload 本身（s.buf.Write(p)）
//	5 n = 实际写入 payload 字节 + lenWidth（头也算进这条记录的宽度）
//	6 size += n（下一条记录接着往后落）
//	7 返回 n, pos, nil
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	panic("TODO s1: store.Append —— 定长头 + payload 追加写")
}

// Read 你来实现（凭 pos 把一条记录读回来——先读头知道多长，再读那么多）：
// 给定 Append 返回过的 pos，读出原始 payload。
//
//	1 加锁
//	2 buf.Flush()——缓冲里可能还压着没落盘的数据，读前必须刷出去，否则读到旧内容
//	3 读 lenWidth 字节的长度头：make([]byte, lenWidth) + s.File.ReadAt(size, int64(pos))
//	4 按头里的数字 enc.Uint64(size) 分配 payload 缓冲
//	5 从 pos+lenWidth 处 ReadAt 读出 payload
//	6 返回 payload
func (s *store) Read(pos uint64) ([]byte, error) {
	panic("TODO s1: store.Read —— 先读 8 字节长度头，再读那么多 payload")
}

// Close：已就位（AI 生成）。plumbing——落盘缓冲再关文件。
// 顺序不能反：先 Flush（把 buf 写进 OS）再 Close，否则丢数据。
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
