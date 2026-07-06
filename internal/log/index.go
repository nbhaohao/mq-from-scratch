package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// 每条索引项定长 12 字节：4 字节相对 offset + 8 字节 store 内 pos。
// 定长是关键——第 i 条项永远在 i*entWidth 处，读时直接乘法定位，不用扫描。
var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

// index：offset → store 位置的映射，内存映射（mmap）一个文件。
// mmap 让我们像操作内存切片一样读写文件，OS 负责回盘——比每次 read/write 系统调用快。
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64 // 已写入的字节数（= 项数 * entWidth），也是下一条项的落点
}

// newIndex：已就位（AI 生成）。plumbing——把文件预扩到 MaxIndexBytes 再 mmap。
// 为什么先 Truncate 撑大：mmap 不能映射超过文件大小的区域，得先占好位；关闭时再截回真实 size。
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Read 你来实现（第 in 条索引项 → 它的相对 offset 和 store 内 pos）：
// in==-1 表示「最后一条」（段重启时用它恢复 nextOffset）。越界返回 io.EOF。
//
//	1 若 size==0（空索引）直接 return 0,0,io.EOF
//	2 若 in==-1：out = (size/entWidth) - 1（最后一条的序号）；否则 out = uint32(in)
//	3 算这条项在文件里的字节偏移：pos = uint64(out) * entWidth
//	4 越界检查：若 size < pos+entWidth 说明这条项不存在 → return 0,0,io.EOF
//	5 从 mmap 切片里解码：out = enc.Uint32(mmap[pos : pos+offWidth])
//	6 pos = enc.Uint64(mmap[pos+offWidth : pos+entWidth])
//	7 return out, pos, nil
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	var entryIdx uint32
	if in == -1 {
		entryIdx = uint32(i.size/entWidth) - 1
	} else {
		entryIdx = uint32(in)
	}

	byteOff := uint64(entryIdx) * entWidth
	if i.size < byteOff+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[byteOff : byteOff+offWidth])
	pos = enc.Uint64(i.mmap[byteOff+offWidth : byteOff+entWidth])

	return out, pos, nil
}

// Write 你来实现（追加一条 offset→pos 映射到 mmap 尾部）：
// 索引项永远顺序追加，写在当前 size 处。写满了返回 io.EOF（段该滚动了）。
//
//	1 容量检查：若 len(mmap) < size+entWidth，没地方写了 → return io.EOF
//	2 写相对 offset：enc.PutUint32(mmap[size : size+offWidth], off)
//	3 写 pos：enc.PutUint64(mmap[size+offWidth : size+entWidth], pos)
//	4 size += entWidth（下一条项往后挪）
//	5 return nil
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth

	return nil
}

// Close：已就位（AI 生成）。plumbing——刷 mmap、刷文件、把撑大的文件截回真实 size 再关。
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

// Name：已就位（AI 生成）。段删除时要拿文件名去 os.Remove。
func (i *index) Name() string {
	return i.file.Name()
}
