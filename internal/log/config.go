// 已就位（AI 生成）：段配置。滚动阈值 + 起始 offset，段/log 都读它。
package log

type Config struct {
	Segment struct {
		MaxStoreBytes uint64 // store 文件超过它 → 段满，滚动新段
		MaxIndexBytes uint64 // index 文件超过它 → 段满（索引项定长，先满的往往是它）
		InitialOffset uint64 // 空目录首段的 baseOffset
	}
}
