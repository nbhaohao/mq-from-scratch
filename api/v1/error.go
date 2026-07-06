package v1

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrOffsetOutOfRange：一个「懂 gRPC」的领域错误。
// 问题背景：普通 Go error 跨 gRPC 边界时，客户端只会收到 codes.Unknown + 一坨错误字符串，
// 没法按类型编程判断「是不是越界」。实现了 GRPCStatus() 的 error 则会被 gRPC 识别，
// 把你指定的状态码/消息原样传给客户端（status.Code(err) 就能拿到 OutOfRange）。
type ErrOffsetOutOfRange struct {
	Offset uint64
}

// GRPCStatus 你来实现（返回一个带 codes.OutOfRange 的 *status.Status）：
// gRPC 在序列化 error 时会先看它有没有这个方法，有就用它给的状态码，而不是兜底成 Unknown。
//
//	1 import "google.golang.org/grpc/codes"（实现时补上这个 import）
//	2 return status.New(codes.OutOfRange, fmt.Sprintf("offset out of range: %d", e.Offset))
func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	return status.New(codes.OutOfRange, fmt.Sprintf("offset out of range: %d", e.Offset))
}

// Error：已就位（AI 生成）。满足 error 接口，纯文本消息（给日志/人看）。
func (e ErrOffsetOutOfRange) Error() string {
	return fmt.Sprintf("offset out of range: %d", e.Offset)
}
