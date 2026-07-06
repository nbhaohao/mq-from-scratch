package server

import (
	"context"

	api "github.com/nbhaohao/mini-mq/api/v1"
	"google.golang.org/grpc"
)

// CommitLog：服务端依赖的存储抽象。注意签名 = m01 的 *log.Log 原样满足
// （Append([]byte)(uint64,error) / Read(uint64)([]byte,error)），所以 log 包零改动就能被塞进来。
// 用 interface 而非直接依赖 *log.Log：服务只认「能追加、能按 offset 读」的东西，测试里可换实现。
type CommitLog interface {
	Append([]byte) (uint64, error)
	Read(uint64) ([]byte, error)
}

// Config：已就位（AI 生成）。服务端的依赖注入口，眼下只有一个 commit log。
type Config struct {
	CommitLog CommitLog
}

// grpcServer：已就位（AI 生成）。内嵌 UnimplementedLogServer 满足「向前兼容」——
// proto 以后加新 rpc，老 server 不写也能编译（自动返回 Unimplemented）。
type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

// NewGRPCServer：已就位（AI 生成）。plumbing——建一个 *grpc.Server 并把我们的实现注册进去。
// 调用方 grpc.Server.Serve(listener) 就开始收请求。
func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv := &grpcServer{Config: config}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

// Produce 你来实现（收一条记录，写进 commit log，返回它的 offset）：
// 这是「写」的 RPC 入口。protobuf 已经把网络字节解成了 *api.ProduceRequest，
// 你只管把 Record 里的字节喂给存储层，再把分配到的 offset 包成 response。
//
//	1 off, err := s.CommitLog.Append(req.Record.Value)  // Record.Value 就是 m01 存的裸 []byte
//	2 err != nil → return nil, err
//	3 return &api.ProduceResponse{Offset: off}, nil
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	off, err := s.CommitLog.Append(req.Record.Value)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: off}, nil
}

// Consume 你来实现（按 offset 从 commit log 读一条，包成 Record 返回）：
// 这是「读」的 RPC 入口。存储层返回的是裸 []byte，你要把它 + offset 组装回结构化 Record。
//
//	1 value, err := s.CommitLog.Read(req.Offset)
//	2 err != nil → return nil, err
//	3 return &api.ConsumeResponse{Record: &api.Record{Value: value, Offset: req.Offset}}, nil
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	value, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: &api.Record{Value: value, Offset: req.Offset}}, nil
}

// ConsumeStream 你来实现（服务端流式：从 req.Offset 起，把后续记录一条条推给客户端）：
// unary 的 Consume 一次一条；这里握着一条长连接（stream），循环复用 Consume 把连续记录源源发出。
// 每发一条就把 offset++，直到读不到（越界报错）或客户端断开（ctx.Done）。
//
//	for {
//	  1 select ctx 是否已取消：case <-stream.Context().Done(): return nil
//	  2 res, err := s.Consume(stream.Context(), req)  // 复用上面写好的 Consume
//	  3 err != nil → return err                       // 读到末尾越界会走这里，把流结束掉（s3 会把它变成规范的 gRPC 状态码）
//	  4 stream.Send(res) 出错 → return err
//	  5 req.Offset++                                   // 下一轮读下一条
//	}
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream grpc.ServerStreamingServer[api.ConsumeResponse]) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}

		res, err := s.Consume(stream.Context(), req)
		if err != nil {
			return err
		}

		if err := stream.Send(res); err != nil {
			return err
		}

		req.Offset++
	}
}
