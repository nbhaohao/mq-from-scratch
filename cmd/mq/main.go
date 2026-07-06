// 已就位（AI 生成）：mini-mq 的体感 CLI。三条子命令，开两个终端就能 produce/consume 对发。
//
//	终端 A:  go run ./cmd/mq serve                       # 起 gRPC 服务，log 落在 ./mq-data
//	终端 B:  go run ./cmd/mq produce "hello"             # 写一条，打印分到的 offset
//	         go run ./cmd/mq consume 0                    # 读 offset=0 那条
//	         go run ./cmd/mq stream 0                     # 从 offset=0 起流式跟读（Ctrl-C 停）
//
// 纯 plumbing——网络往返都靠你在 server.go 里实现的 RPC。CLI 本身没有业务逻辑。
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"time"

	api "github.com/nbhaohao/mini-mq/api/v1"
	"github.com/nbhaohao/mini-mq/internal/log"
	"github.com/nbhaohao/mini-mq/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const addr = "127.0.0.1:8400"

func main() {
	if len(os.Args) < 2 {
		fmt.Println("用法: mq serve | produce <msg> | consume <offset> | stream <offset>")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "serve":
		serve()
	case "produce":
		produce(os.Args[2])
	case "consume":
		consume(os.Args[2])
	case "stream":
		stream(os.Args[2])
	default:
		fmt.Println("未知命令:", os.Args[1])
		os.Exit(1)
	}
}

func serve() {
	must(os.MkdirAll("mq-data", 0o755))
	clog, err := log.NewLog("mq-data", log.Config{})
	must(err)
	srv, err := server.NewGRPCServer(&server.Config{CommitLog: clog})
	must(err)
	ln, err := net.Listen("tcp", addr)
	must(err)
	fmt.Println("mini-mq 服务已启动，监听", addr, "（log 目录 ./mq-data）")
	must(srv.Serve(ln))
}

func dial() api.LogClient {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	must(err)
	return api.NewLogClient(conn)
}

func produce(msg string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := dial().Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte(msg)}})
	must(err)
	fmt.Printf("已写入 → offset=%d\n", res.Offset)
}

func consume(offStr string) {
	off := parseOffset(offStr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := dial().Consume(ctx, &api.ConsumeRequest{Offset: off})
	if err != nil {
		fmt.Println("读取失败:", err) // s3 后这里会是规范的 OutOfRange 状态
		os.Exit(1)
	}
	fmt.Printf("offset=%d  value=%q\n", res.Record.Offset, res.Record.Value)
}

func stream(offStr string) {
	off := parseOffset(offStr)
	s, err := dial().ConsumeStream(context.Background(), &api.ConsumeRequest{Offset: off})
	must(err)
	for {
		res, err := s.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println("流结束:", err)
			return
		}
		fmt.Printf("offset=%d  value=%q\n", res.Record.Offset, res.Record.Value)
	}
}

func parseOffset(s string) uint64 {
	off, err := strconv.ParseUint(s, 10, 64)
	must(err)
	return off
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "错误:", err)
		os.Exit(1)
	}
}
