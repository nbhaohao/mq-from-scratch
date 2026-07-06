// 已就位（AI 生成）：m02 红测试。每关 describe 用独立 Test 函数，方便 -run 过滤：
//
//	s1 → TestServerProduceConsume    s2 → TestServerConsumeStream    s3 → TestServerConsumePastBoundary
package server

import (
	"context"
	"net"
	"testing"

	api "github.com/nbhaohao/mini-mq/api/v1"
	"github.com/nbhaohao/mini-mq/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// setupTest：起一个真 gRPC server（随机端口），返回连上它的 client + 清理函数。
// 每个测试都跑在真网络往返上——这才验证得了「服务化」，而不是直接调函数。
func setupTest(t *testing.T) (api.LogClient, func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	clog, err := log.NewLog(t.TempDir(), log.Config{})
	require.NoError(t, err)

	server, err := NewGRPCServer(&Config{CommitLog: clog})
	require.NoError(t, err)
	go func() { _ = server.Serve(l) }()

	conn, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	client := api.NewLogClient(conn)

	teardown := func() {
		server.Stop()
		_ = conn.Close()
		_ = l.Close()
		_ = clog.Close()
	}
	return client, teardown
}

// s1：produce 一条，再 consume 同一个 offset，字节要原样回来。
func TestServerProduceConsume(t *testing.T) {
	client, teardown := setupTest(t)
	defer teardown()
	ctx := context.Background()

	want := []byte("hello mini-mq")
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: want}})
	require.NoError(t, err)
	require.Equal(t, uint64(0), produce.Offset)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want, consume.Record.Value)
	require.Equal(t, produce.Offset, consume.Record.Offset)
}

// s2：produce 三条，ConsumeStream 从 0 起要按顺序流回这三条。
func TestServerConsumeStream(t *testing.T) {
	client, teardown := setupTest(t)
	defer teardown()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	records := [][]byte{[]byte("m0"), []byte("m1"), []byte("m2")}
	for _, r := range records {
		_, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: r}})
		require.NoError(t, err)
	}

	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	require.NoError(t, err)
	for i, want := range records {
		res, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, want, res.Record.Value)
		require.Equal(t, uint64(i), res.Record.Offset)
	}
}

// s3：读一个还不存在的 offset，客户端拿到的 err 必须是规范的 gRPC 状态码 OutOfRange，
// 而不是被 gRPC 兜底成 Unknown 的一坨裸 error 字符串。
func TestServerConsumePastBoundary(t *testing.T) {
	client, teardown := setupTest(t)
	defer teardown()
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("only one")}})
	require.NoError(t, err)

	// offset+1 还没写入 → 越界
	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	require.Nil(t, consume)
	require.Error(t, err)
	require.Equal(t, codes.OutOfRange, status.Code(err))
}
