# 已就位（AI 生成）：改了 api/v1/log.proto 后跑 `make gen` 重新生成 log.pb.go / log_grpc.pb.go。
# 需要 protoc + 两个 Go 插件（一次性安装）：
#   brew install protobuf
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
gen:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       api/v1/log.proto

test:
	go test ./...
