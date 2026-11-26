// Command rpc-interop exercises real grpc-go clients and servers.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"example.org/rpc-interop/pb"
	rpc "github.com/goairix/rpc"
	"github.com/goairix/rpc/config"
	rpcerr "github.com/goairix/rpc/errors"
	"github.com/goairix/rpc/limiter"
	"github.com/goairix/rpc/logger"
	meta "github.com/goairix/rpc/metadata"
	naming "github.com/goairix/rpc/naming/etcd"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const serviceName = "interop.Echo"

type echo struct {
	pb.UnimplementedEchoServer
	meta.UnimplementedServiceRegister
	id string
}

func (s *echo) RegisterMetadata() meta.ServiceRegisterMetadata {
	return meta.ServiceRegisterMetadata{ServiceName: serviceName, Version: "v1"}
}

func (s *echo) reply(m *pb.Message) *pb.Message {
	return &pb.Message{Sequence: m.Sequence, Value: m.Value, Instance: s.id}
}

func (s *echo) Say(ctx context.Context, m *pb.Message) (*pb.Message, error) {
	switch m.Value {
	case "invalid":
		return nil, status.Error(codes.InvalidArgument, "invalid input")
	case "business":
		return nil, rpcerr.New(11001, "business")
	case "wait":
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if md := metadata.ValueFromIncomingContext(ctx, "x-interop"); len(md) > 0 {
		if err := grpc.SetHeader(ctx, metadata.Pairs("x-interop", md[0])); err != nil {
			return nil, err
		}
	}
	return s.reply(m), nil
}

func (s *echo) Upload(stream pb.Echo_UploadServer) error {
	var total int32
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(s.reply(&pb.Message{Sequence: total}))
		}
		if err != nil {
			return err
		}
		total += m.Sequence
	}
}

func (s *echo) Download(m *pb.Message, stream pb.Echo_DownloadServer) error {
	for i := int32(1); i <= m.Sequence; i++ {
		if err := stream.Send(s.reply(&pb.Message{Sequence: i, Value: m.Value})); err != nil {
			return err
		}
	}
	if m.Value == "invalid" {
		return status.Error(codes.InvalidArgument, "invalid input")
	}
	return nil
}

func (s *echo) Chat(stream pb.Echo_ChatServer) error {
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err = stream.Send(s.reply(m)); err != nil {
			return err
		}
	}
}

func etcdConf() config.EtcdConfig {
	return config.EtcdConfig{Endpoints: []string{os.Getenv("ETCD_ENDPOINT")}, Namespace: os.Getenv("RPC_NAMESPACE")}
}

func serve(target, id string) error {
	svc := &echo{id: id}
	if os.Getenv("ETCD_ENDPOINT") == "" {
		lis, err := net.Listen("tcp", target)
		if err != nil {
			return err
		}
		server := grpc.NewServer()
		pb.RegisterEchoServer(server, svc)
		return server.Serve(lis)
	}
	conf := config.ServerConfig{ServiceAddr: target, EtcdConfig: etcdConf(), ServiceReflection: true}
	reg, err := naming.NewEtcdRegistry(&conf)
	if err != nil {
		return err
	}
	defer reg.Close()
	var opts []rpc.ServerOption
	if os.Getenv("RPC_LIMIT_ALL") != "" {
		opts = append(opts, rpc.WithServerLimiter(limiter.NewTokenBucketLimiter(1, 0)))
	}
	server := rpc.NewServer(&conf, reg, opts...)
	if err := server.RegisterService(svc, pb.RegisterEchoServer); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() { <-ctx.Done(); _ = server.Stop() }()
	return server.Serve()
}

func client(target string) (pb.EchoClient, func(), error) {
	var conn *grpc.ClientConn
	var err error
	cleanup := func() {}
	if target == "discovery" {
		conf := etcdConf()
		builder := naming.NewEtcdBuilder(&conf)
		cleanup = func() { _ = builder.Close() }
		conn, err = rpc.NewServiceDiscovery(builder).ServiceConn(serviceName, rpc.WithServiceDiscoveryLB(rpc.WeightedRoundRobin))
	} else {
		conn, err = grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return pb.NewEchoClient(conn), func() { _ = conn.Close(); cleanup() }, nil
}

func check(c pb.EchoClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-interop", "go")
	var headers metadata.MD
	m, err := c.Say(ctx, &pb.Message{Sequence: 7, Value: "hello"}, grpc.Header(&headers), grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	if m.Sequence != 7 || m.Value != "hello" || m.Instance == "" || fmt.Sprint(headers.Get("x-interop")) != "[go]" {
		return fmt.Errorf("bad echo or metadata: %v %v", m, headers)
	}
	up, err := c.Upload(ctx)
	if err != nil {
		return err
	}
	for _, v := range []int32{1, 2} {
		if err = up.Send(&pb.Message{Sequence: v}); err != nil {
			return err
		}
	}
	sum, err := up.CloseAndRecv()
	if err != nil {
		return err
	}
	if sum.Sequence != 3 {
		return fmt.Errorf("bad sum %d", sum.Sequence)
	}
	down, err := c.Download(ctx, &pb.Message{Sequence: 2, Value: "invalid"})
	if err != nil {
		return err
	}
	for i := int32(1); i <= 2; i++ {
		v, e := down.Recv()
		if e != nil {
			return e
		}
		if v.Sequence != i {
			return fmt.Errorf("bad download %v", v)
		}
	}
	if _, err = down.Recv(); status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("stream ended with %v", err)
	}
	chat, err := c.Chat(ctx)
	if err != nil {
		return err
	}
	for i := int32(1); i <= 2; i++ {
		if err = chat.Send(&pb.Message{Sequence: i}); err != nil {
			return err
		}
		v, e := chat.Recv()
		if e != nil {
			return e
		}
		if v.Sequence != i {
			return fmt.Errorf("bad chat %v", v)
		}
	}
	if err = chat.CloseSend(); err != nil {
		return err
	}
	if _, err = chat.Recv(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("chat EOF: %v", err)
	}
	_, err = c.Say(ctx, &pb.Message{Value: "invalid"})
	if status.Code(err) != codes.InvalidArgument {
		return fmt.Errorf("unary error: %v", err)
	}
	short, stop := context.WithTimeout(ctx, 60*time.Millisecond)
	defer stop()
	_, err = c.Say(short, &pb.Message{Value: "wait"})
	if status.Code(err) != codes.DeadlineExceeded {
		return fmt.Errorf("deadline: %v", err)
	}
	fmt.Println(m.Instance)
	return nil
}

func run() error {
	if len(os.Args) < 3 {
		return fmt.Errorf("usage: interop serve|check|sample address|discovery [id|count]")
	}
	mode, target := os.Args[1], os.Args[2]
	if mode == "serve" {
		return serve(target, os.Args[3])
	}
	c, closeClient, err := client(target)
	if err != nil {
		return err
	}
	defer closeClient()
	if mode == "check" {
		return check(c)
	}
	if mode == "limited" {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		_, err := c.Say(ctx, &pb.Message{}, grpc.WaitForReady(true))
		if status.Code(err) != codes.ResourceExhausted {
			return fmt.Errorf("expected resource exhausted: %v", err)
		}
		return nil
	}
	if mode == "sample" {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			n, err := strconv.Atoi(scanner.Text())
			if err != nil {
				return err
			}
			var ids []string
			for i := 0; i < n; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				m, e := c.Say(ctx, &pb.Message{}, grpc.WaitForReady(true))
				cancel()
				if e != nil {
					return e
				}
				ids = append(ids, m.Instance)
			}
			fmt.Println(strings.Join(ids, ","))
		}
		return scanner.Err()
	}
	return fmt.Errorf("unsupported mode %q", mode)
}

func main() {
	logger.Init(false)
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
