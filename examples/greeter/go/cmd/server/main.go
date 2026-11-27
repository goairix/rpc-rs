// Command server serves the shared Greeter protocol on loopback.
package main

import (
	"context"
	"log"
	"net"
	"strings"

	"example.org/greeter/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type greetings struct {
	pb.UnimplementedGreeterServer
}

// SayHello identifies the responding server in the greeting.
func (greetings) SayHello(_ context.Context, request *pb.HelloRequest) (*pb.HelloReply, error) {
	name := strings.TrimSpace(request.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	return &pb.HelloReply{Greeting: "Hello, " + name + "! (Go)"}, nil
}

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:45051")
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterGreeterServer(server, greetings{})
	healthpb.RegisterHealthServer(server, health.NewServer())
	if err := server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
