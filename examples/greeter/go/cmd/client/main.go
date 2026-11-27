// Command client calls Greeter at the example's loopback address.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"example.org/greeter/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func run(name string) error {
	conn, err := grpc.NewClient("127.0.0.1:45051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reply, err := pb.NewGreeterClient(conn).SayHello(ctx, &pb.HelloRequest{Name: name})
	if err != nil {
		return err
	}
	fmt.Println(reply.GetGreeting())
	return nil
}

func main() {
	name := "Ada"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}
	if err := run(name); err != nil {
		log.Fatal(err)
	}
}
