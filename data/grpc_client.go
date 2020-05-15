package data

import (
	"context"
	"log"
	"net"

	"github.com/farshidtz/senml/v2"
	"github.com/farshidtz/senml/v2/codec"
	data "github.com/linksmart/historical-datastore/data/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type GrpcClient struct {
	Client data.DataClient
}

func NewGrpcClient(serverEndpoint string) (*GrpcClient, error) {
	conn, err := grpc.Dial(serverEndpoint, grpc.WithInsecure())
	client := data.NewDataClient(conn)
	if err != nil {
		return nil, err
	}
	return &GrpcClient{Client: client}, nil
}

func NewBufferClient(listener *bufconn.Listener) (*GrpcClient, error) {
	bufDialer := func(ctx context.Context, s string) (conn net.Conn, err error) {
		return listener.Dial()
	}
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to dial bufnet: %v", err)
	}
	client := data.NewDataClient(conn)
	if err != nil {
		return nil, err
	}
	return &GrpcClient{Client: client}, nil
}

func (c *GrpcClient) Submit(pack senml.Pack) error {
	message := codec.ExportProtobufMessage(pack)
	_, err := c.Client.Submit(context.Background(), &message)
	return err
}
