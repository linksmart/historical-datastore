package data

import (
	"context"

	"github.com/farshidtz/senml/v2"
	"github.com/farshidtz/senml/v2/codec"
	data "github.com/linksmart/historical-datastore/data/proto"
	"google.golang.org/grpc"
)

type GrpcClient struct {
	Client data.DataClient
}

func NewGrpcClient(serverEndpoint string) (*GrpcClient, error) {
	conn, err := grpc.Dial(serverEndpoint, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := data.NewDataClient(conn)
	return &GrpcClient{Client: client}, nil
}

func (c *GrpcClient) Submit(pack senml.Pack) error {
	message := codec.ExportProtobufMessage(pack)
	_, err := c.Client.Submit(context.Background(), &message)
	return err
}
