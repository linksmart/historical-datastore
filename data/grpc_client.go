package data

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

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

// TODO facilitate aborting of the query (using channels)
func (c *GrpcClient) Query(streams []string, q Query) (senml.Pack, error) {
	request := data.QueryRequest{
		Streams:         streams,
		From:            q.From.Format(time.RFC3339),
		To:              q.To.Format(time.RFC3339),
		RecordPerPacket: int32(q.PerPage),
		DenormaMask:     data.DenormMask(q.Denormalize),
		SortAsc:         q.SortAsc,
		Limit:           int32(q.Limit),
		Offset:          int32(q.Offset),
	}
	stream, err := c.Client.Query(context.Background(), &request)

	records := make(senml.Pack, 0, q.PerPage)
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("can not receive %v", err)
		}
		pack := codec.ImportProtobufMessage(*message)
		records = append(records, pack...)
	}

	return records, err
}


func (c *GrpcClient) Count(streams []string, q Query) (total int, err error) {
	request := data.QueryRequest{
		Streams:         streams,
		From:            q.From.Format(time.RFC3339),
		To:              q.To.Format(time.RFC3339),
		RecordPerPacket: int32(q.PerPage),
		DenormaMask:     data.DenormMask(q.Denormalize),
		SortAsc:         q.SortAsc,
		Limit:           int32(q.Limit),
		Offset:          int32(q.Offset),
	}
	totalResponse, err := c.Client.Count(context.Background(),&request)
	if err != nil {
		return 0,fmt.Errorf("error retrieving the count: %v",err)
	}
	return int(totalResponse.Total),nil
}