package data

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/farshidtz/senml/v2/codec"
	_go "github.com/linksmart/historical-datastore/protobuf/go"
	"google.golang.org/grpc"
)

type GrpcClient struct {
	Client _go.DataClient
}

type ResponsePack struct {
	p   senml.Pack
	err error
}

func NewGrpcClient(serverEndpoint string) (*GrpcClient, error) {
	conn, err := grpc.Dial(serverEndpoint, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := _go.NewDataClient(conn)
	return &GrpcClient{Client: client}, nil
}

func (c *GrpcClient) Submit(pack senml.Pack) error {
	message := codec.ExportProtobufMessage(pack)
	stream, err := c.Client.Submit(context.Background())
	if err != nil {
		return err
	}
	err = stream.Send(&message)
	if err == io.EOF {
		return fmt.Errorf("unexpected EOF")
	}
	if err != nil {
		return err
	}
	_, err = stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("error receving response: %w", err)
	}
	return nil
}

// TODO facilitate aborting of the query (using channels)
func (c *GrpcClient) Query(seriesNames []string, q Query) (senml.Pack, error) {
	request := _go.QueryRequest{
		Series:          seriesNames,
		From:            q.From.Format(time.RFC3339),
		To:              q.To.Format(time.RFC3339),
		RecordPerPacket: int32(q.PerPage),
		DenormaMask:     _go.DenormMask(q.Denormalize),
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

func (c *GrpcClient) Count(series []string, q Query) (total int, err error) {
	request := _go.QueryRequest{
		Series:          series,
		From:            q.From.Format(time.RFC3339),
		To:              q.To.Format(time.RFC3339),
		RecordPerPacket: int32(q.PerPage),
		DenormaMask:     _go.DenormMask(q.Denormalize),
		SortAsc:         q.SortAsc,
		Limit:           int32(q.Limit),
		Offset:          int32(q.Offset),
	}
	totalResponse, err := c.Client.Count(context.Background(), &request)
	if err != nil {
		return 0, fmt.Errorf("error retrieving the count: %v", err)
	}
	return int(totalResponse.Total), nil
}

func (c *GrpcClient) Delete(seriesNames []string, from time.Time, to time.Time) error {
	request := _go.DeleteRequest{
		Series: seriesNames,
		From:   from.Format(time.RFC3339),
		To:     to.Format(time.RFC3339),
	}
	_, err := c.Client.Delete(context.Background(), &request)
	if err != nil {
		return fmt.Errorf("error deleting: %v", err)
	}
	return nil
}

func (c *GrpcClient) Subscribe(ctx context.Context, seriesNames ...string) (chan ResponsePack, error) {
	request := _go.SubscribeRequest{
		Series: seriesNames,
	}
	stream, err := c.Client.Subscribe(ctx, &request)
	if err != nil {
		return nil, fmt.Errorf("error deleting: %v", err)
	}
	ch := make(chan ResponsePack)
	go func() {
		defer close(ch)
		for {
			message, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				ch <- ResponsePack{p: nil, err: err}
				return
			}
			ch <- ResponsePack{p: codec.ImportProtobufMessage(*message), err: nil}
		}
	}()
	return ch, err
}
