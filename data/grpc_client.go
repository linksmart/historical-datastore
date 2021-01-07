package data

import (
	"context"
	"fmt"
	"io"
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
	Pack senml.Pack
	Err  error
}

func NewGrpcClient(serverEndpoint string, opts ...grpc.DialOption) (*GrpcClient, error) {
	conn, err := grpc.Dial(serverEndpoint, opts...)
	if err != nil {
		return nil, err
	}
	client := _go.NewDataClient(conn)
	return &GrpcClient{Client: client}, nil
}

func (c *GrpcClient) Submit(ctx context.Context, pack senml.Pack) error {
	message := codec.ExportProtobufMessage(pack)
	stream, err := c.Client.Submit(ctx)
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

func (c *GrpcClient) Query(ctx context.Context, seriesNames []string, q Query) (senml.Pack, error) {
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
	stream, err := c.Client.Query(ctx, &request)
	if err != nil {
		return nil, fmt.Errorf("error querying: %v", err)
	}
	records := make(senml.Pack, 0, q.PerPage)
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			err = fmt.Errorf("can not receive %v", err)
			break
		}
		pack := codec.ImportProtobufMessage(*message)
		records = append(records, pack...)
	}

	return records, err
}

func (c *GrpcClient) Count(ctx context.Context, series []string, q Query) (total int, err error) {
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
	totalResponse, err := c.Client.Count(ctx, &request)
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
		return nil, fmt.Errorf("error subscribing: %v", err)
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
				ch <- ResponsePack{Pack: nil, Err: err}
				return
			}
			ch <- ResponsePack{Pack: codec.ImportProtobufMessage(*message), Err: nil}
		}
	}()
	return ch, err
}

func (c *GrpcClient) QueryStream(ctx context.Context, seriesNames []string, q Query) (chan ResponsePack, error) {
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
	stream, err := c.Client.Query(ctx, &request)
	if err != nil {
		return nil, fmt.Errorf("error querying stream: %v", err)
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
				ch <- ResponsePack{Pack: nil, Err: err}
				return
			}
			ch <- ResponsePack{Pack: codec.ImportProtobufMessage(*message), Err: nil}
		}
	}()
	return ch, err
}

func (c *GrpcClient) CreateSubmitStream(ctx context.Context) (stream _go.Data_SubmitClient, err error) {
	stream, err = c.Client.Submit(ctx)
	return stream, err
}

func (c *GrpcClient) SubmitToStream(stream _go.Data_SubmitClient, pack senml.Pack) error {
	message := codec.ExportProtobufMessage(pack)
	err := stream.Send(&message)
	if err == io.EOF {
		return fmt.Errorf("unexpected EOF")
	}
	if err != nil {
		return err
	}
	return nil
}

func (c *GrpcClient) CloseSubmitStream(stream _go.Data_SubmitClient) error {
	_, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("error receving response: %w", err)
	}
	return nil
}
