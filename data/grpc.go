package data

import (
	"context"
	"io"
	"log"
	"strings"
	"time"

	"github.com/farshidtz/senml/v2"
	"github.com/farshidtz/senml/v2/codec"
	"github.com/linksmart/historical-datastore/common"
	pbgo "github.com/linksmart/historical-datastore/protobuf/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// API describes the RESTful HTTP data API
type GrpcAPI struct {
	c                Controller
	restrictedAccess bool
}

// Register the Data API to the server
func RegisterGRPCAPI(srv *grpc.Server, c Controller, restricted bool) {
	grpcAPI := &GrpcAPI{
		c:                c,
		restrictedAccess: restricted,
	}
	pbgo.RegisterDataServer(srv, grpcAPI)
}

func (a GrpcAPI) Submit(stream pbgo.Data_SubmitServer) error {
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pbgo.Void{})
		}
		if err != nil {
			return err
		}
		if message == nil {
			return status.Errorf(codes.InvalidArgument, "empty message received")
		}
		senmlPack := codec.ImportProtobufMessage(*message)

		submitErr := a.c.Submit(stream.Context(), senmlPack, nil)
		if submitErr != nil {
			return status.Errorf(submitErr.GrpcStatus(), "Error submitting:"+submitErr.Error())
		}
	}
	return nil
}

func (a GrpcAPI) Query(request *pbgo.QueryRequest, stream pbgo.Data_QueryServer) (err error) {
	var q Query
	q.From, err = parseFromValue(request.From)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Error parsing from value: "+err.Error())
	}

	q.To, err = parseToValue(request.To)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Error parsing to Value: "+err.Error())
	}

	q.Denormalize = DenormMask(request.DenormaMask)
	q.SortAsc = request.SortAsc
	q.PerPage = int(request.RecordPerPacket)
	if q.PerPage > MaxPerPage || q.PerPage == 0 {
		q.PerPage = MaxPerPage
	}
	q.Limit = int(request.Limit)
	q.Offset = int(request.Offset)

	if request.Aggregator != "" {
		q.AggrFunc = strings.ToLower(strings.TrimSpace(request.Aggregator))
		if !common.SupportedAggregate(q.AggrFunc) {
			return status.Errorf(codes.InvalidArgument, "Unknown aggregation function: %s", request.Aggregator)
		}

		q.AggrWindow, err = time.ParseDuration(request.AggrInterval)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "Error parsing aggregation interval %s:%s ", request.AggrInterval, err.Error())
		}
	}
	ctx := stream.Context()
	var sendFunc sendFunction = func(pack senml.Pack) error {
		if ctx.Err() == context.Canceled || ctx.Err() == context.DeadlineExceeded {
			return ctx.Err()
		}
		message := codec.ExportProtobufMessage(pack)
		return stream.Send(&message)
	}

	queryErr := a.c.QueryStream(stream.Context(), q, request.Series, sendFunc)
	if queryErr != nil {
		return status.Errorf(queryErr.GrpcStatus(), "Error querying: "+queryErr.Error())
	}

	return nil
}

func (a GrpcAPI) Count(ctx context.Context, request *pbgo.QueryRequest) (*pbgo.CountResponse, error) {
	var q Query
	var err error
	q.From, err = parseFromValue(request.From)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Error parsing from value: "+err.Error())
	}

	q.To, err = parseToValue(request.To)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Error parsing to Value: "+err.Error())
	}

	q.Limit = int(request.Limit)
	if request.Aggregator != "" {
		q.AggrFunc = strings.ToLower(strings.TrimSpace(request.Aggregator))
		if !common.SupportedAggregate(q.AggrFunc) {
			return nil, status.Errorf(codes.InvalidArgument, "Unknown aggregation function: %s", request.Aggregator)
		}

		q.AggrWindow, err = time.ParseDuration(request.AggrInterval)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "Error parsing aggregation interval %s:%s ", request.AggrInterval, err.Error())
		}
	}

	total, queryErr := a.c.Count(ctx, q, request.Series)
	if queryErr != nil {
		return nil, status.Errorf(queryErr.GrpcStatus(), "Error querying: "+queryErr.Error())
	}
	response := pbgo.CountResponse{Total: int32(total)}
	return &response, nil
}

func (a GrpcAPI) Delete(ctx context.Context, request *pbgo.DeleteRequest) (*pbgo.Void, error) {
	if a.restrictedAccess {
		return &pbgo.Void{}, status.Errorf(codes.PermissionDenied, "data: deleting is not allowed using gRPC")
	}
	from, err := parseFromValue(request.From)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Error parsing from value: "+err.Error())
	}

	to, err := parseToValue(request.To)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Error parsing to Value: "+err.Error())
	}

	deleteErr := a.c.Delete(ctx, request.Series, from, to)
	if deleteErr != nil {
		return nil, status.Errorf(deleteErr.GrpcStatus(), "Error deleting: "+deleteErr.Error())
	}
	return &pbgo.Void{}, nil
}

func (a GrpcAPI) Subscribe(request *pbgo.SubscribeRequest, stream pbgo.Data_SubscribeServer) error {
	names := request.Series
	ch, err := a.c.Subscribe(names...)
	if err != nil {
		return status.Errorf(err.GrpcStatus(), "Error subscribing: %v", err)
	}
	defer a.c.Unsubscribe(ch, names...)

	// Use a temporary buffer to store the subscribed measurements so that the publisher is not
	// stuck because of a slow clients.
	const PubSubBufferSize = 100
	tempCh := make(chan interface{}, PubSubBufferSize)

	ctx := stream.Context()
	go func() {
		defer close(tempCh)
		for pack := range ch {
			if len(tempCh) == PubSubBufferSize {
				log.Printf("pubsub buffer overflow. unsubscribing for the data events: %v", names)
				a.c.Unsubscribe(ch, names...)
				break
			}
			tempCh <- pack
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res := <-tempCh:
			if p, ok := res.(senml.Pack); ok {
				message := codec.ExportProtobufMessage(p)
				if err := stream.Send(&message); err != nil {
					return err
				}
			} else {
				log.Printf("channel closed: streams: %v", names)
				return nil
			}
		}
	}

}
