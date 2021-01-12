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
	_go "github.com/linksmart/historical-datastore/protobuf/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// API describes the RESTful GRPC data API
type GrpcAPI struct {
	c Controller
}

// Register the Data API to the server
func RegisterGRPCAPI(srv *grpc.Server, c Controller) {
	grpcAPI := &GrpcAPI{c: c}
	_go.RegisterDataServer(srv, grpcAPI)
}

func (a GrpcAPI) Submit(stream _go.Data_SubmitServer) error {
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&_go.Void{})
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

func (a GrpcAPI) Query(request *_go.QueryRequest, stream _go.Data_QueryServer) (err error) {
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

func (a GrpcAPI) Count(ctx context.Context, request *_go.QueryRequest) (*_go.CountResponse, error) {
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
	response := _go.CountResponse{Total: int32(total)}
	return &response, nil
}

func (a GrpcAPI) Delete(ctx context.Context, request *_go.DeleteRequest) (*_go.Void, error) {
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
	return &_go.Void{}, nil
}

func (a GrpcAPI) Subscribe(request *_go.SubscribeRequest, stream _go.Data_SubscribeServer) error {
	names := request.Series
	ch, err := a.c.Subscribe(names...)
	if err != nil {
		return status.Errorf(err.GrpcStatus(), "Error subscribing: %v", err)
	}
	defer a.c.Unsubscribe(ch, names...)

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res := <-ch:
			if p, ok := res.(senml.Pack); ok {
				message := codec.ExportProtobufMessage(p)
				if err := stream.Send(&message); err != nil {
					return err
				}
			} else {
				log.Print("channel closed")
				return nil
			}
		}
	}

}
