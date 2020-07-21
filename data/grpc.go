package data

import (
	"context"
	"net"
	"strings"
	"time"

	senml_protobuf "github.com/farshidtz/senml-protobuf/go"
	"github.com/farshidtz/senml/v2"
	"github.com/farshidtz/senml/v2/codec"
	"github.com/linksmart/historical-datastore/common"
	_go "github.com/linksmart/historical-datastore/protobuf/go"
	"github.com/linksmart/historical-datastore/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// API describes the RESTful HTTP data API
type GrpcAPI struct {
	c      *Controller
	server *grpc.Server
}

// NewAPI returns the configured Data API
func NewGrpcAPI(registry registry.Storage, storage Storage, autoRegistration bool) *GrpcAPI {
	srv := grpc.NewServer()
	grpcAPI := &GrpcAPI{&Controller{registry, storage, autoRegistration}, srv} //TODO: Sharing controller between HTTP and Grpc instead of creating one for both
	_go.RegisterDataServer(srv, grpcAPI)
	return grpcAPI
}

func (a *GrpcAPI) StartGrpcServer(l net.Listener) error {
	return a.server.Serve(l)
}

func (a *GrpcAPI) StopGrpcServer() {
	a.server.Stop()
}

func (a *GrpcAPI) Submit(ctx context.Context, message *senml_protobuf.Message) (*_go.Void, error) {
	if message == nil {
		return nil, status.Errorf(codes.InvalidArgument, "empty message received")
	}
	senmlPack := codec.ImportProtobufMessage(*message)

	err := a.c.submit(senmlPack, nil)
	if err != nil {
		return nil, status.Errorf(err.GrpcStatus(), "Error submitting:"+err.Error())
	}
	return &_go.Void{}, nil
}

func (a *GrpcAPI) Query(request *_go.QueryRequest, stream _go.Data_QueryServer) (err error) {
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
	q.Limit = int(request.Limit)
	q.Offset = int(request.Offset)

	if request.Aggregator != "" {
		q.Aggregator = strings.ToLower(strings.TrimSpace(request.Aggregator))
		if !common.SupportedAggregate(q.Aggregator) {
			return status.Errorf(codes.InvalidArgument, "Unknown aggregation function: %s", request.Aggregator)
		}

		q.AggrInterval, err = time.ParseDuration(request.AggrInterval)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "Error parsing aggregation interval %s:%s ", request.AggrInterval, err.Error())
		}
	}
	var sendFunc sendFunction = func(pack senml.Pack) error {
		ctx := stream.Context()
		if ctx.Err() == context.Canceled || ctx.Err() == context.DeadlineExceeded {
			return ctx.Err()
		}
		message := codec.ExportProtobufMessage(pack)
		return stream.Send(&message)
	}

	queryErr := a.c.QueryStream(q, request.Series, sendFunc)
	if queryErr != nil {
		return status.Errorf(queryErr.GrpcStatus(), "Error querying: "+queryErr.Error())
	}

	return nil
}

func (a *GrpcAPI) Count(ctx context.Context, request *_go.QueryRequest) (*_go.CountResponse, error) {
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
		q.Aggregator = strings.ToLower(strings.TrimSpace(request.Aggregator))
		if !common.SupportedAggregate(q.Aggregator) {
			return nil, status.Errorf(codes.InvalidArgument, "Unknown aggregation function: %s", request.Aggregator)
		}

		q.AggrInterval, err = time.ParseDuration(request.AggrInterval)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "Error parsing aggregation interval %s:%s ", request.AggrInterval, err.Error())
		}
	}

	total, queryErr := a.c.Count(q, request.Series)
	if queryErr != nil {
		return nil, status.Errorf(queryErr.GrpcStatus(), "Error querying: "+queryErr.Error())
	}
	response := _go.CountResponse{Total: int32(total)}
	return &response, nil
}

func (a *GrpcAPI) Delete(ctx context.Context, request *_go.DeleteRequest) (*_go.Void, error) {
	from, err := parseFromValue(request.From)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Error parsing from value: "+err.Error())
	}

	to, err := parseToValue(request.To)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Error parsing to Value: "+err.Error())
	}

	deleteErr := a.c.Delete(request.Series, from, to)
	if deleteErr != nil {
		return nil, status.Errorf(deleteErr.GrpcStatus(), "Error deleting: "+deleteErr.Error())
	}
	return &_go.Void{}, nil
}
