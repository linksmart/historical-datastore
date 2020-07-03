package data

import (
	"context"
	"net"

	senml_protobuf "github.com/farshidtz/senml-protobuf/go"
	"github.com/farshidtz/senml/v2"
	"github.com/farshidtz/senml/v2/codec"
	data "github.com/linksmart/historical-datastore/data/proto"
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
	data.RegisterDataServer(srv, grpcAPI)
	return grpcAPI
}

func (a *GrpcAPI) StartGrpcServer(l net.Listener) error {
	return a.server.Serve(l)
}

func (a *GrpcAPI) StopGrpcServer() {
	a.server.Stop()
}

func (a *GrpcAPI) Submit(ctx context.Context, message *senml_protobuf.Message) (*data.Void, error) {
	void := &data.Void{}
	if message == nil {
		return void, status.Errorf(codes.InvalidArgument, "empty message received")
	}
	senmlPack := codec.ImportProtobufMessage(*message)

	err := a.c.submit(senmlPack, nil)
	if err != nil {
		return void, status.Errorf(err.GrpcStatus(), "Error submitting:"+err.Error())
	}
	return void, nil
}

func (a *GrpcAPI) Query(request *data.QueryRequest, stream data.Data_QueryServer) (err error) {
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

	var sendFunc SendFunction = func(pack senml.Pack) error {
		ctx := stream.Context()
		if ctx.Err() == context.Canceled || ctx.Err() == context.DeadlineExceeded {
			return ctx.Err()
		}
		message := codec.ExportProtobufMessage(pack)
		return stream.Send(&message)
	}

	queryErr := a.c.QueryStream(q, request.Streams, sendFunc)
	if err != nil {
		return status.Errorf(queryErr.GrpcStatus(), "Error querying: "+err.Error())
	}

	return nil
}

func (a *GrpcAPI) Count(ctx context.Context, request *data.QueryRequest) (*data.CountResponse, error) {
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
	total, queryErr := a.c.Count(q, request.Streams)
	if err != nil {
		return nil, status.Errorf(queryErr.GrpcStatus(), "Error querying: "+err.Error())
	}
	response := data.CountResponse{Total: int32(total)}
	return &response, nil
}
