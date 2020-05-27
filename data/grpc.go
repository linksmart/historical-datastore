package data

import (
	"context"
	"net"

	senml_protobuf "github.com/farshidtz/senml-protobuf/go"
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
	//panic("implement me")
	if message == nil {
		return &data.Void{}, status.Errorf(codes.InvalidArgument, "empty message received")
	}
	senmlPack := codec.ImportProtobufMessage(*message)

	err := a.c.submit(senmlPack, nil)
	return &data.Void{}, status.Errorf(err.GrpcStatus(), err.Error())
}

func (a *GrpcAPI) Query(ctx context.Context, request *data.QueryRequest) (response *data.QueryResponse, err error) {
	panic("implement me")
	//from := request.From
}
