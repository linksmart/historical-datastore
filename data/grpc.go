package data

import (
	"context"
	"fmt"
	"net"

	senml_protobuf "github.com/farshidtz/senml-protobuf/go"
	"github.com/farshidtz/senml/v2/codec"
	data "github.com/linksmart/historical-datastore/data/proto"
	"github.com/linksmart/historical-datastore/registry"
	"google.golang.org/grpc"
)

// API describes the RESTful HTTP data API
type GrpcAPI struct {
	registry         registry.Storage
	storage          Storage
	server           *grpc.Server
	autoRegistration bool
}

func (a *GrpcAPI) Submit(ctx context.Context, message *senml_protobuf.Message) (*data.Void, error) {
	//panic("implement me")
	if message == nil {
		return &data.Void{}, fmt.Errorf("empty message received")
	}
	senmlPack := codec.ImportProtobufMessage(*message)

	_, err := AddToStorage(senmlPack, a.storage, a.registry, nil, false)
	return &data.Void{}, err
}

// NewAPI returns the configured Data API
func NewGrpcAPI(registry registry.Storage, storage Storage, autoRegistration bool) *GrpcAPI {
	srv := grpc.NewServer()
	grpcAPI := &GrpcAPI{registry, storage, srv, autoRegistration}
	data.RegisterDataServer(srv, grpcAPI)
	return grpcAPI
}

func (a *GrpcAPI) StartGrpcServer(l net.Listener) error {
	return a.server.Serve(l)
}
