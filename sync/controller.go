package sync

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/test/bufconn"
)

type Controller struct {
	// srcDataClient is the connection to the source host
	srcDataClient *data.GrpcClient
	// dstDataClient is the connection to the destination host
	dstDataClient *data.GrpcClient
	// srcRegistryClient
	srcRegistryClient *registry.GrpcClient
	// dstRegistryClient
	dstRegistryClient *registry.GrpcClient
}

func NewController(dataController *data.Controller, regController *registry.Controller, destHDSHost string, creds *credentials.TransportCredentials) (*Controller, error) {
	controller := new(Controller)

	// start a bufconn server for registry and data and connect to it
	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)
	// start the bufconn gRPC server
	srv := grpc.NewServer()
	// register APIS
	registry.RegisterGRPCAPI(srv, *regController)
	data.RegisterGRPCAPI(srv, *dataController)
	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	// dial to the server
	bufDialer := func(ctx context.Context, s string) (conn net.Conn, err error) {
		return lis.Dial()
	}

	// get the connections
	conn, err := grpc.DialContext(context.Background(), "", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	controller.srcDataClient = data.NewGrpcClientFromConnection(conn)
	controller.srcRegistryClient = registry.NewGrpcClientFromConnection(conn)

	//connect to the destination server
	conn, err = grpc.Dial(destHDSHost, grpc.WithTransportCredentials(*creds))
	if err != nil {
		return nil, err
	}

	controller.dstDataClient = data.NewGrpcClientFromConnection(conn)
	controller.dstRegistryClient = registry.NewGrpcClientFromConnection(conn)
	return controller, nil
}

func (c Controller) StartSyncForAll() error {
	// Get all the registry entries
	page := 1
	perPage := 100
	remaining := 0
	for do := true; do; do = remaining > 0 {

		seriesList, total, err := c.srcRegistryClient.GetMany(page, perPage)
		if err != nil {
			return fmt.Errorf("error getting registry:%v", err)
		}
		// For each registry entry, check if the synchronization is enabled for that particular time series
		remaining = total - len(seriesList)
		for _, series := range seriesList {

		}
	}
}

func (c Controller) UpdateSync(series string) {
	// Check if the synchronization is enabled or not.
	// If disabled, disable the enabled thread
	// If enabled, enable the disabled thread
}
