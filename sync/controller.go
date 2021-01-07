package sync

import (
	"fmt"
	"log"
	"net/url"

	"github.com/linksmart/historical-datastore/data"
	"github.com/linksmart/historical-datastore/registry"
	"google.golang.org/grpc"
)

type Controller struct {
	// srcClient is the connection to the source host
	srcClient *data.GrpcClient
	// dstClient is the connection to the destination host
	dstClient *data.GrpcClient
	// srcRegistry
	srcRegistry *registry.GrpcClient
	// dstRegistry
	dstRegistry *registry.GrpcClient
}

func NewController(primaryHDSHost string, cd *certs.CertDirectory) (*Controller, error) {
	controller := new(Controller)
	controller.cd = cd
	creds, err := getCreds(cd, primaryHDSHost)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize the transport credentials for %s: %v", primaryHDSHost, err)
	}
	hostUrl, err := url.Parse(primaryHDSHost)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url %s: %v", primaryHDSHost, err)
	}
	hds, err := data.NewGrpcClient(hostUrl.Host+hostUrl.Path, grpc.WithTransportCredentials(*creds)) //
	if err != nil {
		return nil, fmt.Errorf("unable to connect to %s: %v", primaryHDSHost, err)
	}
	log.Printf("connected to source: %s", primaryHDSHost)
	controller.primaryHDS = hds
	controller.mappingList = map[string]*Synchronization{}
	controller.destConnMap = map[string]*data.GrpcClient{}
	return controller, nil
}