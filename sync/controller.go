package sync

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"strings"
	"time"

	"github.com/linksmart/historical-datastore/common"
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

	syncInterval  time.Duration
	destinatinURL string

	SyncMap map[string]*Synchronization
}

func NewController(dataController *data.Controller, regController *registry.Controller, syncConf common.SyncConf, pkiConf common.PKIConf) (*Controller, error) {
	controller := new(Controller)
	var err error
	controller.destinatinURL = syncConf.Destination
	controller.syncInterval, err = time.ParseDuration(syncConf.SyncInterval)
	if err != nil {
		return nil, fmt.Errorf("unable to parse synchronization interval:%w", err)
	}
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
	log.Println("Connecting to source using bufConn")
	conn, err := grpc.DialContext(context.Background(), "", grpc.WithContextDialer(bufDialer), grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("error dialing to the source: %w", err)
	}
	controller.srcDataClient = data.NewGrpcClientFromConnection(conn)
	controller.srcRegistryClient = registry.NewGrpcClientFromConnection(conn)

	//connect to the destination server
	creds, err := getClientTransportCredentials(syncConf, pkiConf)
	if err != nil {
		return nil, fmt.Errorf("error getting transport credentials: %w", err)
	}

	conn, err = grpc.Dial(syncConf.Destination, grpc.WithTransportCredentials(*creds))
	if err != nil {
		return nil, fmt.Errorf("error dialing to the destination: %w", err)
	}

	controller.dstDataClient = data.NewGrpcClientFromConnection(conn)
	controller.dstRegistryClient = registry.NewGrpcClientFromConnection(conn)

	controller.SyncMap = make(map[string]*Synchronization)
	return controller, nil
}

func getClientTransportCredentials(syncConf common.SyncConf, pki common.PKIConf) (*credentials.TransportCredentials, error) {
	serverCertFile := pki.ServerCert
	serverPrivatekey := pki.ServerKey
	caFile := pki.CaCert
	// Load the certificates from disk
	certificate, err := tls.LoadX509KeyPair(serverCertFile, serverPrivatekey)
	if err != nil {
		return nil, fmt.Errorf("could not load server key pair: %s", err)
	}

	// Create a certificate pool from the certificate authority
	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("could not read ca certificate: %s", err)
	}

	// Append the client certificates from the CA
	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, fmt.Errorf("failed to append client certs")
	}
	host := syncConf.Destination
	if strings.Contains(syncConf.Destination, ":") {
		host, _, err = net.SplitHostPort(host)
		if err != nil {
			return nil, fmt.Errorf("error splitting the port and host name from %s: %v", syncConf.Destination, err)
		}
	}
	creds := credentials.NewTLS(&tls.Config{
		ServerName:   host,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      certPool,
	})
	return &creds, nil
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
		if page == 1 {
			remaining = total
		}
		remaining = remaining - len(seriesList)
		for _, series := range seriesList {
			c.SyncMap[series.Name] = newSynchronization(series.Name, c.srcDataClient, c.dstDataClient, c.syncInterval)
		}
		page += 1
	}
	return nil
}

func (c Controller) UpdateSync(series string) {
	//TODO:
	// Check if the synchronization is enabled or not.
	// If disabled, disable the enabled thread
	// If enabled, enable the disabled thread
}

func (c Controller) StopSyncForAll() {
	for _, s := range c.SyncMap {
		s.clear()
	}
}
