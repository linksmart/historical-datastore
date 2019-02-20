package integration_tests

import (
	"os"
	"testing"
)

var (
	endpoint         string
	registryEndpoint string
	dataEndpoint     string
)

func TestMain(m *testing.M) {
	endpoint = os.Getenv("HDS_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:8085"
	}
	registryEndpoint = endpoint + "/registry"
	dataEndpoint = endpoint + "/data"

	os.Exit(m.Run())
}
