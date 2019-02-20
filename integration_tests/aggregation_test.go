package integration_tests

import (
	"fmt"
	"testing"
)

func TestFun(t *testing.T) {
	n, _ := fmt.Print("Endpoint " + endpoint)
	if n == 0 {
		t.Fail()
	}
	t.Fail()
}
