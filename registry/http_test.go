package registry

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIndex(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(RegistryIndex))
	defer ts.Close()

	res, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	if res.StatusCode != http.StatusOK {
		t.Errorf("Server response is not %v but %v", http.StatusOK, res.StatusCode)
	}

	_, err = ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Error("TODO: check response body")
}

func TestCreate(t *testing.T) {
	t.Error("TODO: API handler test")
}

func TestRetrieve(t *testing.T) {
	t.Error("TODO: API handler test")
}

func TestUpdate(t *testing.T) {
	t.Error("TODO: API handler test")
}

func TestDelete(t *testing.T) {
	t.Error("TODO: API handler test")
}
