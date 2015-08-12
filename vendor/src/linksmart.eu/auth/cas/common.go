package cas

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Formats error messages for log
func fErr(err error) error {
	return fmt.Errorf("CAS Error: %s", err.Error())
}

// Writes formatted error to HTTP ResponseWriter
func errorResponse(code int, msg string, w http.ResponseWriter) {
	e := map[string]interface{}{
		"code":    code,
		"message": msg,
	}
	b, _ := json.Marshal(&e)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(b)
}
