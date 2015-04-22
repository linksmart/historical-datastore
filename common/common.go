package common

import (
	"encoding/json"
	"net/http"
)

const (
	// IDSeparator is used for separation of IDs in the URL
	IDSeparator = ","
	// APIVersion defines the API version
	APIVersion = "0.0.1"
)

// Error describes an API error (serializable in JSON)
type Error struct {
	// Code is the (http) code of the error
	Code int `json:"code"`
	// Message is the (human-readable) error message
	Message string `json:"message"`
}

// ErrorResponse writes error to HTTP ResponseWriter
func ErrorResponse(code int, msg string, w http.ResponseWriter) {
	e := &Error{
		code,
		msg,
	}
	b, _ := json.Marshal(e)
	w.Header().Set("Content-Type", "application/json;version="+APIVersion)
	w.WriteHeader(code)
	w.Write(b)
}
