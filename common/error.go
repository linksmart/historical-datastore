package common

import (
	"encoding/json"
	"net/http"

	"google.golang.org/grpc/codes"
)

type Error interface {
	error
	HttpStatus() int
	GrpcStatus() codes.Code
}

// Not Found
type InternalError struct{ S string }

func (e *InternalError) Error() string { return e.S }

func (e *InternalError) HttpStatus() int { return http.StatusInternalServerError }

func (e *InternalError) GrpcStatus() codes.Code { return codes.Unknown }

// Not Found
type NotFoundError struct{ S string }

func (e *NotFoundError) Error() string { return e.S }

func (e *NotFoundError) HttpStatus() int { return http.StatusNotFound }

func (e *NotFoundError) GrpcStatus() codes.Code { return codes.NotFound }

// Conflict (non-unique id, assignment to read-only data)
type ConflictError struct{ S string }

func (e *ConflictError) Error() string { return e.S }

func (e *ConflictError) HttpStatus() int { return http.StatusConflict }

func (e *ConflictError) GrpcStatus() codes.Code { return codes.AlreadyExists }

// Bad Request
type BadRequestError struct{ S string }

func (e *BadRequestError) Error() string { return e.S }

func (e *BadRequestError) HttpStatus() int { return http.StatusBadRequest }

func (e *BadRequestError) GrpcStatus() codes.Code { return codes.InvalidArgument }

// Deadline Exceeded Error
type UnsupportedMediaTypeError struct{ S string }

func (e *UnsupportedMediaTypeError) Error() string          { return e.S }
func (e *UnsupportedMediaTypeError) HttpStatus() int        { return http.StatusUnsupportedMediaType }
func (e *UnsupportedMediaTypeError) GrpcStatus() codes.Code { return codes.Unknown }

// HttpErrorResponse writes error to HTTP ResponseWriter
func HttpErrorResponse(err Error, w http.ResponseWriter) {
	// Error describes an API error (serializable in JSON)
	type Error struct {
		// Code is the (http) code of the error
		Code int `json:"code"`
		// Message is the (human-readable) error message
		Message string `json:"message"`
	}

	code := err.HttpStatus()
	e := &Error{
		code,
		err.Error(),
	}
	b, _ := json.Marshal(e)
	w.Header().Set("Content-Type", "application/json;version="+APIVersion)
	w.WriteHeader(code)
	w.Write(b)
}
