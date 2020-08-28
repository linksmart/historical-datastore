package main

import (
	"net/http"
	"strings"

	"google.golang.org/grpc"
)

type grpcHandler struct {
	grpcSrv *grpc.Server
}

func (g grpcHandler) grpcHandler(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
			g.grpcSrv.ServeHTTP(w, r)
		} else {
			next.ServeHTTP(w, r)
		}
	}
	return http.HandlerFunc(fn)
}
