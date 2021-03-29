package main

import (
	"context"
	"log"
	"time"

	"github.com/linksmart/historical-datastore/common"
	"google.golang.org/grpc"
)

// Unary log Interceptor
func UnaryLogInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, srv interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		h, err := handler(ctx, srv)

		if err != nil || common.Debug {
			log.Printf("%s gRPC Unary %s err:%v", info.FullMethod, time.Since(start), err)
		}

		return h, err
	}

}

// Stream log Interceptor
func StreamLogInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()

		err := handler(srv, stream)

		if err != nil || common.Debug {
			var streamText string
			if info.IsClientStream {
				streamText = "cli"
			} else {
				streamText = "srv"
			}
			log.Printf("%s gRPC %s stream %s err:%v", info.FullMethod, streamText, time.Since(start), err)
		}

		return err
	}
}
