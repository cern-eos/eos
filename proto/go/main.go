// Package main exports the example reverse-proxy implementation
// as a C/C++ shared library.
package main

import (
	"C"
	"context"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"eos_gateway/gateway"
)

type server struct {
	ch     <-chan error
	cancel func()
}

var (
	servers = make(map[int]*server)
)

//export SpawnGrpcGateway
func SpawnGrpcGateway(listenAddr, network, endpoint, swaggerDir *C.char) int {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	opts := gateway.Options{
		Addr: C.GoString(listenAddr),
		GRPCServer: gateway.Endpoint{
			Network: C.GoString(network),
			Addr:    C.GoString(endpoint),
		},
		OpenAPIDir: C.GoString(swaggerDir),
	}
	ch := make(chan error, 1)
	fmt.Println("Preparing to start gateway!")
	go func(ch chan<- error) {
		defer close(ch)
		fmt.Println("Before calling run!")
		if err := gateway.Run(ctx, opts); err != nil {
			fmt.Println("Failed after calling run!")
			glog.Error("grpc-gateway failed with an error: %v", err)
			ch <- err
		}
	}(ch)

	fmt.Println("Successful after calling run!")
	handle := len(servers) + 1
	servers[handle] = &server{
		ch:     ch,
		cancel: cancel,
	}
	return handle
}

//export WaitForGrpcGateway
func WaitForGrpcGateway(handle int) bool {
   	s, ok := servers[handle]
	if !ok {
		glog.Errorf("invalid handle: %d", handle)
		return false
	}
	s.cancel()
	err := <-s.ch
	return err == nil
}

func main() {
	flag.Parse()
	defer glog.Flush()
}
