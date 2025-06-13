package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func handleIncomingConnection(c net.Conn, ctx context.Context) {
	defer c.Close()
	reader := bufio.NewReader(c)

	for {
		data, err := parseRespData(reader)

		select {
		case <-ctx.Done():
			return

		default:
			if  errors.Is(err, io.EOF) {
				return
			}

			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to parse input: %v", err)
				return
			}

			if err := handleCommands(c, data); err != nil {
				fmt.Fprint(os.Stderr, err)
				return
			}
		}
	}
}

func main() {
	doneC := make(chan os.Signal, 1)
	signal.Notify(doneC, syscall.SIGTERM, syscall.SIGINT)

	listener, err := net.Listen("tcp", "0.0.0.0:6379")

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Listening on port: 6379")
	ctx, cancelFunc := context.WithCancel(context.Background())

	go func() {
		for {
			conn, err := listener.Accept()

			select {
			case <-ctx.Done():
				return

			default:
				if err != nil {
					log.Fatal(err)
				}

				go handleIncomingConnection(conn, ctx)
			}
		}
	}()

	<-doneC
	fmt.Println("shutting down...")

	cancelFunc()
	listener.Close()

	os.Exit(0)
}
