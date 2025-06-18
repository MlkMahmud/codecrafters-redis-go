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

	"github.com/urfave/cli/v2"
)

var (
	config map[string]string
	st     *store
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
			if errors.Is(err, io.EOF) {
				return
			}

			if errors.Is(err, errSyntax) {
				errorMessage := generateErrorString("ERR", err.Error())
				c.Write(errorMessage)
				return
			}

			if err != nil {
				errorMessage := generateErrorString("ERR", "unexpected server error")
				c.Write(errorMessage)
				return
			}

			if err := handleCommands(c, data); err != nil && !errors.Is(err, errInternal) {
				errorMessage := generateErrorString("ERR", err.Error())
				c.Write(errorMessage)
				return
			}
		}
	}
}

func main() {
	app := &cli.App{
		Name: "Redis",
		Action: func(ctx *cli.Context) error {
			config = map[string]string{
				"dir":        ctx.String("dir"),
				"dbfilename": ctx.String("dbfilename"),
			}

			doneC := make(chan os.Signal, 1)
			signal.Notify(doneC, syscall.SIGTERM, syscall.SIGINT)

			listener, err := net.Listen("tcp", "0.0.0.0:6379")

			if err != nil {
				return err
			}

			fmt.Println("Listening on port: 6379")
			ct, cancelFunc := context.WithCancel(context.Background())
			st = newStore()

			go st.init()

			go func() {
				for {
					conn, err := listener.Accept()

					select {
					case <-ctx.Done():
						return

					default:
						if err != nil {
							log.Println(err)
							return
						}

						go handleIncomingConnection(conn, ct)
					}
				}
			}()

			<-doneC
			fmt.Println("shutting down...")

			cancelFunc()
			st.shutdown()
			listener.Close()

			return nil
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "dir",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "dbfilename",
				Required: false,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
