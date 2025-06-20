package main

import (
	"log"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/server"
	"github.com/urfave/cli/v2"
)

// func handleIncomingConnection(c net.Conn, ctx context.Context) {
// 	defer c.Close()
// 	reader := bufio.NewReader(c)

// 	for {
// 		data, err := parseRespData(reader)

// 		select {
// 		case <-ctx.Done():
// 			return

// 		default:
// 			if errors.Is(err, io.EOF) {
// 				return
// 			}

// 			if errors.Is(err, errSyntax) {
// 				errorMessage := generateErrorString("ERR", err.Error())
// 				c.Write(errorMessage)
// 				return
// 			}

// 			if err != nil {
// 				errorMessage := generateErrorString("ERR", "unexpected server error")
// 				c.Write(errorMessage)
// 				return
// 			}

// 			if err := handleCommands(c, data); err != nil && !errors.Is(err, errInternal) {
// 				errorMessage := generateErrorString("ERR", err.Error())
// 				c.Write(errorMessage)
// 				return
// 			}
// 		}
// 	}
// }

func main() {
	app := &cli.App{
		Name: "Redis",
		Action: func(ctx *cli.Context) error {
			server := server.NewServer(server.ServerConfig{
				Host: ctx.String("host"),
				HZ:   ctx.Int("hz"),
				Port: ctx.Int("port"),
			})

			server.SetConfigProperty("dir", ctx.String("dir"))
			server.SetConfigProperty("dbfilename", ctx.String("dbfilename"))

			if err := server.Start(); err != nil {
				return err
			}

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
			&cli.StringFlag{
				Name:     "host",
				Required: false,
				Value:    "0.0.0.0",
			},
			&cli.IntFlag{
				Name:     "hz",
				Required: false,
				Value:    5000,
			},
			&cli.IntFlag{
				Name:    "port",
				Aliases: []string{"p"},
				Value:   6379,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
