package main

import (
	"log"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/server"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name: "Redis",
		Action: func(ctx *cli.Context) error {
			server := server.NewServer(server.ServerConfig{
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
				Name:     "dbfilename",
				Required: false,
			},
			&cli.StringFlag{
				Name:     "dir",
				Required: false,
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
