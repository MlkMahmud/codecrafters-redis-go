package main

import (
	"bytes"
	"fmt"
	"net"
)

var (
	ECHO = []byte("ECHO")
	PING = []byte("PING")
)

func handleRequest(conn net.Conn, input []byte) error {
	entries, err := parse(input)
	
	if err != nil {
		return err
	}

	for i, entriesLen := 0, len(entries); i < entriesLen; {
		command := bytes.ToUpper(entries[i])

		switch {
		case bytes.Equal(command, ECHO):
			if entriesLen <= i+1 {
				conn.Write(generateBulkString([]byte{}))
				i += 1
				continue
			}

			conn.Write(generateBulkString(entries[i+1]))
			i += 2

		case bytes.Equal(command, PING):
			conn.Write(generateString([]byte("PONG")))
			i += 1

		default:
			return fmt.Errorf("unsupported command \"%s\"", command)
		}
	}

	return nil
}
