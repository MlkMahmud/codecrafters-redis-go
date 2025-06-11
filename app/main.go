package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
)

func handleConnection(c net.Conn) {
	defer c.Close()
	
	buffer := make([]byte, 512)

	for {
		bytesRead, err := c.Read(buffer)

		if errors.Is(err, io.EOF) {
			fmt.Println("client closed the connection")
			return
		}

		if err != nil {
			fmt.Fprint(os.Stderr, err)
			return
		}

		if bytesRead == 0 {
			continue
		}

		if err := handleRequest(c, buffer[0:bytesRead]); err != nil {
			fmt.Fprint(os.Stderr, err)
			return
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}
