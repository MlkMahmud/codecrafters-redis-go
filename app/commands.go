package main

import (
	"bytes"
	"fmt"
	"net"
)

var (
	ECHO = []byte("ECHO")
	GET  = []byte("GET")
	PING = []byte("PING")
	SET  = []byte("SET")
)

var (
	st = newStore()
)

func executeCommand(c net.Conn, command []byte, argv []any) (int, error) {
	switch {
	case bytes.Equal(command, PING):
		{
			if err := handlePingCommand(c); err != nil {
				return 0, err
			}

			return 1, nil
		}

	case bytes.Equal(command, ECHO):
		{
			args, err := getNArgs(1, argv)

			if err != nil {
				return 0, fmt.Errorf("\"ECHO\" command error: %w", err)
			}

			arg, ok := args[0].([]byte)

			if !ok {
				return 0, fmt.Errorf("ECHO argument must be a bulk string, got %T", args[0])
			}

			if err := handleEchoCommand(c, arg); err != nil {
				return 0, err
			}

			return 2, nil
		}

	case bytes.Equal(command, GET):
		{
			args, err := getNArgs(1, argv)

			if err != nil {
				return 0, fmt.Errorf("\"GET\" command error: %w", err)
			}

			key, ok := args[0].([]byte)

			if !ok {
				return 0, fmt.Errorf("GET argument must be a bulk string, got %T", args[0])
			}

			if err := handleGetCommand(c, key); err != nil {
				return 0, err
			}

			return 2, nil
		}

	case bytes.Equal(command, SET):
		{
			args, err := getNArgs(2, argv)

			if err != nil {
				return 0, fmt.Errorf("\"SET\" command error: %w", err)
			}

			key, ok := args[0].([]byte)

			if !ok {
				return 0, fmt.Errorf("SET argument must be a bulk string, got %T", args[0])
			}

			if err := handleSetCommand(c, key, args[1]); err != nil {
				return 0, err
			}

			return 3, nil
		}

	default:
		return 0, fmt.Errorf("unsupported command \"%s\"", command)
	}
}

func handleCommands(c net.Conn, data any) error {
	switch v := data.(type) {
	// redis client sends commands as an array, however we might sometimes receive a "PING" command as a string.

	case []any:
		for i := 0; i < len(v); {
			switch t := v[i].(type) {
			case []byte:
				command := bytes.ToUpper(t)

				args := v[i+1:]
				argsConsumed, err := executeCommand(c, command, args)

				if err != nil {
					return err
				}

				i += argsConsumed

			default:
				return fmt.Errorf("unsupported data type %T", t)
			}
		}

	case []byte:
		command := bytes.ToUpper(v)
		_, err := executeCommand(c, command, []any{})

		return err

	default:
		return fmt.Errorf("unsupported data type %T", v)
	}

	return nil
}

func handleEchoCommand(c net.Conn, arg []byte) error {
	response := generateBulkString(string(arg))
	_, err := c.Write(response)
	return err
}

func handleGetCommand(c net.Conn, key []byte) error {
	item := st.getItem(string(key))
	var response []byte

	switch v := item.(type) {
	case []byte:
		response = generateBulkString(string(v))

	case nil:
		response = generateBulkString("-1")

	default:
		return fmt.Errorf("unsupported data type")
	}

	_, err := c.Write(response)
	return err
}

func handlePingCommand(c net.Conn) error {
	response := generateSimpleString("PONG")
	_, err := c.Write(response)
	return err
}

func handleSetCommand(c net.Conn, key []byte, value any) error {
	st.setItem(string(key), value)
	response := generateSimpleString("OK")
	_, err := c.Write(response)
	return err
}
