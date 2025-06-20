package main

// import (
// 	"bytes"
// 	"fmt"
// 	"net"
// 	"os"
// 	"strconv"
// 	"time"
// )

// var (
// 	ECHO = []byte("ECHO")
// 	GET  = []byte("GET")
// 	PING = []byte("PING")
// 	SET  = []byte("SET")
// )

// func executeCommand(c net.Conn, command []byte, args []any) (int, error) {
// 	switch {
// 	case bytes.Equal(command, PING):
// 		return handlePingCommand(c)

// 	case bytes.Equal(command, ECHO):
// 		return handleEchoCommand(c, args)

// 	case bytes.Equal(command, GET):
// 		return handleGetCommand(c, args)

// 	case bytes.Equal(command, SET):
// 		return handleSetCommand(c, args)

// 	default:
// 		return 0, fmt.Errorf("unsupported command \"%s\"", command)
// 	}
// }

// func handleCommands(c net.Conn, data any) error {
// 	switch v := data.(type) {
// 	// redis client sends commands as an array, however we might sometimes receive a "PING" command as a string.

// 	case []any:
// 		for i := 0; i < len(v); {
// 			switch t := v[i].(type) {
// 			case []byte:
// 				command := bytes.ToUpper(t)

// 				args := v[i+1:]
// 				argsConsumed, err := executeCommand(c, command, args)

// 				if err != nil {
// 					return err
// 				}

// 				i += 1 // add one for the command
// 				i += argsConsumed

// 			default:
// 				return fmt.Errorf("unsupported data type %T", t)
// 			}
// 		}

// 	case []byte:
// 		command := bytes.ToUpper(v)
// 		_, err := executeCommand(c, command, []any{})

// 		return err

// 	default:
// 		return fmt.Errorf("unsupported data type %T", v)
// 	}

// 	return nil
// }

// func handleEchoCommand(c net.Conn, args []any) (int, error) {
// 	if len(args) < 1 {
// 		return 0, fmt.Errorf("\"ECHO\" command requires at least 1 argument")
// 	}

// 	arg, ok := args[0].([]byte)

// 	if !ok {
// 		return 0, fmt.Errorf("\"ECHO\" command argument must be a string argument")
// 	}

// 	response := generateBulkString(string(arg))

// 	if _, err := c.Write(response); err != nil {
// 		fmt.Fprint(os.Stderr, err)
// 		return 1, errInternal
// 	}

// 	return 1, nil
// }

// func handleGetCommand(c net.Conn, args []any) (int, error) {
// 	if len(args) < 1 {
// 		return 0, fmt.Errorf("\"GET\" command requires at least 1 argument")
// 	}

// 	key, ok := args[0].([]byte)

// 	if !ok {
// 		return 0, fmt.Errorf("\"GET\" command argument must be a string")
// 	}

// 	item := st.getItem(string(key))
// 	var response []byte

// 	switch v := item.(type) {
// 	case []byte:
// 		response = generateBulkString(string(v))

// 	case nil:
// 		response = generateNullString()

// 	default:
// 		return 1, fmt.Errorf("unsupported data type")
// 	}

// 	if _, err := c.Write(response); err != nil {
// 		fmt.Fprint(os.Stderr, err)
// 		return 1, errInternal
// 	}

// 	return 1, nil
// }

// func handlePingCommand(c net.Conn) (int, error) {
// 	response := generateSimpleString("PONG")
// 	if _, err := c.Write(response); err != nil {
// 		fmt.Fprint(os.Stderr, err)
// 		return 0, errInternal
// 	}

// 	return 0, nil
// }

// func handleSetCommand(c net.Conn, args []any) (int, error) {
// 	argsLen := len(args)
// 	argsConsumed := 0

// 	if argsLen < 2 {
// 		return argsConsumed, fmt.Errorf("\"SET\" command requires at least 2 arguments")
// 	}

// 	key, ok := args[0].([]byte)

// 	if !ok {
// 		return argsConsumed, fmt.Errorf("\"SET\" command argument must be a string")
// 	}

// 	value := args[1]
// 	var expiry time.Time

// 	if argsLen < 3 {
// 		argsConsumed = 2
// 		expiry = time.Time{}
// 	} else {
// 		switch v := args[2].(type) {
// 		case []byte:
// 			if bytes.Equal(bytes.ToUpper(v), []byte("PX")) {
// 				argsConsumed = 3

// 				if argsLen < 4 {
// 					return argsConsumed, fmt.Errorf("\"SET\" command with \"PX\" option requires an expiry value")
// 				}

// 				var duration time.Duration
// 				argsConsumed = 4

// 				switch px := args[3].(type) {
// 				case int:
// 					duration = time.Duration(px) * time.Millisecond

// 				case []byte:
// 					d, err := strconv.Atoi(string(px))

// 					if err != nil {
// 						return argsConsumed, fmt.Errorf("\"SET\" command \"PX\" options requires an integer expiry value")
// 					}

// 					duration = time.Duration(d) * time.Millisecond

// 				default:
// 					return argsConsumed, fmt.Errorf("\"SET\" command \"PX\" options requires an integer expiry value")
// 				}

// 				expiry = time.Now().Add(duration)
// 			}
// 		}
// 	}

// 	st.setItem(string(key), value, expiry)
// 	response := generateSimpleString("OK")

// 	if _, err := c.Write(response); err != nil {
// 		fmt.Fprint(os.Stderr, err)
// 		return argsConsumed, errInternal
// 	}

// 	return argsConsumed, nil
// }
