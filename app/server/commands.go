package server

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

var (
	CONFIG   = "CONFIG"
	ECHO     = "ECHO"
	GET      = "GET"
	INFO     = "INFO"
	KEYS     = "KEYS"
	PING     = "PING"
	PSYNC    = "PSYNC"
	REPLCONF = "REPLCONF"
	SET      = "SET"
)

const (
	RDB_DUMP = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
)

func handleConfigGetCommand(config *Config, args []any) []byte {
	if len(args) == 0 {
		return resp.EncodeError("\"CONFIG GET\" command requires at least one argument")
	}

	entries := [][]byte{}

	for _, arg := range args {
		parameter, ok := arg.([]byte)

		if !ok {
			return resp.EncodeError("\"CONFIG GET\" argument must be a string")
		}

		key := string(parameter)
		value := config.Get(key)

		entries = append(entries, resp.EncodeBulkString(key))

		if value == "" {
			entries = append(entries, resp.EncodeNull())
		} else {
			entries = append(entries, resp.EncodeBulkString(value))
		}
	}

	return resp.EncodeArray(entries)
}

func (s *Server) handleConfigCommand(conn net.Conn, args []any) {
	err := resp.EncodeError("\"CONFIG\" command must be followed by one of the following subcommands \"GET\", \"HELP\", \"RESETSTAT\", \"REWRITE\" or \"SET\"")

	if len(args) == 0 {
		conn.Write(err)
		return
	}

	subcommand, ok := args[0].([]byte)

	if !ok {
		conn.Write(err)
		return
	}

	switch {
	case bytes.Equal(bytes.ToUpper(subcommand), []byte("GET")):
		response := handleConfigGetCommand(s.config, args[1:])
		conn.Write(response)
		return

		//todo: handle other subcommands
	default:
		conn.Write(err)
		return
	}
}

func (s *Server) handleEchoCommand(conn net.Conn, args []any) {
	if len(args) < 1 {
		conn.Write(resp.EncodeError("\"ECHO\" command requires at least 1 argument"))
		return
	}

	arg, ok := args[0].([]byte)

	if !ok {
		conn.Write(resp.EncodeError("\"ECHO\" command argument must be a string argument"))
		return
	}

	response := resp.EncodeBulkString(string(arg))
	conn.Write(response)
}

func (s *Server) handleGetCommand(conn net.Conn, args []any) {
	if len(args) < 1 {
		conn.Write(resp.EncodeError("\"GET\" command requires at least 1 argument"))
		return
	}

	key, ok := args[0].([]byte)

	if !ok {
		conn.Write(resp.EncodeError("\"GET\" command argument must be a string"))
		return
	}

	item := s.cache.GetItem(string(key))
	var response []byte

	switch v := item.(type) {
	case []byte:
		response = resp.EncodeBulkString(string(v))

	case string:
		response = resp.EncodeBulkString(v)

	case nil:
		response = resp.EncodeNull()

	default:
		response = resp.EncodeError("unsupported data type")
	}

	conn.Write(response)
}

func (s *Server) handleInfoCommand(conn net.Conn, args []any) {
	if len(args) < 1 {
		// todo: handle "INFO" command without 'section' argument
		conn.Write(resp.EncodeNull())
		return
	}

	section, ok := args[0].([]byte)

	if !ok {
		conn.Write(resp.EncodeError("\"INFO\" command argument must be a string"))
		return
	}

	if !bytes.Equal(bytes.ToLower(section), []byte("replication")) {
		// todo: handle "INFO" section arguments beyond "replication"
		conn.Write(resp.EncodeNull())
		return
	}

	response := resp.EncodeBulkString(fmt.Sprintf("role:%s\nmaster_replid:%s\nnmaster_repl_offset:%d", s.role, s.replicationId, s.replicationOffset))

	conn.Write(response)
}

func (s *Server) handleKeysCommand(conn net.Conn, args []any) {
	if len(args) < 1 {
		conn.Write(resp.EncodeError("\"KEYS\" command requires at least 1 argument"))
		return
	}

	pattern, ok := args[0].([]byte)

	if !ok {
		conn.Write(resp.EncodeError("\"KEYS\" command argument must be a string"))
		return
	}

	if !bytes.Equal(pattern, []byte("*")) {
		conn.Write(resp.EncodeArray([][]byte{}))
		return
	}

	entries := make([][]byte, s.cache.Size())
	index := 0

	for key := range s.cache.GetItems() {
		entries[index] = resp.EncodeBulkString(key)
		index += 1
	}

	conn.Write(resp.EncodeArray(entries))
}

func (s *Server) handlePingCommand(conn net.Conn) {
	response := resp.EncodeSimpleString("PONG")

	conn.Write(response)
}

func (s *Server) handlePsyncCommand(conn net.Conn) {
	decodedBytes, err := hex.DecodeString(RDB_DUMP)

	if err != nil {
		conn.Write(resp.EncodeError("internal server error"))
		return
	}

	conn.Write(resp.EncodeSimpleString(fmt.Sprintf("FULLRESYNC %s 0", s.replicationId)))
	conn.Write(fmt.Appendf(nil, "$%d\r\n%s", len(decodedBytes), string(decodedBytes)))
}

func (s *Server) handleReplConfCommand(conn net.Conn) {
	response := resp.EncodeSimpleString("OK")

	conn.Write(response)
}

func (s *Server) handleSetCommand(conn net.Conn, args []any) {
	argsLen := len(args)

	if argsLen < 2 {
		conn.Write(resp.EncodeError("\"SET\" command requires at least 2 arguments"))
		return
	}

	rawKey, ok := args[0].([]byte)

	if !ok {
		conn.Write(resp.EncodeError("\"SET\" command argument must be a string"))
		return
	}

	value := args[1]
	key := string(rawKey)

	expiry := time.Time{}

	if argsLen < 3 {
		s.cache.SetItem(key, value, expiry)
		conn.Write(resp.EncodeSimpleString("OK"))
		return
	}

	switch v := args[2].(type) {
	case []byte:
		if !bytes.Equal(bytes.ToUpper(v), []byte("PX")) {
			s.cache.SetItem(key, value, expiry)
			conn.Write(resp.EncodeSimpleString("OK"))
			return
		}

		if argsLen < 4 {
			conn.Write(resp.EncodeError("\"SET\" command with \"PX\" option requires an expiry value"))
			return
		}

		var duration time.Duration

		switch px := args[3].(type) {
		case int:
			duration = time.Duration(px) * time.Millisecond

		case []byte:
			d, err := strconv.Atoi(string(px))

			if err != nil {
				conn.Write(resp.EncodeError("\"SET\" command \"PX\" options requires an integer expiry value"))
				return
			}

			duration = time.Duration(d) * time.Millisecond

		default:
			conn.Write(resp.EncodeError("\"SET\" command \"PX\" options requires an integer expiry value"))
			return
		}

		expiry = time.Now().Add(duration)
	}

	s.cache.SetItem(key, value, expiry)
	response := resp.EncodeSimpleString("OK")
	conn.Write(response)
}

func (s *Server) executeCommand(conn net.Conn, command []byte, args []any) {
	switch string(bytes.ToUpper(command)) {
	case CONFIG:
		s.handleConfigCommand(conn, args)
		return

	case ECHO:
		s.handleEchoCommand(conn, args)
		return

	case GET:
		s.handleGetCommand(conn, args)
		return

	case INFO:
		s.handleInfoCommand(conn, args)
		return

	case KEYS:
		s.handleKeysCommand(conn, args)
		return

	case PING:
		s.handlePingCommand(conn)
		return

	case PSYNC:
		s.handlePsyncCommand(conn)
		return

	case REPLCONF:
		s.handleReplConfCommand(conn)
		return

	case SET:
		s.handleSetCommand(conn, args)
		return

	default:
		conn.Write(resp.EncodeError(fmt.Sprintf("unsupported command \"%s\"", command)))
		return
	}
}

func (s *Server) handleCommands(conn net.Conn, input any) {
	argv, ok := input.([]any)

	if !ok {
		conn.Write(resp.EncodeError("commands must be encoded as a list of bulk strings"))
		return
	}

	command, ok := argv[0].([]byte)

	if !ok {
		conn.Write(resp.EncodeError("command must be encoded as a bulk string"))
		return
	}

	s.executeCommand(conn, command, argv[1:])
}
