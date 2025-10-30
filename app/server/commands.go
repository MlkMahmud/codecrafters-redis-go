package server

import (
	"bytes"
	"fmt"
	"iter"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/cache"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

var (
	CONFIG   = []byte("CONFIG")
	ECHO     = []byte("ECHO")
	GET      = []byte("GET")
	INFO     = []byte("INFO")
	KEYS     = []byte("KEYS")
	PING     = []byte("PING")
	PSYNC    = []byte("PSYNC")
	REPLCONF = []byte("REPLCONF")
	SET      = []byte("SET")
)

func handleConfigCommand(config *Config, args []any) (int, []byte) {
	argsLen := len(args)
	invalidSubcommandErr := "\"CONFIG\" command must be followed by one of the following subcommands \"GET\", \"HELP\", \"RESETSTAT\", \"REWRITE\" or \"SET\""

	if argsLen == 0 {
		return 0, resp.EncodeError(invalidSubcommandErr)
	}

	subcommand, ok := args[0].([]byte)

	if !ok {
		return 0, []byte(invalidSubcommandErr)
	}

	switch {
	case bytes.Equal(bytes.ToUpper(subcommand), []byte("GET")):
		response := handleConfigGetCommand(config, args[1:])
		return argsLen, response

		//todo: handle other subcommands
	default:
		return 0, []byte(invalidSubcommandErr)
	}
}

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

func handleEchoCommand(args []any) (int, []byte) {
	if len(args) < 1 {
		return 0, resp.EncodeError("\"ECHO\" command requires at least 1 argument")
	}

	arg, ok := args[0].([]byte)

	if !ok {
		return 0, resp.EncodeError("\"ECHO\" command argument must be a string argument")
	}

	response := resp.EncodeBulkString(string(arg))

	return 1, response
}

func handleGetCommand(cache *cache.Cache, args []any) (int, []byte) {
	if len(args) < 1 {
		return 0, resp.EncodeError("\"GET\" command requires at least 1 argument")
	}

	key, ok := args[0].([]byte)

	if !ok {
		return 0, resp.EncodeError("\"GET\" command argument must be a string")
	}

	item := cache.GetItem(string(key))
	var response []byte

	switch v := item.(type) {
	case []byte:
		response = resp.EncodeBulkString(string(v))

	case string:
		response = resp.EncodeBulkString(v)

	case nil:
		response = resp.EncodeNull()

	default:
		return 1, resp.EncodeError("unsupported data type")
	}

	return 1, response
}

func handleInfoCommand(s *Server, args []any) (int, []byte) {
	if len(args) < 1 {
		// todo: handle "INFO" command without 'section' argument
		return 0, resp.EncodeNull()
	}

	section, ok := args[0].([]byte)

	if !ok {
		return 0, resp.EncodeError("\"INFO\" command argument must be a string")
	}

	if !bytes.Equal(bytes.ToLower(section), []byte("replication")) {
		// todo: handle "INFO" section arguments beyond "replication"
		return 0, resp.EncodeNull()
	}

	response := resp.EncodeBulkString(fmt.Sprintf("role:%s\nmaster_replid:%s\nnmaster_repl_offset:%d", s.role, s.replicationId, s.replicationOffset))

	return 1, response
}

func handleKeysCommand(cache *cache.Cache, args []any) (int, []byte) {
	if len(args) < 1 {
		return 0, resp.EncodeError("\"KEYS\" command requires at least 1 argument")
	}

	pattern, ok := args[0].([]byte)

	if !ok {
		return 0, resp.EncodeError("\"KEYS\" command argument must be a string")
	}

	if !bytes.Equal(pattern, []byte("*")) {
		return 1, resp.EncodeArray([][]byte{})
	}

	entries := make([][]byte, cache.Size())
	index := 0

	for key := range cache.GetItems() {
		entries[index] = resp.EncodeBulkString(key)
		index += 1
	}

	return 1, resp.EncodeArray(entries)
}

func handlePingCommand() (int, []byte) {
	response := resp.EncodeSimpleString("PONG")

	return 0, response
}

func handlePsyncCommand(replicationId string) (int, []byte) {
	return 2, resp.EncodeSimpleString(fmt.Sprintf("FULLRESYNC %s 0", replicationId))
}

func handleReplConfCommand() (int, []byte) {
	response := resp.EncodeSimpleString("OK")

	return 2, response
}

func handleSetCommand(cache *cache.Cache, args []any) (int, []byte) {
	argsLen := len(args)
	argsConsumed := 0

	if argsLen < 2 {
		return argsConsumed, resp.EncodeError("\"SET\" command requires at least 2 arguments")
	}

	rawKey, ok := args[0].([]byte)

	if !ok {
		return argsConsumed, resp.EncodeError("\"SET\" command argument must be a string")
	}

	value := args[1]
	argsConsumed = 2
	key := string(rawKey)

	expiry := time.Time{}

	if argsLen < 3 {
		cache.SetItem(key, value, expiry)
		return argsConsumed, resp.EncodeSimpleString("OK")
	}

	switch v := args[2].(type) {
	case []byte:
		if !bytes.Equal(bytes.ToUpper(v), []byte("PX")) {
			cache.SetItem(key, value, expiry)
			return argsConsumed, resp.EncodeSimpleString("OK")
		}

		argsConsumed += 1

		if argsLen < 4 {
			return argsConsumed, resp.EncodeError("\"SET\" command with \"PX\" option requires an expiry value")
		}

		var duration time.Duration
		argsConsumed += 1

		switch px := args[3].(type) {
		case int:
			duration = time.Duration(px) * time.Millisecond

		case []byte:
			d, err := strconv.Atoi(string(px))

			if err != nil {
				return argsConsumed, resp.EncodeError("\"SET\" command \"PX\" options requires an integer expiry value")
			}

			duration = time.Duration(d) * time.Millisecond

		default:
			return argsConsumed, resp.EncodeError("\"SET\" command \"PX\" options requires an integer expiry value")
		}

		expiry = time.Now().Add(duration)
	}

	cache.SetItem(key, value, expiry)
	response := resp.EncodeSimpleString("OK")

	return argsConsumed, response
}

func (s *Server) executeCommand(command []byte, args []any) (int, []byte) {
	switch {
	case bytes.Equal(command, CONFIG):
		return handleConfigCommand(s.config, args)

	case bytes.Equal(command, ECHO):
		return handleEchoCommand(args)

	case bytes.Equal(command, GET):
		return handleGetCommand(s.cache, args)

	case bytes.Equal(command, INFO):
		return handleInfoCommand(s, args)

	case bytes.Equal(command, KEYS):
		return handleKeysCommand(s.cache, args)

	case bytes.Equal(command, PING):
		return handlePingCommand()

	case bytes.Equal(command, PSYNC):
		return handlePsyncCommand(s.replicationId)

	case bytes.Equal(command, REPLCONF):
		return handleReplConfCommand()

	case bytes.Equal(command, SET):
		return handleSetCommand(s.cache, args)

	default:
		return 0, resp.EncodeError(fmt.Sprintf("unsupported command \"%s\"", command))
	}
}

func (s *Server) handleCommands(commands any) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		switch v := commands.(type) {

		case []any:
			for i := 0; i < len(v); {
				switch t := v[i].(type) {
				case []byte:
					command := bytes.ToUpper(t)

					args := v[i+1:]
					argsConsumed, response := s.executeCommand(command, args)

					i += 1 // add one for the command
					i += argsConsumed

					if !yield(response) {
						return
					}

				default:
					yield(resp.EncodeError(fmt.Sprintf("unsupported data type %T", t)))
					return
				}
			}

			return

		// redis client sends commands as an array of bulk, however we might sometimes receive inline commands commands.
		case []byte:
			command := bytes.ToUpper(v)
			_, response := s.executeCommand(command, []any{})

			yield(response)
			return

		default:
			yield(resp.EncodeError(fmt.Sprintf("unsupported data type %T", v)))
			return
		}
	}
}
