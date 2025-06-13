package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	arrayPrefix        = '*'
	bulkStringPrefix   = '$'
	errorPrefix        = '-'
	simpleStringPrefix = '+'
)

func parseRespData(r *bufio.Reader) (any, error) {
	delim, err := r.Peek(1)

	if errors.Is(err, io.EOF) {
		return nil, io.EOF
	}

	if err != nil {
		return nil, fmt.Errorf("failed to peek data type from buffer: %w", err)
	}

	prefix := delim[0]

	switch prefix {
	case arrayPrefix:
		return parseArray(r)

	case bulkStringPrefix:
		return parseBulkString(r)

	case simpleStringPrefix:
		return parseSimpleString(r)

	default:
		return nil, fmt.Errorf("unsupported data type \"%c\"", prefix)
	}
}

func parseArray(r *bufio.Reader) ([]any, error) {
	lengthLine, err := r.ReadBytes(10)

	if err != nil {
		return nil, fmt.Errorf("failed to read array length from buffer: %w", err)
	}

	lengthLine = bytes.TrimRight(lengthLine, "\r\n")

	if len(lengthLine) == 0 || lengthLine[0] != arrayPrefix {
		return nil, fmt.Errorf("malformed array - array must begin with \"%c\" prefix", arrayPrefix)
	}

	if len(lengthLine) < 2 {
		return nil, fmt.Errorf("malformed array - expected content after \"%c\" prefix", arrayPrefix)
	}

	length, err := strconv.Atoi(string(lengthLine[1:]))

	if err != nil {
		return nil, fmt.Errorf("failed to parse array length: %w", err)
	}

	arr := make([]any, length)

	for i := range length {
		data, err := parseRespData(r)

		if err != nil {
			return nil, fmt.Errorf("malformed array - failed to entry at index %d: %w", i, err)
		}

		arr[i] = data
	}

	return arr, nil
}

func parseBulkString(r *bufio.Reader) ([]byte, error) {
	lengthLine, err := r.ReadBytes('\n')

	if err != nil {
		return nil, fmt.Errorf("failed to read bulk string length: %w", err)
	}

	lengthLine = bytes.TrimRight(lengthLine, "\r\n")

	if len(lengthLine) < 2 {
		return nil, fmt.Errorf("malformed bulk string - length prefix \"%s\" is invalid", lengthLine)
	}

	if prefix := lengthLine[0]; prefix != bulkStringPrefix {
		return nil, fmt.Errorf("bulk strings must begin with a \"%c\" prefix not \"%c\"", bulkStringPrefix, prefix)
	}

	length, err := strconv.Atoi((string(lengthLine[1:])))

	if err != nil {
		return nil, fmt.Errorf("failed to parse bulk string length: %w", err)
	}

	if length == 0 {
		return []byte(""), nil
	}

	dataLine, err := r.ReadBytes('\n')

	if err != nil {
		return nil, fmt.Errorf("failed to read bulk string data from buffer: %w", err)
	}

	dataLine = bytes.TrimRight(dataLine, "\r\n")

	if strLength := len(dataLine); strLength != length {
		return nil, fmt.Errorf("bulk string length %d does not match expected length: %d", strLength, length)
	}

	return dataLine, nil
}

func parseSimpleString(r *bufio.Reader) ([]byte, error) {
	dataLine, err := r.ReadBytes('\n')

	if err != nil {
		return nil, fmt.Errorf("failed to read simple string from buffer")
	}

	dataLine = bytes.TrimRight(dataLine, "\r\n")
	dataLineLength := len(dataLine)

	if dataLineLength == 0 || dataLine[0] != simpleStringPrefix {
		return nil, fmt.Errorf("malformed simple string - string must begin with \"%c\" prefix", simpleStringPrefix)
	}

	if dataLineLength < 2 {
		return nil, fmt.Errorf("malformed simple string - expected content after \"%c\" prefix", simpleStringPrefix)
	}

	return dataLine[1:], nil
}
