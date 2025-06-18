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
	integerPrefix      = ':'
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

	case integerPrefix:
		return parseInteger(r)

	case simpleStringPrefix:
		return parseSimpleString(r)

	default:
		return nil, fmt.Errorf("%w: unsupported data type \"%c\"", errSyntax, prefix)
	}
}

func parseArray(r *bufio.Reader) ([]any, error) {
	lengthLine, err := r.ReadBytes(10)

	if err != nil {
		return nil, fmt.Errorf("failed to read array length from buffer: %w", err)
	}

	lengthLine = bytes.TrimRight(lengthLine, "\r\n")

	if len(lengthLine) == 0 || lengthLine[0] != arrayPrefix {
		return nil, fmt.Errorf("%w: malformed array - array must begin with \"%c\" prefix", errSyntax, arrayPrefix)
	}

	if len(lengthLine) < 2 {
		return nil, fmt.Errorf("%w: malformed array - expected content after \"%c\" prefix", errSyntax, arrayPrefix)
	}

	length, err := strconv.Atoi(string(lengthLine[1:]))

	if err != nil {
		return nil, fmt.Errorf("%w: malformed array length \"%s\"", errSyntax, lengthLine[1:])
	}

	arr := make([]any, length)

	for i := range length {
		data, err := parseRespData(r)

		if err != nil {
			return nil, err
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
		return nil, fmt.Errorf("%w: malformed bulk string - length prefix \"%s\" is invalid", errSyntax, lengthLine)
	}

	if prefix := lengthLine[0]; prefix != bulkStringPrefix {
		return nil, fmt.Errorf("%w: bulk strings must begin with a \"%c\" prefix not \"%c\"", errSyntax, bulkStringPrefix, prefix)
	}

	length, err := strconv.Atoi((string(lengthLine[1:])))

	if err != nil {
		return nil, fmt.Errorf("%w: malformed bulk string length \"%s\"", errSyntax, lengthLine[1:])
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
		return nil, fmt.Errorf("%w: bulk string length %d does not match expected length: %d", errSyntax, strLength, length)
	}

	return dataLine, nil
}

func parseInteger(r *bufio.Reader) (int, error) {
	dataLine, err := r.ReadBytes('\n')

	if err != nil {
		return 0, fmt.Errorf("failed to read integer from buffer")
	}

	dataLine = bytes.TrimRight(dataLine, "\r\n")
	dataLineLength := len(dataLine)

	if dataLineLength == 0 || dataLine[0] != integerPrefix {
		return 0, fmt.Errorf("%w: malformed integer - integer must begin with \"%c\" prefix", errSyntax, integerPrefix)
	}

	if dataLineLength < 2 {
		return 0, fmt.Errorf("%w: malformed integer - expected content after \"%c\" prefix", errSyntax, integerPrefix)
	}

	num, err := strconv.Atoi(string(dataLine[1:]))

	if err != nil {
		return 0, fmt.Errorf("%w: malformed integer value \"%s\"", errSyntax, dataLine[1:])
	}

	return num, nil
}

func parseSimpleString(r *bufio.Reader) ([]byte, error) {
	dataLine, err := r.ReadBytes('\n')

	if err != nil {
		return nil, fmt.Errorf("failed to read simple string from buffer")
	}

	dataLine = bytes.TrimRight(dataLine, "\r\n")
	dataLineLength := len(dataLine)

	if dataLineLength == 0 || dataLine[0] != simpleStringPrefix {
		return nil, fmt.Errorf("%w: malformed simple string - string must begin with \"%c\" prefix", errSyntax, simpleStringPrefix)
	}

	if dataLineLength < 2 {
		return nil, fmt.Errorf("%w: malformed simple string - expected content after \"%c\" prefix", errSyntax, simpleStringPrefix)
	}

	return dataLine[1:], nil
}
