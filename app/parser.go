package main

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

const (
	arrayDelim      = '*'
	bulkStringDelim = '$'
	errorDelim      = '-'
	integerDelim    = ':'
	stringDelim     = '+'
)

var (
	errEmptyInput             = errors.New("input is empty")
	errInvalidBulkStringDelim = errors.New("bulk string delimiter is invalid")
	errInvalidStringDelim     = errors.New("string delimiter is invalid")
)

func parse(input []byte) ([][]byte, error) {
	delimiter := []byte("\r\n")
	parts := [][]byte{}

	for part := range bytes.SplitSeq(input, delimiter) {
		if bytes.Equal(bytes.TrimSpace(part), []byte("")) {
			continue
		}

		parts = append(parts, part)
	}

	if len(parts) == 0 {
		return nil, errEmptyInput
	}

	firstByte := parts[0][0]

	switch firstByte {
	case arrayDelim:
		return parseArray(parts)

	case bulkStringDelim:
		return parseBulkString(parts)

	case errorDelim:
		return parseError(parts)

	case integerDelim:
		return parseInteger(parts)

	case stringDelim:
		return parseString(parts)

	default:
		return nil, fmt.Errorf("input first byte \"%c\" is invalid", firstByte)
	}
}

// todo: refactor (make func recursive)
func parseArray(parts [][]byte) ([][]byte, error) {
	if len(parts) < 1 {
		return nil, fmt.Errorf("array string length is too short")
	}

	lengthPrefix := parts[0]

	if delim := lengthPrefix[0]; delim != arrayDelim {
		return nil, fmt.Errorf("array string delimiter is invalid. expected \"%c\", got \"%c\"", arrayDelim, delim)
	}

	_, err := strconv.Atoi(string(lengthPrefix[1:]))

	if err != nil {
		return nil, fmt.Errorf("failed to parse array length: %w", err)
	}

	entries := [][]byte{}
	entryIndex := 0

	for i, subParts := 0, parts[1:]; i < len(subParts); {
		firstByte := subParts[i][0]

		switch firstByte {
		case bulkStringDelim:
			entry, err := parseBulkString(subParts[i:])

			if err != nil {
				return nil, fmt.Errorf("failed to parse list item at entry \"%d\": %w", entryIndex, err)
			}

			entries = append(entries, entry...)
			entryIndex++
			i += 2

		case stringDelim:
			entry, err := parseString(subParts[i:])

			if err != nil {
				return nil, fmt.Errorf("failed to parse list item at entry \"%d\": %w", entryIndex, err)
			}

			entries = append(entries, entry...)
			entryIndex++
			i += 1

		default:
			return nil, fmt.Errorf("failed to parse list item: invalid delim \"%c\" at entry: %d", firstByte, entryIndex)
		}
	}

	return entries, nil
}

func parseBulkString(parts [][]byte) ([][]byte, error) {
	if len(parts) < 2 {
		return nil, fmt.Errorf("bulk string length is too short")
	}

	if delim := parts[0][0]; delim != bulkStringDelim {
		return nil, fmt.Errorf("bulk string delimiter is invalid. expected \"%c\", got \"%c\"", bulkStringDelim, delim)
	}

	lengthPrefix := parts[0]
	str := parts[1]

	// convert everything after the "$" delim to an integer
	length, err := strconv.Atoi(string(lengthPrefix[1:]))

	if err != nil {
		return nil, fmt.Errorf("failed to parse bulk integer string length: %w", err)
	}

	if length == 0 {
		return [][]byte{}, nil
	}

	if strLen := len(str); strLen != length {
		return nil, fmt.Errorf("actual string length \"%d\" does not match expected length \"%d\"", strLen, length)
	}

	return [][]byte{str}, nil
}

func parseError(_ [][]byte) ([][]byte, error) {
	return nil, nil
}

func parseInteger(_ [][]byte) ([][]byte, error) {
	return nil, nil
}

func parseString(parts [][]byte) ([][]byte, error) {
	if len(parts) < 1 {
		return nil, fmt.Errorf("string length is too short")
	}

	if parts[0][0] != stringDelim {
		return nil, errInvalidStringDelim
	}

	return [][]byte{parts[0][1:]}, nil
}
