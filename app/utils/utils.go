package utils

import (
	"bytes"
	"fmt"
)

func GenerateArrayString(items [][]byte) []byte {
	delimiter := []byte("")
	return fmt.Appendf(nil, "*%d\r\n%s", len(items), bytes.Join(items, delimiter))
}

func GenerateBulkString(str string) []byte {
	return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(str), str)
}

func GenerateErrorString(prefix, message string) []byte {
	return fmt.Appendf(nil, "-%s %s\r\n", prefix, message)
}

func GenerateNullString() []byte {
	return []byte("$-1\r\n")
}

func GenerateSimpleString(str string) []byte {
	return fmt.Appendf(nil, "+%s\r\n", str)
}
