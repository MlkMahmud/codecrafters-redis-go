package resp

import (
	"bytes"
	"fmt"
)

func EncodeArray(entries [][]byte) []byte {
	return fmt.Appendf(nil, "*%d\r\n%s", len(entries), bytes.Join(entries, []byte("")))
}

func EncodeBulkString(str string) []byte {
	return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(str), str)
}

func EncodeError(message string) []byte {
	return fmt.Appendf(nil, "-ERR %s\r\n", message)
}

func EncodeNull() []byte {
	return []byte("$-1\r\n")
}

func EncodeSimpleString(str string) []byte {
	return fmt.Appendf(nil, "+%s\r\n", str)
}
