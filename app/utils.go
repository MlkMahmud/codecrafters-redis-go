package main

import "fmt"

func generateBulkString(str string) []byte {
	return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(str), str)
}

func generateNullString() []byte {
	return []byte("$-1\r\n")
}

func generateSimpleString(str string) []byte {
	return fmt.Appendf(nil, "+%s\r\n", str)
}
