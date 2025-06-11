package main

import "fmt"

func generateBulkString(str []byte) []byte {
	return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(str), str)
}

func generateString(str []byte) []byte {
	return fmt.Appendf(nil, "+%s\r\n", str)
}
