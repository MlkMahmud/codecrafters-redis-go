package main

import "fmt"

func generateBulkString(str string) []byte {
	return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(str), str)
}

func generateSimpleString(str string) []byte {
	return fmt.Appendf(nil, "+%s\r\n", str)
}

func getNArgs(n int, args []any) ([]any, error) {
	if len(args) < n {
		return nil, fmt.Errorf("requires at least %d arguments", n)
	}

	result := make([]any, n)
	copy(result, args[0:n])

	
	return result, nil
}
