package main

import "fmt"

const (
	Array = '*'
	Bulk  = '$'
	Empty = "$-1"
	CRLF  = "\r\n"
)

func encodeBulkString(str string) string {
	return fmt.Sprintf("%s%d%s%s%s", string(Bulk), len(str), CRLF, str, CRLF)
}
