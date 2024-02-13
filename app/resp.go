package main

import "fmt"

const (
	String = '+'
	Array  = '*'
	Bulk   = '$'
	Empty  = "$-1"
	CRLF   = "\r\n"
)

func encodeSimpleString(str string) string {
	return fmt.Sprintf("%v%s%s", String, str, CRLF)
}

func encodeBulkString(str string) string {
	return fmt.Sprintf("%s%d%s%s%s", string(Bulk), len(str), CRLF, str, CRLF)
}

func encodeArray(args []string) string {
	response := ""
	response = fmt.Sprintf("%s%s%d%s", response, string(Array), len(args), CRLF)
	for i := 0; i < len(args); i++ {
		response = fmt.Sprintf("%s%s%d%s%s%s", response, string(Bulk), len(args[i]), CRLF, args[i], CRLF)
	}
	return response
}
