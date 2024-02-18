package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

const (
	String = '+'
	Array  = '*'
	Bulk   = '$'
	Empty  = "$-1"
	CRLF   = "\r\n"
)

func validBytes(r *bufio.Reader) ([]byte, error) {
	barr, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return barr[:len(barr)-2], nil
}

func encodeEmptyString() string {
	return fmt.Sprintf("%s%s", Empty, "\r\n")
}

func encodeSimpleString(str string) string {
	return fmt.Sprintf("%s%s%s", string(String), str, CRLF)
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

func decode(r *bufio.Reader) ([]string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch b {
	case Bulk:
		vbarr, err := validBytes(r)
		if err != nil {
			return nil, err
		}
		count, err := strconv.Atoi(string(vbarr))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, count+2)
		_, err = io.ReadFull(r, buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		return []string{string(bytes.TrimSpace(buf))}, nil
	default:
		return nil, fmt.Errorf("could not decode the stream")
	}
}

func decodeArray(r *bufio.Reader) ([]string, error) {
	vbarr, err := validBytes(r)
	if err != nil {
		return nil, err
	}
	l, err := strconv.Atoi(string(vbarr))
	if err != nil {
		return nil, err
	}
	cmds := []string{}
	for i := 0; i < l; i++ {
		vals, err := decode(r)
		if err == io.EOF {
			return vals, nil
		}
		if err != nil {
			return nil, err
		}
		cmds = append(cmds, vals...)
	}
	return cmds, nil
}
