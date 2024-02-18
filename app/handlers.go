package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

type input struct {
	cmds []string
}

func NewInput() *input {
	return &input{}
}

func configDetail(key string) []string {
	resp := []string{key}
	if key == "dir" {
		resp = append(resp, dir)
	}
	if key == "dbfilename" {
		resp = append(resp, dbfilename)
	}
	return resp
}

func setKey(key, val string, db *dbstore) {
	db.mu.Lock()
	db.database[key] = val
	db.mu.Unlock()
}

func getKey(key string, db *dbstore) (string, bool) {
	db.mu.RLock()
	val, OK := db.database[key]
	db.mu.RUnlock()
	return val, OK
}

func getKeys(db *dbstore) []string {
	keys := make([]string, 0)
	for key := range db.database {
		keys = append(keys, key)
	}
	return keys
}

func getInfoDetails(cmds ...string) string {
	if len(cmds) > 0 && cmds[0] == "replication" {
		info := ""
		nodeMap := node.FieldVapMap()
		for key, val := range nodeMap {
			info += fmt.Sprintf("%s:%v%s", key, val, CRLF)
		}
		return info
	}
	return ""
}

func set(cmds []string, db *dbstore) {
	if len(cmds) >= 2 {
		setKey(cmds[0], cmds[1], db)
	}
	if len(cmds) == 4 && strings.ToUpper(cmds[2]) == "PX" {
		d, err := strconv.Atoi(cmds[3])
		if err != nil {
			log.Print("invalid expire val: ", cmds[3])
			return
		}
		expireChannel <- expireInfo{cmds[0], time.Duration(d) * time.Millisecond}
	}
}

func (i *input) parse(raw []byte) {
	r := bufio.NewReader(bytes.NewReader(raw))
	// first byte is always array
	_, _ = r.ReadByte()
	i.cmds, _ = decodeArray(r)
}

func (i *input) handle(db *dbstore) string {
	cmd := strings.ToUpper(i.cmds[0])
	switch cmd {
	case "PING":
		return encodeSimpleString("PONG")
	case "ECHO":
		return encodeSimpleString(i.cmds[1])
	case "SET":
		set(i.cmds[1:], db)
		return encodeSimpleString("OK")
	case "GET":
		val, OK := getKey(i.cmds[1], db)
		if !OK {
			return encodeEmptyString()
		}
		return encodeSimpleString(val)
	case "CONFIG":
		if strings.ToUpper(i.cmds[1]) == "GET" {
			resp := configDetail(i.cmds[2])
			return encodeArray(resp)
		}
	case "KEYS":
		keys := getKeys(db)
		return encodeArray(keys)
	case "INFO":
		info := getInfoDetails(i.cmds[1:]...)
		return encodeBulkString(info)
	case "REPLCONF":
		return encodeSimpleString("OK")
	default:
		return encodeSimpleString("OK")
	}
	return ""
}
