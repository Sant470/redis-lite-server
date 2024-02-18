package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"log"
	"net"
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

func sendSimpleString(conn net.Conn, resp string) {
	conn.Write([]byte(encodeSimpleString(resp)))
}

func sendEmptyResponse(conn net.Conn) {
	conn.Write([]byte(encodeEmptyString()))
}

func sendArrayResponse(conn net.Conn, resp []string) {
	conn.Write([]byte(encodeArray(resp)))
}

func sendBulkString(conn net.Conn, resp string) {
	conn.Write([]byte(encodeBulkString(resp)))
}

func psyncMessage(cmds ...string) string {
	result := "FULLRESYNC"
	if cmds[0] == "?" {
		result += fmt.Sprintf(" %s", *node.MasterReplID)
	}
	if cmds[1] == "-1" {
		result += fmt.Sprintf(" %d", *node.MasterReplOffset)
	}
	return result
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

func (i *input) handle(conn net.Conn, db *dbstore) {
	cmd := strings.ToUpper(i.cmds[0])
	fmt.Println("cmds from handler: ", i.cmds)
	switch cmd {
	case "PING":
		sendSimpleString(conn, "PONG")
	case "ECHO":
		sendSimpleString(conn, i.cmds[1])
	case "SET":
		set(i.cmds[1:], db)
		sendSimpleString(conn, "OK")
	case "GET":
		val, OK := getKey(i.cmds[1], db)
		fmt.Println("val: ", val)
		if !OK {
			sendEmptyResponse(conn)
			break
		}
		sendSimpleString(conn, val)
	case "CONFIG":
		if strings.ToUpper(i.cmds[1]) == "GET" {
			resp := configDetail(i.cmds[2])
			sendArrayResponse(conn, resp)
		}
	case "KEYS":
		keys := getKeys(db)
		sendArrayResponse(conn, keys)
	case "INFO":
		info := getInfoDetails(i.cmds[1:]...)
		sendBulkString(conn, info)
	case "REPLCONF":
		sendSimpleString(conn, "OK")
	case "PSYNC":
		psyncResp := psyncMessage(i.cmds[1:]...)
		sendSimpleString(conn, psyncResp)
		content, _ := hex.DecodeString(EMPTY_RDB_HEX_STRING)
		conn.Write([]byte(fmt.Sprintf("%s%d%s%s", string(Bulk), len(content), CRLF, content)))
	default:
		sendSimpleString(conn, "OK")
	}
}
