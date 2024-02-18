package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// expire channel
type expireInfo struct {
	key string
	ttm time.Duration
}

var expireChannel = make(chan expireInfo, 100)

// replica varibales
var node = &Node{Role: "master"}
var rm = NewRePlicationManager()

// data store
type dbstore struct {
	database map[string]string
	mu       sync.RWMutex
}

// file info
var dir string
var dbfilename string

const EMPTY_RDB_HEX_STRING = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

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

func expireKeys(db *dbstore, in <-chan expireInfo) {
	for val := range in {
		tick := time.NewTicker(val.ttm)
		<-tick.C
		db.mu.Lock()
		delete(db.database, val.key)
		db.mu.Unlock()
		tick.Stop()
	}
}

func handleConn(conn net.Conn, db *dbstore) {
	if conn == nil {
		return
	}
	alive := false
	defer func() {
		if !alive {
			conn.Close()
		}
	}()
	go expireKeys(db, expireChannel)
	barr := make([]byte, 1024)
	for {
		size, err := conn.Read(barr)
		if err == io.EOF {
			log.Println("client is done")
			return
		}
		if err != nil {
			log.Fatal(err)
		}
		data := barr[:size]
		inp := NewInput()
		inp.parse(data)
		cmd := strings.ToUpper(inp.cmds[0])
		if cmd == "SET" && node.Role == "master" {
			rm.AppendBuffer(string(data))
			rm.sendDataToReplicas()
		}
		if cmd == "SET" && node.Role == "slave" {
			continue
		}
		if cmd == "PSYNC" {
			alive = true
			rm.AddReplica(Node{Writer: conn})
			psyncResp := psyncMessage(inp.cmds[1:]...)
			conn.Write([]byte(encodeSimpleString(psyncResp)))
			content, _ := hex.DecodeString(EMPTY_RDB_HEX_STRING)
			conn.Write([]byte(fmt.Sprintf("%s%d%s%s", string(Bulk), len(content), CRLF, content)))
		} else {
			resp := inp.handle(db)
			conn.Write([]byte(resp))
		}

	}
}

func main() {
	args := os.Args
	var port int
	var replicaOf string
	flag.IntVar(&port, "port", 6379, "port for different nodes of cluster")
	flag.StringVar(&replicaOf, "replicaof", "", "host and port of replica")
	flag.StringVar(&dir, "dir", "", "directory of the rdb file")
	flag.StringVar(&dbfilename, "dbfilename", "", "rdb file name")
	flag.Parse()
	db := dbstore{database: make(map[string]string)}
	node.Port = &port
	if replicaOf != "" {
		node.Role = "slave"
		masterPort, _ := strconv.Atoi(args[len(args)-1])
		// Once the handshake is complete, the connection will be read only stream
		node.HandShake(fmt.Sprintf("%s:%d", replicaOf, masterPort))
		go handleConn(node.Reader, &db)
		// for i := 0; i < 10; i++ {
		// 	go handleConn(node.Reader, &db)
		// }
	}
	if node.Role == "master" {
		offset := 0
		repId := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		node.MasterReplOffset = &offset
		node.MasterReplID = &repId
	}
	path := filepath.Join(dir, dbfilename)
	file, err := os.Open(path)
	if err != nil {
		log.Println("error opening file", err)
	}
	_, err = parseRDB(&db, file)
	file.Close()
	if err != nil {
		log.Println(err)
	}
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", "localhost", port))
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleConn(conn, &db)
	}
}
