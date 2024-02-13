package main

import (
	"bufio"
	"bytes"
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

type input struct {
	raw  []byte
	cmds []string
}

type expireInfo struct {
	key string
	ttm time.Duration
}

// replica varibales
var node = &Node{Role: "master"}
var masterNode = &Node{Role: "master"}

// data store
var dbstore = make(map[string]string)
var mu sync.RWMutex

// file info
var dir string
var dbfilename string

// rdb golbal instance
var rdb *RDB

var expireChannel = make(chan expireInfo, 100)

func mustCopy(dst io.Writer, src io.Reader) {
	if _, err := io.Copy(dst, src); err != nil {
		log.Fatal(err)
	}
}

// set random_key random_value px 100
func set(args ...string) {
	if len(args) >= 2 {
		mu.Lock()
		dbstore[args[0]] = args[1]
		mu.Unlock()
	}
	if len(args) == 4 {
		if strings.ToUpper(args[2]) == "PX" {
			d, err := strconv.Atoi(args[3])
			if err != nil {
				log.Print("invalid expire val: ", args[3])
				return
			}
			expireChannel <- expireInfo{args[0], time.Duration(d) * time.Millisecond}
		}
	}
}

func get(key string) (string, bool) {
	mu.RLock()
	val, OK := dbstore[key]
	mu.RUnlock()
	return val, OK
}

func expireKeys(in <-chan expireInfo) {
	for val := range in {
		tick := time.NewTicker(val.ttm)
		<-tick.C
		mu.Lock()
		delete(dbstore, val.key)
		mu.Unlock()
		tick.Stop()
	}
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

func validBytes(r *bufio.Reader) ([]byte, error) {
	barr, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return barr[:len(barr)-2], nil
}

func configDetail(key string) string {
	args := []string{key}
	if key == "dir" {
		args = append(args, dir)
	}
	if key == "dbfilename" {
		args = append(args, dbfilename)
	}
	return encodeArray(args)
}

func getKey(key string) string {
	if rdb == nil || rdb.Database == nil || rdb.Database[0].Store == nil {
		return ""
	}
	item := rdb.Database[0].Store[key]
	if item != nil {
		val := rdb.Database[0].Store[key].Val
		exp := rdb.Database[0].Store[key].Expire
		if !exp.IsZero() && exp.Before(time.Now()) {
			return ""
		}
		return val
	}
	return ""
}

func getKeys() []string {
	keys := []string{}
	for key := range rdb.Database[0].Store {
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

func (i *input) parse() {
	r := bufio.NewReader(bytes.NewReader(i.raw))
	b, err := r.ReadByte()
	if err != nil {
		log.Fatal("error reading 1st byte: ", err)
	}
	switch b {
	case Array:
		cmds, err := decodeArray(r)
		if err != nil {
			log.Fatal("error parsing input stream: ", err)
		}
		i.cmds = cmds
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	go expireKeys(expireChannel)
	for {
		barr := make([]byte, 1024)
		_, err := conn.Read(barr)
		if err == io.EOF {
			log.Println("client is done")
			return
		}
		if err != nil {
			log.Fatal(err)
		}
		in := input{raw: barr}
		in.parse()
		switch strings.ToUpper(in.cmds[0]) {
		// TODO: write handlers for each of the query
		case "PING":
			mustCopy(conn, strings.NewReader("+PONG\r\n"))
		case "ECHO":
			mustCopy(conn, strings.NewReader(fmt.Sprintf("%s%s%s", "+", in.cmds[1], "\r\n")))
		case "SET":
			set(in.cmds[1:]...)
			mustCopy(conn, strings.NewReader("+OK\r\n"))
		case "GET":
			val, OK := get(in.cmds[1])
			if !OK {
				val = getKey(in.cmds[1])
				if val == "" {
					mustCopy(conn, strings.NewReader(fmt.Sprintf("%s%s", Empty, "\r\n")))
					break
				}
				res := encodeBulkString(val)
				mustCopy(conn, strings.NewReader(res))
				break
			}
			mustCopy(conn, strings.NewReader(fmt.Sprintf("%s%s%s", "+", val, "\r\n")))
		case "CONFIG":
			if strings.ToUpper(in.cmds[1]) == "GET" {
				resp := configDetail(in.cmds[2])
				mustCopy(conn, strings.NewReader(resp))
			}
		case "KEYS":
			vals := getKeys()
			resp := encodeArray(vals)
			mustCopy(conn, strings.NewReader(resp))
		case "INFO":
			info := getInfoDetails(in.cmds[1:]...)
			mustCopy(conn, strings.NewReader(encodeBulkString(info)))
		default:
			mustCopy(conn, strings.NewReader("+PONG\r\n"))
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
	node.Port = &port
	if replicaOf != "" {
		node.Role = "slave"
		masterPort, _ := strconv.Atoi(args[len(args)-1])
		masterNode.Host = &replicaOf
		masterNode.Port = &masterPort
		go masterNode.HandShake(node)
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
	rdb, err = parseRDB(file)
	file.Close()
	if err != nil {
		log.Println(err)
	}
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", "localhost", port))
	if err != nil {
		log.Fatal(err)
	}
	// accepts connections in loop ..
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleConn(conn)
	}
}
