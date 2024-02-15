package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

// node information
type Node struct {
	Role             string  `json:"role"`
	MasterReplID     *string `json:"master_replid,omitempty"`
	MasterReplOffset *int    `json:"master_repl_offset,omitempty"`
	Host             *string `json:"host,omitempty"`
	Port             *int    `json:"port,omitempty"`
	Signal           chan struct{}
	Lock             sync.RWMutex
	Conn             net.Conn
}

// TODO: implements using reflect
func (node *Node) FieldVapMap() map[string]interface{} {
	nodeMap := map[string]interface{}{
		"role": node.Role,
	}
	if node.MasterReplID != nil {
		nodeMap["master_replid"] = *node.MasterReplID
	}
	if node.MasterReplOffset != nil {
		nodeMap["master_repl_offset"] = *node.MasterReplOffset
	}
	return nodeMap
}

func must(err error) {
	if err != nil {
		log.Fatalf("error reading from connection: %s", err)
	}
}

// TODO: implements retries in case of failure
func (master *Node) HandShake(replica *Node) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%d", *master.Host, *master.Port))
	if err != nil {
		log.Printf("error connecting to node: %s, error: %s", master.Role, err.Error())
		return
	}
	// defer conn.Close()
	replica.Conn = conn
	barr := make([]byte, 1024)
	mustCopy(conn, strings.NewReader(encodeArray([]string{"ping"})))
	_, err = conn.Read(barr)
	must(err)
	mustCopy(conn, strings.NewReader(encodeArray([]string{"REPLCONF", "listening-port", strconv.Itoa(*replica.Port)})))
	_, err = conn.Read(barr)
	must(err)
	mustCopy(conn, strings.NewReader(encodeArray([]string{"REPLCONF", "capa", "psync2"})))
	_, err = conn.Read(barr)
	must(err)
	mustCopy(conn, strings.NewReader(encodeArray([]string{"PSYNC", "?", "-1"})))
	_, err = conn.Read(barr)
	must(err)
	_, err = conn.Read(barr)
	must(err)
	close(replica.Signal)
}
