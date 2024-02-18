package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

// node information
type Node struct {
	Role             string  `json:"role"`
	MasterReplID     *string `json:"master_replid,omitempty"`
	MasterReplOffset *int    `json:"master_repl_offset,omitempty"`
	Host             *string `json:"host,omitempty"`
	Port             *int    `json:"port,omitempty"`
	Writer           net.Conn
	Reader           net.Conn
}

func NewNode(role string) *Node {
	return &Node{Role: role}
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
		log.Printf("error reading from connection: %s", err)
	}
}

func (replica *Node) HandShake(addr string) net.Conn {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("error connecting to node: %s, error: %s", addr, err.Error())
	}
	barr := make([]byte, 1024)
	conn.Write([]byte(encodeArray([]string{"ping"})))
	_, err = conn.Read(barr)
	must(err)
	conn.Write([]byte(encodeArray([]string{"REPLCONF", "listening-port", strconv.Itoa(*replica.Port)})))
	_, err = conn.Read(barr)
	must(err)
	conn.Write([]byte(encodeArray([]string{"REPLCONF", "capa", "psync2"})))
	_, err = conn.Read(barr)
	must(err)
	conn.Write([]byte(encodeArray([]string{"PSYNC", "?", "-1"})))
	_, err = conn.Read(barr)
	must(err)
	_, err = conn.Read(barr)
	must(err)
	return conn
}

func (rep *Node) SyncDBfromMaster(db *dbstore) {
	barr := make([]byte, 1024)
	for {
		size, err := rep.Reader.Read(barr)
		if err != nil {
			fmt.Println("error reading from master: ", err)
		}
		data := barr[:size]
		inp := NewInput()
		inp.parse(data)
		cmd := strings.ToUpper(inp.cmds[0])
		fmt.Println("cmds: ", inp.cmds)
		if cmd == "SET" {
			set(inp.cmds[1:], db)
		}
	}
}
