package main

import (
	"encoding/json"
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
}

// TODO: implements using reflect
func (node *Node) FieldVapMap() map[string]interface{} {
	nodeMap := map[string]interface{}{}
	barr, _ := json.Marshal(node)
	json.Unmarshal(barr, &nodeMap)
	return nodeMap
}

// TODO: implements retries in case of failure
func (node *Node) HandShake(replica *Node) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%v:%d", *node.Host, *node.Port))
	if err != nil {
		log.Fatalf("error connecting to node: %s, error: %s", node.Role, err.Error())
	}
	defer conn.Close()
	mustCopy(conn, strings.NewReader(encodeArray([]string{"ping"})))
	mustCopy(conn, strings.NewReader(encodeArray([]string{"REPLCONF", "listening-port", strconv.Itoa(*replica.Port)})))
	mustCopy(conn, strings.NewReader(encodeArray([]string{"REPLCONF", "capa", "psync2"})))
}
