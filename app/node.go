package main

import "encoding/json"

// node information
type Node struct {
	Role             string  `json:"role"`
	MasterReplID     *string `json:"master_repl_id,omitempty"`
	MasterReplOffset *int    `json:"master_repl_offset,omitempty"`
}

// TODO: implements using reflect
func (node *Node) FieldVapMap() map[string]interface{} {
	nodeMap := map[string]interface{}{}
	barr, _ := json.Marshal(node)
	json.Unmarshal(barr, &nodeMap)
	return nodeMap
}
