package main

import (
	"fmt"
	"sync"
)

type ReplicaManager struct {
	Nodes  []*Node
	Buffer []string
	Mu     sync.Mutex
}

func NewRePlicationManager() *ReplicaManager {
	return &ReplicaManager{Nodes: []*Node{}, Buffer: make([]string, 0)}
}

func (rm *ReplicaManager) AddReplica(node Node) {
	rm.Mu.Lock()
	rm.Nodes = append(rm.Nodes, &node)
	rm.Mu.Unlock()
}

func (rm *ReplicaManager) AppendBuffer(data string) {
	rm.Mu.Lock()
	rm.Buffer = append(rm.Buffer, data)
	rm.Mu.Unlock()
}

func (rm *ReplicaManager) populateReplicas() {
	rm.Mu.Lock()
	for _, node := range rm.Nodes {
		for _, data := range rm.Buffer {
			// fmt.Println("data: ", data)
			fmt.Println("node:", node)
			node.Writer.Write([]byte(data))
		}
	}
	rm.Buffer = make([]string, 0)
	rm.Mu.Unlock()
}
