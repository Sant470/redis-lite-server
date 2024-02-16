package main

import (
	"net"
	"sync"
)

type ReplicaManager struct {
	Writers []net.Conn
	Buffer  []string
	Mu      sync.Mutex
}

func NewRePlicationManager() *ReplicaManager {
	return &ReplicaManager{Writers: []net.Conn{}, Buffer: []string{}}
}

func (rm *ReplicaManager) AddWriters(conn net.Conn) {
	rm.Mu.Lock()
	rm.Writers = append(rm.Writers, conn)
	rm.Mu.Unlock()
}

func (rm *ReplicaManager) AppendBuffer(data string) {
	rm.Mu.Lock()
	rm.Buffer = append(rm.Buffer, data)
	rm.Mu.Unlock()
}

func (rm *ReplicaManager) populateReplicas() {
	rm.Mu.Lock()
	for _, writer := range rm.Writers {
		for _, data := range rm.Buffer {
			// testing once again
			writer.Write([]byte(data))
		}
	}
	rm.Buffer = make([]string, 0)
	rm.Mu.Unlock()
}
