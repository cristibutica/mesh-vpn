package server

import (
	"net"
	"sync"

	"github.com/google/uuid"
)

// reflexive and connection for node
type nodeEntry struct {
	id    string
	addrs []string
	conn  net.Conn
}

// thread-safe map for storing nodes
type Registry struct {
	mu    sync.Mutex
	nodes map[string]*nodeEntry
}

// get all peer connections excluding the one in the parameters list
func (r *Registry) Peers(exclude string) []*nodeEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*nodeEntry
	for id, nodeEntry := range r.nodes {
		if id != exclude {
			out = append(out, nodeEntry)
		}
	}
	return out
}

// get node
func (r *Registry) Get(id string) *nodeEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.nodes[id]
}

// add node
func (r *Registry) Add(id string, nodeEntry *nodeEntry) {
	r.mu.Lock()
	r.nodes[id] = nodeEntry
	r.mu.Unlock()
}

// remove node
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	delete(r.nodes, id)
	r.mu.Unlock()
}

func (r *Registry) GenerateID() string {
	return uuid.New().String()
}
