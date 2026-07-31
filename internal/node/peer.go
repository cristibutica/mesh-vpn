package node

import (
	"log"
	"net"
	"sync"
)

type Peer struct {
	id        string
	addrs     []*net.UDPAddr
	done      chan struct{}
	connected bool
	once      sync.Once
	chosen    *net.UDPAddr
}

type PeersMap struct {
	peers map[string]*Peer
	mu    sync.Mutex
}

func (pm *PeersMap) AddPeer(peer *Peer) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.peers[peer.id] = peer
}

func (pm *PeersMap) DeletePeer(id string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.peers, id)
}

// iterate though peers and find the one that has addr
func (pm *PeersMap) FindByAddr(addr string) (*Peer, *net.UDPAddr) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for _, peer := range pm.peers {
		for _, address := range peer.addrs {
			if address.String() == addr {
				return peer, address
			}
		}
	}
	return nil, nil
}

// mark connection to a peer's first routable address
func (pm *PeersMap) MarkConnected(n *Node, peer *Peer, via *net.UDPAddr) {
	peer.once.Do(func() {
		peer.connected = true
		peer.chosen = via
		close(peer.done)
		go n.Keepalive(peer, "ping")
		log.Println("connected to peer", peer.id, "via", via.String())
	})
}
