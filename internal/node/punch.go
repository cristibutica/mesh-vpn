package node

import (
	"log"
	"net"
	"time"
)

// UDP hole punching function
func (n *Node) Punch(peer *Peer, target *net.UDPAddr, message string) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-peer.done:
			return
		case <-ticker.C:
			_, err := n.socket.WriteToUDP([]byte(message), target)
			if err != nil {
				log.Println("write udp error: ", err)
				return
			}
		}
	}

}

func (n *Node) Keepalive(peer *Peer, message string) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		_, err := n.socket.WriteToUDP([]byte(message), peer.chosen)
		if err != nil {
			log.Println("write udp error: ", err)
			return
		}
		log.Println("keepalive ->", peer.chosen)
	}
}
