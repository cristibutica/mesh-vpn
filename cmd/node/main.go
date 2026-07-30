package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"mesh/internal/protocol"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"github.com/pion/stun"
)

type Node struct {
	socket     *net.UDPConn
	reflexive  string
	candidates []string
	peersMap   *PeersMap
	tcpConn    net.Conn
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

type Peer struct {
	id        string
	addrs     []*net.UDPAddr
	done      chan struct{}
	connected bool
	once      sync.Once
	chosen    *net.UDPAddr
}

func (n *Node) Keepalive(peer *Peer, message string) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		_, err := n.socket.WriteToUDP([]byte(message), peer.chosen)
		log.Println("keepalive ->", peer.chosen)
		if err != nil {
			log.Println("write udp error: ", err)
			return
		}

	}
}

// sets loopback and local IPv4 addresses to Node's candidates field
func (n *Node) SetLocalCandidates() {

	addresses, err := net.InterfaceAddrs()
	if err != nil {
		log.Println("interface error: ", err)
		return
	}

	for _, addr := range addresses {
		ipnet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		address := ipnet.IP
		if address.To4() != nil &&
			(address.IsLoopback() ||
				address.IsGlobalUnicast() ||
				address.IsPrivate()) {
			port := n.socket.LocalAddr().(*net.UDPAddr).Port
			// then per address:
			n.candidates = append(n.candidates, net.JoinHostPort(address.String(), strconv.Itoa(port)))
		}
	}
}

func (n *Node) DiscoverReflexive(stunAddr *net.UDPAddr, ch chan string) {

	message := stun.MustBuild(stun.TransactionID, stun.BindingRequest)

	fmt.Println("socket bound to", n.socket.LocalAddr())

	// read loop
	go func() {
		buf := make([]byte, 1500)

		for {

			count, from, err := n.socket.ReadFromUDP(buf)
			if err != nil {
				log.Println("read error: ", err)
				return
			}
			if stun.IsMessage(buf[:count]) {

				message := &stun.Message{
					Raw: buf[:count],
				}
				err = message.Decode()
				if err != nil {
					log.Println("decode error: ", err)
					return
				}

				var xorAddr stun.XORMappedAddress
				err = xorAddr.GetFrom(message)
				if err != nil {
					log.Println("xor mapping error: ", err)
					return
				}

				reflexive := xorAddr.String()
				fmt.Printf("your reflexive is: %s\n", reflexive)
				ch <- reflexive

			} else {
				peer, chosen := n.peersMap.FindByAddr(from.String())
				// unknown sender
				if peer == nil {
					continue
				}

				if !peer.connected {
					fmt.Printf("received %d bytes from %s: %q\n", count, from, buf[:count])
					n.socket.WriteToUDP([]byte("ping"), from)
					n.peersMap.MarkConnected(n, peer, chosen)
				}

			}
		}

	}()

	// reflexive discovery (STUN)
	_, err := n.socket.WriteToUDP(message.Raw, stunAddr)
	if err != nil {
		log.Println("write udp error: ", err)
		return
	}
}

func (n *Node) Register(serverAddr string) error {
	registerMessage := &protocol.Message{Type: "register", Addrs: n.candidates}

	tcpConn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Println("dial error: ", err)
		return err
	}

	n.tcpConn = tcpConn

	// construct the json register message
	registerJson, err := json.Marshal(registerMessage)
	if err != nil {
		log.Println("json marshal error: ", err)
		return err
	}

	registerJson = append(registerJson, '\n')

	_, err = tcpConn.Write(registerJson)
	if err != nil {
		log.Println("write tcp error: ", err)
		return err
	}

	return nil
}

func (n *Node) GetPeers() {

	reader := bufio.NewReader(n.tcpConn)

	for {
		data, err := reader.ReadBytes('\n')
		if err != nil {
			log.Println("read error: ", err)
			return
		}

		message := &protocol.Message{}

		err = json.Unmarshal(data, message)
		if err != nil {
			log.Println("unmarshal error: ", err)
			return
		}

		if message.Type == "peer" {

			log.Println("got peer:", message.Addrs)

			var peerAddrs []*net.UDPAddr

			for _, addr := range message.Addrs {
				peerAddr, err := net.ResolveUDPAddr("udp", addr)
				if err != nil {
					log.Println("address resolve error: ", err)
					continue
				}

				peerAddrs = append(peerAddrs, peerAddr)

			}
			peer := &Peer{
				id:        message.ID,
				addrs:     peerAddrs,
				done:      make(chan struct{}),
				connected: false,
			}

			n.peersMap.AddPeer(peer)

			for _, target := range peer.addrs {
				go n.Punch(peer, target, "ping")
			}
		}
	}

}

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

func main() {
	godotenv.Load()

	serverAddr := os.Getenv("MESH_SERVER_ADDR")

	stunServer := os.Getenv("MESH_STUN_ADDR")

	localAddr, err := net.ResolveUDPAddr("udp", "0.0.0.0:0")
	if err != nil {
		log.Fatal(err)
	}

	stunAddr, err := net.ResolveUDPAddr("udp", stunServer)
	if err != nil {
		log.Fatal(err)
	}

	socket, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer socket.Close()

	peersMap := &PeersMap{
		peers: make(map[string]*Peer),
	}

	node := &Node{
		socket:   socket,
		peersMap: peersMap,
	}

	node.SetLocalCandidates()

	// blocking channel for getting reflexive address
	ch := make(chan string)

	node.DiscoverReflexive(stunAddr, ch)

	// blocking wait for reflexive IP
	reflexive := <-ch

	node.reflexive = reflexive

	node.candidates = append(node.candidates, node.reflexive)

	node.Register(serverAddr)

	node.GetPeers()
}
