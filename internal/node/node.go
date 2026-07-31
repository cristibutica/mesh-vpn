package node

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"mesh/internal/protocol"
	"net"
	"strconv"

	"github.com/pion/stun"
)

type Node struct {
	socket     *net.UDPConn
	reflexive  string
	candidates []string
	peersMap   *PeersMap
	tcpConn    net.Conn
	serverAddr string
	stunAddr   *net.UDPAddr
}

func New(serverAddr string, stunAddr *net.UDPAddr) (*Node, error) {
	localAddr, err := net.ResolveUDPAddr("udp", "0.0.0.0:0")
	if err != nil {
		return nil, err
	}

	socket, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		return nil, err
	}

	peersMap := &PeersMap{
		peers: make(map[string]*Peer),
	}

	node := &Node{
		socket:     socket,
		peersMap:   peersMap,
		serverAddr: serverAddr,
		stunAddr:   stunAddr,
	}

	err = node.SetLocalCandidates()

	return node, err
}

func (n *Node) Run() error {
	// start STUN discovery (goroutine + channel)
	// wait for reflexive from the channel
	// append reflexive to candidates
	// register (return error if it fails)
	// GetPeers()  — blocks

	// blocking channel for getting reflexive address
	ch := make(chan string)

	n.DiscoverReflexive(ch)

	// blocking wait for reflexive IP
	reflexive := <-ch

	n.reflexive = reflexive

	n.candidates = append(n.candidates, n.reflexive)

	err := n.Register(n.serverAddr)
	if err != nil {
		return err
	}

	n.GetPeers()
	return nil
}

// sets loopback and local IPv4 addresses to Node's candidates field
func (n *Node) SetLocalCandidates() error {

	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return err
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

	return nil
}

func (n *Node) DiscoverReflexive(ch chan string) {

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
					_, err := n.socket.WriteToUDP([]byte("ping"), from)
					if err != nil {
						log.Println("write udp error: ", err)
						return
					}
					n.peersMap.MarkConnected(n, peer, chosen)
				}

			}
		}

	}()

	// reflexive discovery (STUN)
	_, err := n.socket.WriteToUDP(message.Raw, n.stunAddr)
	if err != nil {
		log.Println("write udp error: ", err)
		return
	}
}

func (n *Node) Register(serverAddr string) error {
	registerMessage := &protocol.Message{Type: "register", Addrs: n.candidates}

	tcpConn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		return err
	}

	n.tcpConn = tcpConn

	// construct the json register message
	registerJson, err := json.Marshal(registerMessage)
	if err != nil {
		return err
	}

	registerJson = append(registerJson, '\n')

	_, err = tcpConn.Write(registerJson)
	if err != nil {
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

func (n *Node) Close() error {
	return n.socket.Close()
}
