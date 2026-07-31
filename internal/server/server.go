package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"mesh/internal/protocol"
	"net"
)

func Start(address string) {

	registry := &Registry{
		nodes: make(map[string]*nodeEntry),
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}

	// accept loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("accept error: ", err)
			return
		}

		// each connection is a new goroutine
		go ProcessConnection(conn, registry)

	}
}

func ProcessConnection(c net.Conn, registry *Registry) {
	id := registry.GenerateID()

	reader := bufio.NewReader(c)

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

		if message.Type == "register" {
			nodeEntry := &nodeEntry{id: id, addrs: message.Addrs, conn: c}
			registry.Add(id, nodeEntry)

			fmt.Printf("data from %s; id = %s: %s\n", c.RemoteAddr(), id, message)

			nodePeers := registry.Peers(id)

			peerMsg := &protocol.Message{
				Type: "peer",
			}

			otherMsg := &protocol.Message{
				Type: "peer",
				ID:   id,
			}

			for i := range nodePeers {
				peerMsg.Addrs = nodePeers[i].addrs
				peerMsg.ID = nodePeers[i].id

				// the current node address
				otherMsg.Addrs = message.Addrs

				otherConn := nodePeers[i].conn

				// construct the json peer message
				peerAddrJson, err := json.Marshal(peerMsg)
				if err != nil {
					log.Println("json marshal error: ", err)
					return
				}

				// tell the current node who are it's neighbors
				peerAddrJson = append(peerAddrJson, '\n')
				_, err = c.Write(peerAddrJson)
				if err != nil {
					log.Println("write tcp error: ", err)
					return
				}

				// construct the json peer message
				otherAddrJson, err := json.Marshal(otherMsg)
				if err != nil {
					log.Println("json marshal error: ", err)
					return
				}

				// tell every other node it has a new neighbor
				otherAddrJson = append(otherAddrJson, '\n')
				_, err = otherConn.Write(otherAddrJson)
				if err != nil {
					log.Println("write tcp error: ", err)
					return
				}

			}

		}

	}
}
