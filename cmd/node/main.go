package main

import (
	"log"
	"mesh/internal/node"
	"net"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()
	serverAddr := os.Getenv("MESH_SERVER_ADDR")
	stunAddr, err := net.ResolveUDPAddr("udp", os.Getenv("MESH_STUN_ADDR"))
	if err != nil {
		log.Fatal(err)
	}

	n, err := node.New(serverAddr, stunAddr)
	if err != nil {
		log.Fatal(err)
	}

	defer n.Close()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
