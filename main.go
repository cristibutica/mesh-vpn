package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/pion/stun"
)

func main() {

	localAddr, err := net.ResolveUDPAddr("udp", "0.0.0.0:0")
	if err != nil {
		log.Fatal(err)
	}

	stunAddr, err := net.ResolveUDPAddr("udp", "stun.l.google.com:19302")
	if err != nil {
		log.Fatal(err)
	}

	// blocking channel for
	//ch := make(chan bool)

	message := stun.MustBuild(stun.TransactionID, stun.BindingRequest)

	socket, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer socket.Close()

	fmt.Println("socket bound to", socket.LocalAddr())

	// read loop
	go func() {
		buf := make([]byte, 1500)

		for {

			n, from, err := socket.ReadFromUDP(buf)
			if err != nil {
				log.Println("read error: ", err)
				return
			}
			if stun.IsMessage(buf[:n]) {

				message := &stun.Message{
					Raw: buf[:n],
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

				fmt.Printf("your IP is: %s\n", xorAddr.String())
				//ch <- true

			} else {
				fmt.Printf("received %d bytes from %s: %q\n", n, from, buf[:n])
			}
		}

	}()

	// reflexive discovery (STUN)
	_, err = socket.WriteToUDP(message.Raw, stunAddr)
	if err != nil {
		log.Fatal(err)
	}

	reader := bufio.NewReader(os.Stdin)

	peer, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}

	peerAddr, err := net.ResolveUDPAddr("udp", strings.TrimSpace(peer))
	if err != nil {
		log.Fatal(err)
	}

	//<-ch

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop() // tickers hold a runtime resource; stop it when done

	i := 0
	for {
		<-ticker.C
		i += 1
		if _, err := socket.WriteToUDP(fmt.Appendf(nil, "ping %d", i), peerAddr); err != nil {
			log.Fatal(err)
		}
	}
}
