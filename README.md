# mesh-vpn

A peer-to-peer mesh VPN built from scratch in Go, in the spirit of Tailscale / NetBird. Nodes discover each other through a lightweight coordination server, then establish **direct** UDP connections to each other using NAT hole punching — the server never carries their traffic.

The long-term goal is a working encrypted mesh (WireGuard, later AmneziaWG for DPI resistance) with a TUN interface, so you can reach a service on one node (e.g. a self-hosted app on a Raspberry Pi) from another node on a different network.

## Current state

The **connectivity layer works**. Two or more nodes can:

- Discover their public (reflexive) address via STUN
- Gather all their candidate addresses (loopback, LAN, and reflexive)
- Register with the coordination server over TCP and receive each other's candidates
- Punch all candidate paths simultaneously and converge on the best working one (ICE-style)
- Keep the connection alive with periodic keepalives

Right now nodes just exchange `"ping"` packets to prove the path is open. There is **no encryption and no TUN interface yet** — that's the next major chunk of work. Everything currently tested on loopback / LAN; cross-NAT (real internet) testing is the immediate next step.

## Architecture

Two binaries under `cmd/`:

- `cmd/server` — the coordination server. Holds a registry of connected nodes and relays each node's candidate list to the others. Control plane only; never sees node traffic.
- `cmd/node` — a mesh node. Does STUN discovery, registers with the server, and punches direct UDP connections to peers.

Shared types live in `internal/protocol`.

The control plane (node ↔ server) is **TCP**. The data plane (node ↔ node) is **UDP** — that's the hole-punched path.

## Requirements

- Go 1.21+
- Nothing else — dependencies are fetched automatically

## Setup

Clone and pull dependencies:

```
git clone https://github.com/cristibutica/mesh-vpn.git
cd mesh-vpn
go mod download
```

Each node reads its config from a `.env` file in the project root (this file is gitignored, so create it yourself):

```
MESH_SERVER_ADDR=127.0.0.1:9000
MESH_STUN_ADDR=stun.l.google.com:19302
```

For local testing, `127.0.0.1:9000` is correct. To test against a remote server, set `MESH_SERVER_ADDR` to that server's public `ip:port`.

## Running (local test)

Open three terminals. Start the server first:

```
go run ./cmd/server
```

Then start two (or more) nodes, each in its own terminal:

```
go run ./cmd/node
```

Each node prints its reflexive address, registers, receives its peers, and connects. You should see a `connected to peer <id> via <address>` line per peer within about a second. After that it goes quiet except for keepalives — that's normal.

## Roadmap (rough order)

1. Real cross-NAT test (server on a public VPS, nodes on different networks)
2. Pre-shared token auth on the server (right now anyone can register)
3. Encryption layer (WireGuard over the punched socket)
4. TUN interface — carry real IP traffic, not just `"ping"`
5. Reach a real service (e.g. Immich on a Pi) across the mesh
6. Swap WireGuard for AmneziaWG (DPI resistance)
7. Admin API + frontend for managing peers