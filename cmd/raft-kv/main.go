package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	pb "github.com/PranavBhatP/raft-kv/proto"
	"github.com/PranavBhatP/raft-kv/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// waitUntilConnReady blocks until c reaches READY or ctx is done. New gRPC
// connections start idle; the first RPC would race election RPCs against
// TCP handshakes, which often exceeds a short deadline under docker-compose.
func waitUntilConnReady(ctx context.Context, c *grpc.ClientConn) error {
	for {
		state := c.GetState()
		switch state {
		case connectivity.Ready:
			return nil
		case connectivity.Shutdown:
			return fmt.Errorf("connection closed before ready")
		case connectivity.Idle:
			c.Connect()
		}
		if !c.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

func main() {

	rand.Seed(time.Now().UnixNano())
	nodeIdStr := os.Getenv("NODE_ID")
	port := os.Getenv("NODE_PORT")
	peers := os.Getenv("PEERS")

	if nodeIdStr == "" || port == "" {
		log.Fatalf("NODE_ID, NODE_PORT, and PEERS must be set")
	}

	nodeId, err := strconv.Atoi(nodeIdStr)
	if err != nil {
		log.Fatalf("failed to parse NODE_ID: %v", err)
	}

	nodeIdInt := int32(nodeId)

	log.Printf("starting raft node %d on port %s", nodeId, port)

	node := raft.NewRaftNode(nodeIdInt)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, node) // register the server with grpc.

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	if peers != "" {
		peerConfigs := strings.Split(peers, ",")
		for _, peerConfig := range peerConfigs {
			parts := strings.SplitN(strings.TrimSpace(peerConfig), ":", 2) // config format is "peerHost:peerPort"
			if len(parts) != 2 {
				log.Fatalf("invalid peer config: %s", peerConfig)
				continue
			}
			peerHost := parts[0]
			peerIdStr := strings.TrimPrefix(peerHost, "peer")
			peerId, err := strconv.Atoi(peerIdStr)
			if err != nil {
				log.Fatalf("invalid peer host %q in config %q; expected peer<id>", peerHost, peerConfig)
				continue
			}
			peerIdInt := int32(peerId)
			peerAddr := fmt.Sprintf("%s:%s", peerHost, parts[1])

			log.Printf("Dialing peer %d at %s", peerId, peerAddr)

			conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("failed to dial peer %d at %s: %v", peerId, peerAddr, err)
				continue
			}
			readyCtx, readyCancel := context.WithTimeout(context.Background(), 30*time.Second)
			err = waitUntilConnReady(readyCtx, conn)
			readyCancel()
			if err != nil {
				log.Fatalf("peer %d at %s did not become ready: %v", peerId, peerAddr, err)
			}
			peerClient := pb.NewRaftServiceClient(conn) //client stub for the peer.
			node.AddPeer(peerIdInt, peerClient)
		}
	}

	go node.Run()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
}
