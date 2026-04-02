package main

import (
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
	"google.golang.org/grpc/credentials/insecure"
)

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
	go node.Run()

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
			parts := strings.Split(peerConfig, ":") //config has format "peerId:peerAddr"
			if len(parts) != 2 {
				log.Fatalf("invalid peer config: %s", peerConfig)
				continue
			}
			peerId, _ := strconv.Atoi(parts[0])
			peerIdInt := int32(peerId)
			peerAddr := parts[1]

			log.Printf("Dialing peer %d at %s", peerId, peerAddr)

			conn, err := grpc.NewClient(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("failed to dial peer %d at %s: %v", peerId, peerAddr, err)
				continue
			}
			peerClient := pb.NewRaftServiceClient(conn) //client stub for the peer.
			node.AddPeer(peerIdInt, peerClient)
		}
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
}
