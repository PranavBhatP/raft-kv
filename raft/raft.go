package raft

import (
	"context"
	"log"
	"sync"
	"time"

	pb "github.com/PranavBhatP/raft-kv/proto"
)

type NodeState int

// enum for the node states.
const (
	Follower NodeState = iota
	Candidate
	Leader
)

type RaftNode struct {
	pb.UnimplementedRaftServiceServer

	mu sync.Mutex // mutex lock for the node's state, ensures atomic access to shared variables
	id int32

	state NodeState

	peers map[int32]pb.RaftServiceClient // peerId -> actual peer client object.

	currTerm int64
	votedFor int32
	log      []*pb.LogEntry

	commitIndex int64
	lastApplied int64

	heartbeatC chan struct{}
}

func NewRaftNode(id int32) *RaftNode {
	return &RaftNode{
		id:         id,
		state:      Follower,
		currTerm:   0,
		votedFor:   -1,
		peers:      make(map[int32]pb.RaftServiceClient), // empty map.
		heartbeatC: make(chan struct{}, 1),               // event channel for heartbeat messages from leader.
	}

}

func (n *RaftNode) Run() {
	n.runElectionTimer()
}

func (n *RaftNode) AddPeer(peerId int32, peerClient pb.RaftServiceClient) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers[peerId] = peerClient
}

func (n *RaftNode) runElectionTimer() {
	for {
		timeout := GetRandomElectionTimeout()
		timer := time.NewTimer(timeout)

		select {
		case <-n.heartbeatC:
			timer.Stop()
		case <-timer.C:
			n.StartElection()
		}
	}
}

// StartElection transitions to candidate and increments the local term.
func (n *RaftNode) StartElection() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Candidate
	n.currTerm++
	n.votedFor = int32(n.id)
	currentTerm := n.currTerm

	log.Printf("Node %d started election in term %d", n.id, currentTerm)

	lastLogIndex := int64(len(n.log) - 1)
	lastLogTerm := int64(0)
	if lastLogIndex >= 0 {
		lastLogTerm = n.log[int(lastLogIndex)].Term
	}

	req := &pb.VoteRequest{
		Term:         currentTerm,
		CandidateId:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votesReceived := 1
	quorum := (len(n.peers)+1)/2 + 1

	var wg sync.WaitGroup // waits for a collection of goroutines to finish. integer counter.
	var voteMu sync.Mutex

	for peerId, peerClient := range n.peers {
		wg.Add(1)

		//new go routine for requesting vote from each peer.
		go func(pId int32, c pb.RaftServiceClient) {
			defer wg.Done()
			//timeout for rpc request to peers.
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := c.RequestVote(ctx, req)
			if err != nil {
				log.Printf("Node %d failed to request vote from peer %d: %v", n.id, pId, err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if res.Term > n.currTerm {
				log.Printf("Node %d is stepping down from candidate to follower due to obsolete term %d", n.id, n.currTerm)
				n.currTerm = res.Term
				n.state = Follower
				n.votedFor = -1
				return
			}
			//invalidate candidate if state changes or new term is started.
			if n.state != Candidate || n.currTerm != currentTerm {
				return
			}

			if res.VoteGranted {
				voteMu.Lock()
				votesReceived++
				voteMu.Unlock()

				if votesReceived >= quorum && n.state == Candidate {
					log.Printf("Node %d has received quorum of votes in term %d and is now the leader", n.id, currentTerm)
					n.state = Leader
				}
			}
		}(peerId, peerClient)
	}

	wg.Wait()
}

func (n *RaftNode) broadcastHeartbeat() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		n.sendHeartbeat()
		<-ticker.C

		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
	}
}

func (n *RaftNode) sendHeartbeat() {
	n.mu.Lock()
	//defer n.mu.Unlock()

	currentTerm := n.currTerm
	leaderId := n.id

	req := &pb.AppendRequest{
		Term:     currentTerm,
		LeaderId: leaderId,
	}
	n.mu.Unlock()

	for peerId, peerClient := range n.peers {
		go func(pId int32, c pb.RaftServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			res, err := c.AppendEntries(ctx, req)
			if err != nil {
				log.Printf("Node %d failed to send heartbeat to peer %d: %v", n.id, pId, err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if res.Term > n.currTerm {
				log.Printf("Node %d is stepping down from leader to follower due to obsolete term %d", n.id, n.currTerm)
				//reset the state and term.
				n.currTerm = res.Term
				n.state = Follower
				n.votedFor = -1
				return
			}
		}(peerId, peerClient)
	}
}

func (n *RaftNode) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currTerm {
		//reject the request forr the vote since it is from and outdated candidate.
		n.state = Follower
		n.votedFor = -1

		if req.CandidateId != n.id {
			return &pb.VoteResponse{Term: n.currTerm, VoteGranted: false}, nil
		}

		log.Printf("Node %d rejected vote request from candidate %d in outdated term %d", n.id, req.CandidateId, req.Term)
		return &pb.VoteResponse{Term: n.currTerm, VoteGranted: false}, nil
	}

	if req.Term > n.currTerm {
		//update the current term and become a follower.
		n.currTerm = req.Term
		n.state = Follower
		n.votedFor = -1
		log.Printf("Node %d updated term to %d and became a follower", n.id, n.currTerm)
	}

	lastLogIndex := int64(len(n.log) - 1)
	lastLogTerm := int64(0)

	if lastLogIndex >= 0 {
		lastLogTerm = n.log[int(lastLogIndex)].Term
	}

	logIsUpToDate := false
	if req.LastLogTerm > lastLogTerm {
		logIsUpToDate = true
	} else if req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex {
		logIsUpToDate = true
	}

	if (n.votedFor == -1 || n.votedFor == req.CandidateId) && logIsUpToDate {
		n.votedFor = req.CandidateId
		log.Printf("Node %d voted for candidate %d in term %d", n.id, req.CandidateId, n.currTerm)
		//reset timer since vote has been cast.
		select {
		case n.heartbeatC <- struct{}{}:
		default:
		}

		return &pb.VoteResponse{Term: n.currTerm, VoteGranted: true}, nil
	}

	log.Printf("Node %d rejected vote request from candidate %d in term %d (outdated log/already voted)", n.id, req.CandidateId, n.currTerm)
	return &pb.VoteResponse{Term: n.currTerm, VoteGranted: false}, nil
}

func (n *RaftNode) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currTerm {
		return &pb.AppendResponse{Term: n.currTerm, Success: false}, nil
	}

	if req.Term > n.currTerm {
		n.currTerm = req.Term
	}
	n.state = Follower
	n.votedFor = -1

	// Heartbeat/append received from a valid leader; reset election timer.
	select {
	case n.heartbeatC <- struct{}{}:
	default:
	}

	return &pb.AppendResponse{Term: n.currTerm, Success: true}, nil
}
