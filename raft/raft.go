package raft

import (
	"context"
	"log"
	"strings"
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

	nextIndex  map[int32]int64 //idx of the next log entry to send to a peer.
	matchIndex map[int32]int64 //idx of the highest log entry known to be replicated on a peer.

	kvStore map[string]string

	heartbeatC chan struct{}
}

func NewRaftNode(id int32) *RaftNode {
	return &RaftNode{
		id:          id,
		state:       Follower,
		currTerm:    0,
		votedFor:    -1,
		peers:       make(map[int32]pb.RaftServiceClient), // empty map.
		kvStore:     make(map[string]string),
		heartbeatC:  make(chan struct{}, 1), // event channel for heartbeat messages from leader.
		commitIndex: -1,
		lastApplied: -1,
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
			n.mu.Lock()
			isLeader := n.state == Leader
			n.mu.Unlock()
			if !isLeader {
				n.StartElection()
			}
		}
	}
}

// StartElection transitions to candidate and increments the local term.
func (n *RaftNode) StartElection() {
	n.mu.Lock()
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
	peersSnapshot := make(map[int32]pb.RaftServiceClient, len(n.peers))
	for peerID, peerClient := range n.peers {
		peersSnapshot[peerID] = peerClient
	}
	n.mu.Unlock()

	var wg sync.WaitGroup // waits for a collection of goroutines to finish. integer counter.
	var voteMu sync.Mutex

	for peerId, peerClient := range peersSnapshot {
		wg.Add(1)

		//new go routine for requesting vote from each peer.
		go func(pId int32, c pb.RaftServiceClient) {
			defer wg.Done()
			//timeout for rpc request to peers.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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
				reachedQuorum := votesReceived >= quorum
				voteMu.Unlock()

				if reachedQuorum && n.state == Candidate {
					log.Printf("Node %d has received quorum of votes in term %d and is now the leader", n.id, currentTerm)
					n.state = Leader
					next := int64(len(n.log))
					n.nextIndex = make(map[int32]int64, len(n.peers))
					n.matchIndex = make(map[int32]int64, len(n.peers))
					for pid := range n.peers {
						n.nextIndex[pid] = next
						n.matchIndex[pid] = -1
					}
					go n.broadcastHeartbeat()
				}
			}
		}(peerId, peerClient)
	}

	wg.Wait()
}

func (n *RaftNode) applyLogs() {
	//as long as a log is unapplied we run the apply logs.
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
		parts := strings.SplitN(entry.Command, " ", 3)
		if len(parts) == 3 && parts[0] == "PUT" {
			n.kvStore[parts[1]] = parts[2]
			log.Printf("Node %d applied PUT command: %s = %s", n.id, parts[1], parts[2])
		}
	}
}

// sends heartbeat to all followers at regular intervals.
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

	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	currentTerm := n.currTerm
	leaderId := n.id
	n.mu.Unlock()

	for peerId, peerClient := range n.peers {
		go func(pId int32, c pb.RaftServiceClient) {
			n.mu.Lock()
			nextIdx := n.nextIndex[pId]
			prevLogIndex := nextIdx - 1
			prevLogTerm := int64(-1)

			if prevLogIndex >= 0 && prevLogIndex < int64(len(n.log)) {
				prevLogTerm = n.log[prevLogIndex].Term
			}
			//copy of the updated log only for the required indices (nextIdx onwards)
			var entries []*pb.LogEntry
			if nextIdx < int64(len(n.log)) {
				entries = n.log[nextIdx:]
			}

			req := &pb.AppendRequest{
				Term:         currentTerm,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: n.commitIndex,
			}
			n.mu.Unlock()

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

			if n.state == Leader && res.Term == n.currTerm {
				if res.Success {
					//follower accepted the log, update our trackers.
					n.matchIndex[pId] = prevLogIndex + int64(len(entries))
					n.nextIndex[pId] = n.matchIndex[pId] + 1
					quorum := (len(n.peers)+1)/2 + 1
					for i := int64(len(n.log) - 1); i > n.commitIndex; i-- {
						// Raft Rule: Only commit entries from our current term
						if n.log[i].Term == n.currTerm {
							matchCount := 1
							for _, matchIdx := range n.matchIndex {
								if matchIdx >= i {
									matchCount++
								}
							}
							if matchCount >= quorum {
								n.commitIndex = i
								n.applyLogs()
								break
							}
						}
					}
				} else {
					// Follower rejected due to log inconsistency. Backtrack and try again.
					n.nextIndex[pId]--
					if n.nextIndex[pId] < 0 {
						n.nextIndex[pId] = 0
					}
				}
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

	if req.Term > n.currTerm || n.state != Follower {
		n.currTerm = req.Term
		n.state = Follower
		n.votedFor = -1
	}

	select {
	case n.heartbeatC <- struct{}{}:
	default:
	}

	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= int64(len(n.log)) {
			return &pb.AppendResponse{Term: n.currTerm, Success: false}, nil
		}

		if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			return &pb.AppendResponse{Term: n.currTerm, Success: false}, nil
		}
	}

	n.log = append(n.log[:req.PrevLogIndex+1], req.Entries...)

	if req.LeaderCommit > n.commitIndex {
		lastNewEntry := int64(len(n.log) - 1)
		if req.LeaderCommit > lastNewEntry {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewEntry
		}
		n.applyLogs()
	}

	return &pb.AppendResponse{Term: n.currTerm, Success: true}, nil
}

func (n *RaftNode) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return &pb.PutResponse{Success: false, LeaderId: -1}, nil
	}
	command := "PUT " + req.Key + " " + req.Value
	entry := &pb.LogEntry{
		Term:    n.currTerm,
		Command: command,
	}
	n.log = append(n.log, entry)
	entryIndex := int64(len(n.log) - 1)
	n.mu.Unlock()

	go n.sendHeartbeat()

	for {
		n.mu.Lock()
		if n.commitIndex >= entryIndex {
			n.mu.Unlock()
			return &pb.PutResponse{Success: true, LeaderId: n.id}, nil
		}
		if n.state != Leader {
			n.mu.Unlock()
			return &pb.PutResponse{Success: false, LeaderId: -1}, nil
		}
		n.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}
func (n *RaftNode) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	value, exists := n.kvStore[req.Key]
	if !exists {
		log.Printf("Leader %d did not find the key %s in the KV store", n.id, req.Key)
		return &pb.GetResponse{Success: false, Value: ""}, nil
	}
	return &pb.GetResponse{Success: true, Value: value}, nil
}
