package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/vishnu1910/Chord_grpc/chordpb" // ensure that your generated proto files are in package chordpb
)

// Global constant: m defines the number of bits in the identifier.
const m = 7

var ringSize = new(big.Int).Exp(big.NewInt(2), big.NewInt(m), nil)

// FingerEntry represents one entry in the finger table.
type FingerEntry struct {
	start uint32
	node  *pb.NodeInfo
}

// Node implements the ChordService server and holds all DHT state.
type Node struct {
	pb.UnimplementedChordServiceServer
	mu sync.Mutex

	ip   string
	port int32
	id   uint32

	predecessor *pb.NodeInfo
	successor   *pb.NodeInfo

	// Finger table with m entries.
	fingerTable []FingerEntry
	// nextFinger pointer indicates the next entry to fix
	nextFinger int

	dataStore map[string]string
}

// NewNode creates and returns a new Node with the given ip and port.
func NewNode(ip string, port int32) *Node {
	node := &Node{
		ip:         ip,
		port:       port,
		dataStore:  make(map[string]string),
		nextFinger: 1, // start fixing from entry 1 (entry 0 is always the immediate successor)
	}
	// Calculate ID from "ip|port"
	input := fmt.Sprintf("%s|%d", ip, port)
	hash := sha256.Sum256([]byte(input))
	bigInt := new(big.Int)
	bigInt.SetString(hex.EncodeToString(hash[:]), 16)
	mod := new(big.Int).Mod(bigInt, ringSize)
	node.id = uint32(mod.Uint64())

	// Initialize finger table for m entries.
	node.fingerTable = make([]FingerEntry, m)
	for i := 0; i < m; i++ {
		// start = (n + 2^i) mod (2^m)
		offset := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(i)), nil)
		nBig := big.NewInt(int64(node.id))
		startBig := new(big.Int).Add(nBig, offset)
		startBig.Mod(startBig, ringSize)
		node.fingerTable[i] = FingerEntry{
			start: uint32(startBig.Uint64()),
			// Initially point to self; will be updated in fixFingers.
			node: &pb.NodeInfo{Ip: ip, Port: port, Id: node.id},
		}
	}

	// Initially, predecessor and successor point to self.
	selfInfo := &pb.NodeInfo{Ip: ip, Port: port, Id: node.id}
	node.predecessor = selfInfo
	node.successor = selfInfo

	return node
}

// ------------ Helper functions ---------------

// between returns true if x is in (a, b] in the circular ID space.
func between(x, a, b uint32) bool {
	if a < b {
		return x > a && x <= b
	}
	// Handle wrap-around.
	return x > a || x <= b
}

// dialNode creates a gRPC client connection to the node identified by ip and port.
func dialNode(ip string, port int32) (pb.ChordServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", ip, port), grpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	client := pb.NewChordServiceClient(conn)
	return client, conn, nil
}

// closestPrecedingFinger returns the closest finger preceding id.
func (n *Node) closestPrecedingFinger(id uint32) *pb.NodeInfo {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i := m - 1; i >= 0; i-- {
		finger := n.fingerTable[i].node
		if finger != nil && between(finger.Id, n.id, id) {
			return finger
		}
	}
	// If none found, return self.
	return &pb.NodeInfo{Ip: n.ip, Port: n.port, Id: n.id}
}

// findSuccessorLocal returns the successor of the given id (using recursion via RPC if needed).
func (n *Node) findSuccessorLocal(id uint32) (*pb.NodeInfo, error) {
	n.mu.Lock()
	// If id is between n and its successor, then successor is responsible.
	if between(id, n.id, n.successor.Id) || n.id == n.successor.Id {
		succ := n.successor
		n.mu.Unlock()
		return succ, nil
	}
	n.mu.Unlock()

	// Otherwise, use the closest preceding finger.
	closest := n.closestPrecedingFinger(id)
	// If the closest finger is ourselves, we have no better choice.
	if closest.Id == n.id {
		n.mu.Lock()
		succ := n.successor
		n.mu.Unlock()
		return succ, nil
	}

	// Make an RPC call to the closest finger.
	client, conn, err := dialNode(closest.Ip, closest.Port)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.FindSuccessor(ctx, &pb.IDRequest{Id: id})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ------------ gRPC Service Methods Implementation ---------------

// GetID returns the node’s own id.
func (n *Node) GetID(ctx context.Context, req *pb.Empty) (*pb.IDResponse, error) {
	return &pb.IDResponse{Id: n.id}, nil
}

// GetSuccessor returns the node’s successor.
func (n *Node) GetSuccessor(ctx context.Context, req *pb.Empty) (*pb.NodeInfo, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.successor, nil
}

// GetPredecessor returns the node’s predecessor.
func (n *Node) GetPredecessor(ctx context.Context, req *pb.Empty) (*pb.NodeInfo, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.predecessor, nil
}

// Insert places a key-value pair into the appropriate node.
// (For simplicity, this example assumes the key belongs to this node.)
func (n *Node) Insert(ctx context.Context, req *pb.InsertRequest) (*pb.InsertResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dataStore[req.Key] = req.Value
	msg := fmt.Sprintf("Key '%s' inserted at node %d", req.Key, n.id)
	return &pb.InsertResponse{Message: msg}, nil
}

// Delete removes a key from the node’s local store.
func (n *Node) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.dataStore, req.Key)
	msg := fmt.Sprintf("Key '%s' deleted at node %d", req.Key, n.id)
	return &pb.DeleteResponse{Message: msg}, nil
}

// Search looks up a key in the node’s local store.
func (n *Node) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if value, ok := n.dataStore[req.Key]; ok {
		return &pb.SearchResponse{Value: value}, nil
	}
	return &pb.SearchResponse{Value: "NOT FOUND"}, nil
}

// Join allows a new node to join the chord ring.
// The joining node calls Join on an existing node in the ring.
func (n *Node) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	joiningNode := req.JoiningNode
	// If joiningNode's ID lies between this node and its successor, update successor.
	if between(joiningNode.Id, n.id, n.successor.Id) || n.id == n.successor.Id {
		n.successor = joiningNode
		n.fingerTable[0].node = joiningNode
	}
	return &pb.JoinResponse{Successor: n.successor}, nil
}

// FindSuccessor returns the successor for a given id.
func (n *Node) FindSuccessor(ctx context.Context, req *pb.IDRequest) (*pb.NodeInfo, error) {
	succ, err := n.findSuccessorLocal(req.Id)
	if err != nil {
		return nil, err
	}
	return succ, nil
}

// FindPredecessor returns the predecessor for a given id.
func (n *Node) FindPredecessor(ctx context.Context, req *pb.IDRequest) (*pb.NodeInfo, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.predecessor, nil
}

// Notify is called by another node that might be our new predecessor.
func (n *Node) Notify(ctx context.Context, req *pb.NotifyRequest) (*pb.NotifyResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	nt := req.Node
	// If no predecessor or nt lies between the current predecessor and self, update.
	if n.predecessor == nil || between(nt.Id, n.predecessor.Id, n.id) {
		n.predecessor = nt
	}
	return &pb.NotifyResponse{Message: "Predecessor updated"}, nil
}

// SendKeys is called by a joining node to transfer keys for which it is now responsible.
func (n *Node) SendKeys(ctx context.Context, req *pb.SendKeysRequest) (*pb.SendKeysResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var kvPairs []*pb.KeyValuePair
	joiningId := req.JoiningNodeId
	for k, v := range n.dataStore {
		// Compute hash for the key.
		h := sha256.New()
		h.Write([]byte(k))
		digest := h.Sum(nil)
		bigInt := new(big.Int).SetBytes(digest)
		keyHash := uint32(new(big.Int).Mod(bigInt, ringSize).Uint64())
		if between(keyHash, n.id, joiningId) {
			kvPairs = append(kvPairs, &pb.KeyValuePair{Key: k, Value: v})
			delete(n.dataStore, k)
		}
	}
	return &pb.SendKeysResponse{KeyValues: kvPairs}, nil
}

// ------------- Background Routines ---------------

// fixFingers periodically updates a single finger table entry in a round-robin fashion.
func (n *Node) fixFingers() {
	for {
		time.Sleep(10 * time.Second)
		// Get the finger table entry to update.
		n.mu.Lock()
		i := n.nextFinger
		// Compute start = (n + 2^i) mod (2^m)
		offset := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(i)), nil)
		nBig := big.NewInt(int64(n.id))
		startBig := new(big.Int).Add(nBig, offset)
		startBig.Mod(startBig, ringSize)
		target := uint32(startBig.Uint64())
		n.mu.Unlock()

		// Find the successor for the target value.
		succ, err := n.findSuccessorLocal(target)
		if err != nil {
			log.Printf("fixFingers RPC error for entry %d: %v", i, err)
		} else {
			n.mu.Lock()
			n.fingerTable[i].node = succ
			n.mu.Unlock()
		}

		// Move to the next finger entry.
		n.mu.Lock()
		n.nextFinger = (n.nextFinger + 1) % m
		if n.nextFinger == 0 {
			n.nextFinger = 1 // always leave index 0 unchanged (immediate successor)
		}
		n.mu.Unlock()
	}
}

// stabilize periodically verifies the immediate successor and notifies it.
func (n *Node) stabilize() {
	for {
		time.Sleep(10 * time.Second)

		// Query the current successor for its predecessor.
		client, conn, err := dialNode(n.successor.Ip, n.successor.Port)
		if err != nil {
			log.Printf("stabilize: error dialing successor: %v", err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		successorPred, err := client.GetPredecessor(ctx, &pb.Empty{})
		cancel()
		conn.Close()

		n.mu.Lock()
		// If successor's predecessor exists and is in-between, update successor.
		if successorPred != nil && between(successorPred.Id, n.id, n.successor.Id) {
			n.successor = successorPred
			n.fingerTable[0].node = successorPred
		}
		n.mu.Unlock()

		// Notify the (possibly updated) successor.
		client2, conn2, err := dialNode(n.successor.Ip, n.successor.Port)
		if err == nil {
			ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
			_, err = client2.Notify(ctx2, &pb.NotifyRequest{
				Node: &pb.NodeInfo{Ip: n.ip, Port: n.port, Id: n.id},
			})
			cancel2()
			conn2.Close()
		}
	}
}

// printFingerTable logs the current finger table for debugging.
func (n *Node) printFingerTable() {
	n.mu.Lock()
	defer n.mu.Unlock()
	log.Printf("Finger table for node %d:", n.id)
	for i, entry := range n.fingerTable {
		log.Printf("Entry %d: start=%d, nodeID=%d", i, entry.start, entry.node.Id)
	}
}

// ------------ Main Function ------------

func main() {
	// Use command-line flags: -port <port> [-join <ip:port>]
	portFlag := flag.Int("port", 50051, "Port for this node")
	joinFlag := flag.String("join", "", "Address (ip:port) of an existing node to join")
	flag.Parse()

	ip := "127.0.0.1"
	port := int32(*portFlag)
	node := NewNode(ip, port)
	log.Printf("Node starting with id %d on %s:%d", node.id, ip, port)

	// If a join address is provided, join an existing chord ring.
	if *joinFlag != "" {
		parts := strings.Split(*joinFlag, ":")
		if len(parts) != 2 {
			log.Fatalf("Invalid join address. Must be in ip:port format.")
		}
		joinIP := parts[0]
		joinPortInt, err := strconv.Atoi(parts[1])
		if err != nil {
			log.Fatalf("Invalid join port: %v", err)
		}
		joinPort := int32(joinPortInt)
		client, conn, err := dialNode(joinIP, joinPort)
		if err != nil {
			log.Fatalf("Error joining ring: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		joinResp, err := client.Join(ctx, &pb.JoinRequest{
			JoiningNode: &pb.NodeInfo{Ip: ip, Port: port, Id: node.id},
		})
		cancel()
		conn.Close()
		if err != nil {
			log.Fatalf("Join RPC failed: %v", err)
		}
		node.mu.Lock()
		node.successor = joinResp.Successor
		node.fingerTable[0].node = joinResp.Successor
		node.mu.Unlock()
		log.Printf("Joined ring; successor is node %d", joinResp.Successor.Id)
	} else {
		log.Println("Creating new ring.")
	}

	// Start gRPC server.
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterChordServiceServer(grpcServer, node)
	// Enable reflection for debugging.
	reflection.Register(grpcServer)

	// Run background routines.
	go node.fixFingers()
	go node.stabilize()
	go func() {
		for {
			time.Sleep(30 * time.Second)
			node.printFingerTable()
		}
	}()

	log.Printf("gRPC server listening on %s:%d", ip, port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}
