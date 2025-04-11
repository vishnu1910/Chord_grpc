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

	pb "chordpb" // Make sure your generated proto files are in package chordpb
)

// Global constant m defines the bit-space (m bits -> ring size = 2^m).
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

	fingerTable []FingerEntry
	dataStore   map[string]string
}

// NewNode creates and returns a new Node with the given ip and port.
func NewNode(ip string, port int32) *Node {
	node := &Node{
		ip:        ip,
		port:      port,
		dataStore: make(map[string]string),
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
			// initialize all entries with self; will be updated over time.
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
// Here, for simplicity, we assume that a lookup has been done and this node is responsible.
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
	// In a full implementation, this node would help the joining node find its proper successor.
	// Here we use a simple check: if joiningNode.Id is between self and current successor, then update.
	if between(joiningNode.Id, n.id, n.successor.Id) || n.id == n.successor.Id {
		n.successor = joiningNode
		// Update finger table at index 0.
		n.fingerTable[0].node = joiningNode
	}
	return &pb.JoinResponse{Successor: n.successor}, nil
}

// FindSuccessor returns the successor for a given id.
// If the id is between this node and its successor, it returns successor;
// otherwise, it asks the closest preceding node (simplified here by returning successor).
func (n *Node) FindSuccessor(ctx context.Context, req *pb.IDRequest) (*pb.NodeInfo, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if between(req.Id, n.id, n.successor.Id) || n.id == n.successor.Id {
		return n.successor, nil
	}
	// In a complete implementation, perform recursive lookup via closestPrecedingNode.
	return n.successor, nil
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
	// If no predecessor or if nt lies between current predecessor and self, update.
	if n.predecessor == nil || between(nt.Id, n.predecessor.Id, n.id) {
		n.predecessor = nt
	}
	return &pb.NotifyResponse{Message: "Predecessor updated"}, nil
}

// SendKeys is called by a joining node to receive keys for which it is now responsible.
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
		// If the key hash lies between this node and the joining node, transfer it.
		if between(keyHash, n.id, joiningId) {
			kvPairs = append(kvPairs, &pb.KeyValuePair{Key: k, Value: v})
			delete(n.dataStore, k)
		}
	}
	return &pb.SendKeysResponse{KeyValues: kvPairs}, nil
}

// ------------- Background Routines ---------------

// fixFingers periodically updates one entry in the finger table.
func (n *Node) fixFingers() {
	for {
		time.Sleep(10 * time.Second)
		n.mu.Lock()
		// Choose a random finger index (except 0, which is the immediate successor).
		i := rand.Intn(m-1) + 1 // index 1..m-1
		// Compute (n + 2^i) mod ringSize
		offset := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(i)), nil)
		nBig := big.NewInt(int64(n.id))
		startBig := new(big.Int).Add(nBig, offset)
		startBig.Mod(startBig, ringSize)
		target := uint32(startBig.Uint64())
		n.mu.Unlock()

		// Perform FindSuccessor RPC on self (in a complete system, you’d forward this request).
		connInfo := &pb.IDRequest{Id: target}
		resp, err := n.FindSuccessor(context.Background(), connInfo)
		if err != nil {
			log.Printf("fixFingers RPC error: %v", err)
			continue
		}

		n.mu.Lock()
		n.fingerTable[i].node = resp
		n.mu.Unlock()
	}
}

// stabilize periodically verifies the immediate successor and notifies it.
func (n *Node) stabilize() {
	for {
		time.Sleep(10 * time.Second)
		var successorPred *pb.NodeInfo

		// Get the predecessor of our successor.
		client, conn, err := dialNode(n.successor.Ip, n.successor.Port)
		if err != nil {
			log.Printf("stabilize: error dialing successor: %v", err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		successorPred, err = client.GetPredecessor(ctx, &pb.Empty{})
		cancel()
		conn.Close()

		n.mu.Lock()
		// If successor's predecessor exists and lies between this node and successor, update.
		if successorPred != nil && between(successorPred.Id, n.id, n.successor.Id) {
			n.successor = successorPred
			n.fingerTable[0].node = successorPred
		}
		// Notify the (new) successor.
		client2, conn2, err := dialNode(n.successor.Ip, n.successor.Port)
		if err == nil {
			ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
			_, err = client2.Notify(ctx2, &pb.NotifyRequest{
				Node: &pb.NodeInfo{Ip: n.ip, Port: n.port, Id: n.id},
			})
			cancel2()
			conn2.Close()
		}
		n.mu.Unlock()
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

	// If a join address is provided, attempt to join the existing chord ring.
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
		// Send a Join RPC to the known node.
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
		// Creating a new ring: predecessor and successor remain self.
		log.Println("Creating new ring.")
	}

	// Start gRPC server.
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterChordServiceServer(grpcServer, node)
	// Enable reflection to simplify debugging with tools like grpcurl.
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
