package main

import (
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/korkmazkadir/rapidchain/common"
	"github.com/korkmazkadir/rapidchain/consensus"
	"github.com/korkmazkadir/rapidchain/network"
	"github.com/korkmazkadir/rapidchain/registery"
	"github.com/shirou/gopsutil/process"
)

const outputDir = "/Users/hamzaparekh/Projects/Results"

func main() {
	hostname := getEnv("NODE_HOSTNAME", "127.0.0.1")
	registryAddress := getEnv("REGISTRY_ADDRESS", "localhost:1234")

	demux := common.NewDemultiplexer(0)
	server := network.NewServer(demux)
	rpc.Register(server)

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:", hostname))
	if err != nil {
		log.Fatal("listen error:", err)
	}
	log.Printf("✅ Node started at %s\n", listener.Addr())

	go func() {
		for {
			conn, _ := listener.Accept()
			go rpc.ServeConn(conn)
		}
	}()

	nodeInfo := getNodeInfo(listener.Addr().String())
	registry := registery.NewRegistryClient(registryAddress, nodeInfo)
	nodeInfo.ID = registry.RegisterNode()
	log.Printf("🆔 Registered as node ID: %d", nodeInfo.ID)

	config := registry.GetConfig()
	for {
		nodes := registry.GetNodeList()
		if len(nodes) == config.NodeCount {
			log.Printf("✅ All %d nodes registered", config.NodeCount)
			break
		}
		log.Printf("⏳ Waiting for all nodes (%d/%d)", len(nodes), config.NodeCount)
		time.Sleep(2 * time.Second)
	}

	statLogger := common.NewStatLogger(nodeInfo.ID)
	peers := createPeerSet(registry.GetNodeList(), config.GossipFanout, nodeInfo)
	rc := consensus.NewRapidchain(demux, config, peers, statLogger)

	runBenchmark(rc, config, nodeInfo.ID, registry.GetNodeList())
	log.Println("✅ Benchmark complete.")
}

func runBenchmark(rc *consensus.RapidchainConsensus, config registery.NodeConfig, nodeID int, nodeList []registery.NodeInfo) {
	os.MkdirAll(outputDir, os.ModePerm)
	process, _ := process.NewProcess(int32(os.Getpid()))
	filePath := fmt.Sprintf("%s/node_%d_metrics.csv", outputDir, nodeID)

	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal("CSV creation failed:", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	// Added "TxCount" and "Throughput(Tx/s)" columns
	writer.Write([]string{"Round", "IsLeader", "PayloadSize", "TxCount", "Latency(s)", "CPU(%)", "Memory(MB)", "Throughput(Tx/s)"})

	// Keep original payloadSizes loop so we test multiple sizes
	payloadSizes := []int{64, 128, 256, 512, 1024, 2048, 4096, 8192}
	const txCount = 5 // Number of transactions (payloads) per block

	previousBlock := []common.Block{{Issuer: []byte("genesis"), Round: 0, Payload: []byte("start")}}

	round := 1
	for _, payloadSize := range payloadSizes {
		for r := 0; r < config.EndRound; r++ {
			log.Printf("🔁 Round %d - Payload %dB × %d tx", round, payloadSize, txCount)

			isLeader := isElectedAsLeader(nodeList, round, nodeID, config.LeaderCount)
			var blocks []common.Block

			// Generate and concatenate txCount separate payloads of size payloadSize
			var combinedPayload []byte
			for i := 0; i < txCount; i++ {
				chunk := make([]byte, payloadSize)
				rand.Read(chunk)
				combinedPayload = append(combinedPayload, chunk...)
			}

			block := common.Block{
				Round:         round,
				Issuer:        []byte{byte(nodeID)},
				Payload:       combinedPayload,
				PrevBlockHash: hashBlock(previousBlock),
			}

			start := time.Now()
			if isLeader {
				blocks = rc.Propose(round, block, hashBlock(previousBlock))
			} else {
				blocks = rc.Decide(round, hashBlock(previousBlock))
			}
			elapsed := time.Since(start).Seconds()

			mem, _ := process.MemoryInfo()
			cpu, _ := process.CPUPercent()
			memMB := float64(mem.RSS) / 1024.0 / 1024.0

			throughput := float64(txCount) / elapsed

			writer.Write([]string{
				strconv.Itoa(round),
				strconv.FormatBool(isLeader),
				strconv.Itoa(payloadSize),       // total block payload size
				strconv.Itoa(txCount),           // number of tx in block
				fmt.Sprintf("%.6f", elapsed),    // latency in seconds
				fmt.Sprintf("%.2f", cpu),        // CPU %
				fmt.Sprintf("%.2f", memMB),      // Memory MB
				fmt.Sprintf("%.2f", throughput), // throughput (tx/s)
			})
			writer.Flush()

			previousBlock = blocks
			round++
			time.Sleep(500 * time.Millisecond)
		}
	}

	log.Printf("📄 Metrics saved to: %s", filePath)
}

func getEnv(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}

func getNodeInfo(addr string) registery.NodeInfo {
	parts := strings.Split(addr, ":")
	port := 0
	fmt.Sscanf(parts[len(parts)-1], "%d", &port)
	return registery.NodeInfo{IPAddress: parts[0], PortNumber: port}
}

func createPeerSet(nodes []registery.NodeInfo, fanout int, self registery.NodeInfo) network.PeerSet {
	peers := network.PeerSet{}
	count := 0
	for _, n := range nodes {
		if n.ID != self.ID {
			if err := peers.AddPeer(n.IPAddress, n.PortNumber); err == nil {
				log.Printf("🔗 Connected to peer: %s:%d", n.IPAddress, n.PortNumber)
				count++
				if count >= fanout {
					break
				}
			}
		}
	}
	return peers
}

func isElectedAsLeader(nodeList []registery.NodeInfo, round, nodeID, leaderCount int) bool {
	rand.Seed(int64(round))
	rand.Shuffle(len(nodeList), func(i, j int) { nodeList[i], nodeList[j] = nodeList[j], nodeList[i] })

	for i := 0; i < leaderCount && i < len(nodeList); i++ {
		if nodeList[i].ID == nodeID {
			log.Println("🌟 Elected as leader")
			return true
		}
	}
	return false
}

func hashBlock(blocks []common.Block) []byte {
	if len(blocks) == 1 {
		return blocks[0].Hash()
	}
	concat := []byte{}
	for _, b := range blocks {
		concat = append(concat, b.Hash()...)
	}
	h := sha256.New()
	h.Write(concat)
	return h.Sum(nil)
}
