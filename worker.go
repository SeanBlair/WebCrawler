package main

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

var (
	portForWorkerRPC string
	serverIpPort     string
)

type MServer int

type WorkerServer int

// A stats struct that summarizes a set of latency measurements to an
// internet host.
type LatencyStats struct {
	Min    int // min measured latency in milliseconds to host
	Median int // median measured latency in milliseconds to host
	Max    int // max measured latency in milliseconds to host
}

// Request that client sends in RPC call to MServer.MeasureWebsite
type MWebsiteReq struct {
	URI              string // URI of the website to measure
	SamplesPerWorker int    // Number of samples, >= 1
}

type LatencyAndHash struct {
	Stats    LatencyStats
	SiteHash [16]byte
}

type ServerPing struct {
	Id    int
	Acked bool
}

func main() {

	err := ParseArguments()
	if err != nil {
		panic(err)
	}
	fmt.Println("serverIpPort:", serverIpPort)

	join()

	fmt.Println("Successfully joined. Server:", serverIpPort, "RPCport:", portForWorkerRPC)

	// listen on own ip, specific port so server knows how to access
	listen(":" + portForWorkerRPC)
}

func (p *WorkerServer) PingSite(req MWebsiteReq, resp *LatencyAndHash) error {
	fmt.Println("received call to PingSite")
	*resp = pingSite(req)
	return nil
}

func (p *WorkerServer) PingServer(samples int, resp *LatencyStats) error {
	fmt.Println("received call to PingServer")
	*resp = pingServer(samples)
	return nil
}

func pingServer(samples int) (stats LatencyStats) {
	var pings []ServerPing
	for i := 1; i <= samples; i++ {
		pings = append(pings, ServerPing{i, false})
	}

	fmt.Println("samples is:", samples, "and initialized pings:", pings)
	var latencyList []int

	missedAck := isMissedAck(pings)

	for missedAck {
		for index, ping := range pings {
			if ping.Acked == false {
				pingServerOnce(ping.Id)
				fmt.Println("pinged server with pingId:", ping.Id)
				// start timer
				start := time.Now()

				pingResponseAddr, err := net.ResolveUDPAddr("udp", ":"+portForWorkerRPC)
				checkError("Error in pingServer(), net.ResolveUDPAddr():", err, true)

				responseConn, err := net.ListenUDP("udp", pingResponseAddr)
				checkError("Error in pingServer(), net.ListenUDP():", err, true)
				// start timeout
				responseConn.SetReadDeadline(time.Now().Add(time.Second * 1))
				// get response
				buffer := make([]byte, 10)
				_, _, err = responseConn.ReadFromUDP(buffer)
				// timed out
				if err, ok := err.(net.Error); ok && err.Timeout() {
					fmt.Println("detected a timeout for pingId:", ping.Id)
					responseConn.Close()
				} else if err != nil {
					checkError("Error in pingServer(), responseConn.ReadFromUDP():", err, true)
				} else {
					fmt.Println("received ack pingid:", int(buffer[0]))
					if int(buffer[0]) == ping.Id {
						pings[index].Acked = true
						elapsed := time.Since(start)
						latencyList = append(latencyList, int(elapsed/time.Millisecond))

						fmt.Println("After correct ack received, pings:", pings, "and latencyList:", latencyList)
					} else {
						fmt.Println("detected a incorrect ack id:", int(buffer[0]), "and pings:", pings)
					}
					responseConn.Close()
				}
			}
		}
		missedAck = isMissedAck(pings)
	}

	missedDoneAck := true
	for missedDoneAck {
		pingServerOnce(0)

		pingResponseAddr, err := net.ResolveUDPAddr("udp", ":"+portForWorkerRPC)
		checkError("Error in pingServer(), net.ResolveUDPAddr():", err, true)

		responseConn, err := net.ListenUDP("udp", pingResponseAddr)
		checkError("Error in pingServer(), net.ListenUDP():", err, true)
		// start timeout
		responseConn.SetReadDeadline(time.Now().Add(time.Second * 1))
		// get response
		buffer := make([]byte, 10)
		_, _, err = responseConn.ReadFromUDP(buffer)
		// timed out
		if err, ok := err.(net.Error); ok && err.Timeout() {
			fmt.Println("detected a timeout for pingId:", int(buffer[0]))
			responseConn.Close()
		} else if err != nil {
			checkError("Error in pingServer(), responseConn.ReadFromUDP():", err, true)
		} else {
			fmt.Println("received ack pingid:", int(buffer[0]))
			if int(buffer[0]) == 0 {
				missedDoneAck = false
			} else {
				fmt.Println("detected a incorrect ack id:", int(buffer[0]), "and pings:", pings)
			}
			responseConn.Close()
		}
	}

	// set stats
	sort.Ints(latencyList)

	fmt.Println("latencyList after sorting:", latencyList)
	min := latencyList[0]
	max := latencyList[len(latencyList)-1]
	median := getMedian(latencyList)
	stats = LatencyStats{min, median, max}

	return
}

func isMissedAck(serverPings []ServerPing) (b bool) {
	b = false
	for _, ping := range serverPings {
		if !ping.Acked {
			return true
		}
	}
	return
}

func pingServerOnce(pingId int) {
	pingAddr, err := net.ResolveUDPAddr("udp", ":"+portForWorkerRPC)
	checkError("Error in pingServerOnce(), net.ResolveUDPAddr():", err, true)

	serverAddr, err := net.ResolveUDPAddr("udp", serverIpPort)
	checkError("Error in pingServerOnce(), net.ResolveUDPAddr():", err, true)

	conn, err := net.DialUDP("udp", pingAddr, serverAddr)
	checkError("Error in pingServerOnce(), net.DialUDP():", err, true)
	_, err = conn.Write([]byte{byte(pingId)})
	checkError("Error in pingServerOnce(), conn.Write():", err, true)
	fmt.Println("Pinged server from address:", pingAddr, "to address:", serverAddr)

	conn.Close()
}

func pingSite(req MWebsiteReq) (statsNHash LatencyAndHash) {
	var latencyList []int

	for i := 0; i < req.SamplesPerWorker; i++ {
		latency := pingSiteOnce(req.URI)
		latencyList = append(latencyList, latency)
	}

	fmt.Println("latencyList before sorting:", latencyList)

	sort.Ints(latencyList)

	fmt.Println("latencyList after sorting:", latencyList)
	min := latencyList[0]
	max := latencyList[len(latencyList)-1]
	median := getMedian(latencyList)
	statsNHash.Stats = LatencyStats{min, median, max}
	statsNHash.SiteHash = getHash(req.URI)
	return
}

func getHash(uri string) (h [16]byte) {
	res, err := http.Get(uri)
	checkError("Error in getHash(), http.Get():", err, true)
	html, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	checkError("Error in getHash(), ioutil.ReadAll()", err, true)
	data := []byte(html)
	h = md5.Sum(data)
	return
}

func pingSiteOnce(uri string) (l int) {
	start := time.Now()
	// res, err := http.Get(uri)
	_, err := http.Get(uri)
	elapsed := time.Since(start)

	checkError("Error in pingSiteOnce(), http.Get():", err, true)
	// html, err := ioutil.ReadAll(res.Body)
	// res.Body.Close()
	// checkError("Error in pingSiteOnce(), ioutil.ReadAll():", err, true)
	// fmt.Printf("%s", html)

	l = int(elapsed / time.Millisecond)

	return l
}

// list is sorted
func getMedian(list []int) (m int) {
	length := len(list)
	var middle int = length / 2
	if (length % 2) == 1 {
		return list[middle]
	} else {
		a := list[middle-1]
		b := list[middle]
		return (a + b) / 2
	}
}

func listen(ipPort string) {
	wServer := rpc.NewServer()
	w := new(WorkerServer)
	wServer.Register(w)
	l, err := net.Listen("tcp", ipPort)
	if err != nil {
		panic(err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}
		go wServer.ServeConn(conn)
	}
}

func join() {

	conn, err := net.Dial("tcp", serverIpPort)
	checkError("Error in join(), net.Dial()", err, true)

	fmt.Println("dialed server")

	// TODO make more elegant than space delimiter...
	port, err := bufio.NewReader(conn).ReadString(' ')
	checkError("Error in join(), bufio.NewReader(conn).ReadString()", err, true)
	fmt.Println("Message from server: ", port)

	portForWorkerRPC = strings.Trim(port, " ")
	fmt.Println("My portForWorkerRPC is:", portForWorkerRPC)
}

func ParseArguments() (err error) {
	arguments := os.Args[1:]
	if len(arguments) == 1 {
		serverIpPort = arguments[0]
	} else {
		err = fmt.Errorf("Usage: {go run server.go [server ip:port]}")
		return
	}
	return
}

// Prints msg + err to console and exits program if exit == true
func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}
