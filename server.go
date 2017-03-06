/*
Implements the server in assignment 5 for UBC CS 416 2016 W2.

Usage:

go run server.go [worker-incoming ip:port] [client-incoming ip:port]

Example:

go run server.go 127.0.0.1:1111 127.0.0.1:2222
*/

package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/url"
	"os"
	// "strconv"
	"strings"
	// "time"
)

var (
	workerIncomingIpPort string
	clientIncomingIpPort string
	samplesPerWorker     int    = 5
	workerRPCPort        string = "20000"
	serverRPCPort        string = "30000"
	workers              []Worker
	domainWorkerMap      map[string]Worker
)

type Worker struct {
	Ip string
}

type WorkerLatency struct {
	Worker  Worker
	Latency int
}

// RPC server type
type MServer int

// Request that client sends in RPC call to MServer.GetWorkers
type GetWorkersReq struct{}

// Response to MServer.GetWorkers
type GetWorkersRes struct {
	WorkerIPsList []string // List of workerIP string
}

// Request that client sends in RPC call to MServer.Crawl
type CrawlReq struct {
	URL   string // URL of the website to crawl
	Depth int    // Depth to crawl to from URL
}

// Response to MServer.Crawl
type CrawlRes struct {
	WorkerIP string // workerIP
}

// Request that client sends in RPC call to MServer.Domains
type DomainsReq struct {
	WorkerIP string // IP of worker
}

// Response to MServer.Domains
type DomainsRes struct {
	Domains []string // List of domain string
}

// Request that client sends in RPC call to MServer.Overlap
type OverlapReq struct {
	URL1 string // URL arg to Overlap
	URL2 string // The other URL arg to Overlap
}

// Response to MServer.Overlap
type OverlapRes struct {
	NumPages int // Computed overlap between two URLs
}

// Worker RPC server type
type WorkerRPC int

// Request that server sends in RPC call to WorkerRPC.GetLatency
type LatencyReq struct {
	URL     string 	// Site to compute latency of
	Samples int   	// Number of measurents to take
}

// Request that server sends in RPC call to WorkerRPC.CrawlPage
type CrawlPageReq struct {
	Domain string 	// Domain of URL
	URL    string 	// Site to crawl
	Depth  int 		// Depth to crawl URL
}

// Request that server sends in RPC call to WorkerRPC.ComputeOverlap
type ComputeOverlapReq struct {
	OwnerURL1 Worker 	// Worker to perform overlap computation
	URL1 string 		// Site to start search of URL2's domain from
	OwnerURL2 Worker 	// Owner of domain of URL2
	URL2 string 		// Site to start search for entry point
}						// of URL2's domain accessed through URL1

func main() {

	err := ParseArguments()
	if err != nil {
		panic(err)
	}
	fmt.Println("workerIncomingIpPort:", workerIncomingIpPort, "clientIncomingIpPort:", clientIncomingIpPort)

	domainWorkerMap = make(map[string]Worker)

	// listens for workers joining the system
	go listenWorkers()
	// listens for RPC calls from the workers
	go listenRpcWorkers()
	// listens for RPC calls from the client
	listenClient()

}

// Returns to client the workers that have joined the system
func (p *MServer) GetWorkers(req GetWorkersReq, resp *GetWorkersRes) error {
	resp.WorkerIPsList = getWorkersIpList()
	return nil
}

// Crawls a URL to a given depth, returns the workerIp with less latency to site
func (p *MServer) Crawl(req CrawlReq, resp *CrawlRes) error {
	workerOwnerIp := crawl(req)
	*resp = CrawlRes{workerOwnerIp}
	return nil
}

// Returns the domains that a given workerIp is responsible for
func (p *MServer) Domains(req DomainsReq, resp *DomainsRes) error {
	domains := getDomains(req)
	*resp = DomainsRes{domains}
	return nil
}

// Returns the overlap in the web-graph between urls from different domains
func (p *MServer) Overlap(req OverlapReq, resp *OverlapRes) error {
	numPages := getOverlap(req)
	*resp = OverlapRes{numPages}
	return nil
}

// Determines wokers to process request, requests the computation and returns the overlap
func getOverlap(request OverlapReq) (numPages int) {
	worker1 := getOwner(request.URL1)
	worker2 := getOwner(request.URL2)
	numPages = computeOverlap(worker1, request.URL1, worker2, request.URL2)
	numPages += computeOverlap(worker2, request.URL2, worker1, request.URL1)
	return
}

// Calls ownerUrl1 worker's WorkerRPC.ComputeOverlap RPC, returns overlap between url1 and url2
func computeOverlap(ownerUrl1 Worker, url1 string, ownerUrl2 Worker, url2 string) (numPages int) {
	wIpPort := getWorkerIpPort(ownerUrl1)
	req := ComputeOverlapReq{ownerUrl1, url1, ownerUrl2, url2}
	client, err := rpc.Dial("tcp", wIpPort)
	checkError("rpc.Dial in computeOverlap()", err, true)
	err = client.Call("WorkerRPC.ComputeOverlap", req, &numPages)
	checkError("client.Call(WorkerRPC.ComputeOverlap: ", err, true)
	err = client.Close()
	checkError("client.Close() in computeOverlap(): ", err, true)
	return
}

// Returns worker responsible for the domain of the given url
func getOwner(url string) (worker Worker) {
	domain := getDomain(url)
	worker = domainWorkerMap[domain]
	return
}

// Returns the domains that a given worker is responsible for
func getDomains(req DomainsReq) (domainsList []string) {
	for k := range domainWorkerMap {
		if domainWorkerMap[k].Ip == req.WorkerIP {
			domainsList = append(domainsList, k)
		}
	}
	return
}

// Returns all workerIp that have joined the server
func getWorkersIpList() (list []string) {
	for _, worker := range workers {
		list = append(list, worker.Ip)
	}
	return
}

// Ensures domain is mapped to closest worker and tells worker to crawl
// Returns the ip of worker that owns the domain of Url
func crawl(req CrawlReq) (workerIp string) {
	domain := getDomain(req.URL)
	var worker Worker
	if isKnown(domain) {
		worker = domainWorkerMap[domain]
	} else {
		schemeAndDomain := getSchemeAndDomain(req.URL)
		worker = findClosestWorker(schemeAndDomain)
		domainWorkerMap[domain] = worker
		fmt.Println("domainWorkerMap:", domainWorkerMap)
	}
	crawlPage(worker, domain, req)
	workerIp = worker.Ip
	return
}

// Returns true if domain has been processed by the server
func isKnown(domain string) bool {
	_, ok := domainWorkerMap[domain]
	return ok
}

// Calls WorkerRPC.CrawlPage to worker responsible for domain
func crawlPage(worker Worker, domain string, crawlReq CrawlReq) {
	wIpPort := getWorkerIpPort(worker)
	req := CrawlPageReq{domain, crawlReq.URL, crawlReq.Depth}
	var resp bool
	client, err := rpc.Dial("tcp", wIpPort)
	checkError("rpc.Dial in crawlPage()", err, true)
	err = client.Call("WorkerRPC.CrawlPage", req, &resp)
	checkError("client.Call(WorkerRPC.CrawlPage: ", err, true)
	err = client.Close()
	checkError("client.Close() in crawlPage(): ", err, true)
	return
}

// Returns the domain of uri
func getDomain(uri string) (domain string) {
	u, err := url.Parse(uri)
	checkError("Error in getDomain(), url.Parse():", err, true)
	domain = u.Host
	return
}

// Prepares domain for an http.GET call
func getSchemeAndDomain(uri string) (schemeAndDomain string) {
	u, err := url.Parse(uri)
	checkError("Error in getSchemeAndDomain(), url.Parse():", err, true)
	schemeAndDomain = u.Scheme + "://" + u.Host
	return
}

// Compares network latencies of all workers to url, returns the closest
func findClosestWorker(url string) (worker Worker) {
	var workerLatencyList []WorkerLatency
	for _, worker := range workers {
		latency := getLatency(worker, url)
		fmt.Println("Worker:", worker, "says latency is:", latency)
		workerLatencyList = append(workerLatencyList, WorkerLatency{worker, latency})
	}
	worker = closestWorker(workerLatencyList)
	return
}

// Returns worker with smallest previously computed network latency
func closestWorker(workerLatencies []WorkerLatency) (worker Worker) {
	workerLatency := workerLatencies[0]
	for i := 1; i < len(workerLatencies); i++ {
		if workerLatencies[i].Latency < workerLatency.Latency {
			workerLatency = workerLatencies[i]
		}
	}
	return workerLatency.Worker
}

// Listens for joining workers
func listenWorkers() {
	ln, err := net.Listen("tcp", workerIncomingIpPort)
	checkError("Error in listenWorkers(), net.Listen():", err, true)
	for {
		conn, err := ln.Accept()
		checkError("Error in listenWorkers(), ln.Accept():", err, true)
		joinWorker(conn)
		fmt.Println("Worker joined. Workers:", workers)
	}
}

// Adds joining workerIp to list and returns port to use as RPC server
// and port to contact server with RPCs
func joinWorker(conn net.Conn) {
	workerIpPort := conn.RemoteAddr().String()
	fmt.Println("joining Workers ip:", workerIpPort)

	workerIp := workerIpPort[:strings.Index(workerIpPort, ":")]

	workers = append(workers, Worker{workerIp})

	fmt.Fprintf(conn, workerRPCPort+" "+serverRPCPort+"\n")
}

// Listens for RPC calls from client
func listenClient() {
	mServer := rpc.NewServer()
	m := new(MServer)
	mServer.Register(m)
	l, err := net.Listen("tcp", clientIncomingIpPort)
	if err != nil {
		panic(err)
	}
	fmt.Println("listening for rpc calls on:", clientIncomingIpPort)
	for {
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}
		go mServer.ServeConn(conn)
	}
}

// Listens for RPC calls from workers
func listenRpcWorkers() {
	mServer := rpc.NewServer()
	m := new(MServer)
	mServer.Register(m)
	ipPort := getIpPortForRpcFromWorkers()

	l, err := net.Listen("tcp", ipPort)
	if err != nil {
		panic(err)
	}
	fmt.Println("listening for rpc calls from workers on:", ipPort)
	for {
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}
		go mServer.ServeConn(conn)
	}
}

// returns the ip:port to listen to worker RPCs on
func getIpPortForRpcFromWorkers() (ipPort string) {
	ipAndPort := strings.Split(workerIncomingIpPort, ":")

	ipPort = ipAndPort[0] + ":" + serverRPCPort
	return
}

// Calls WorkerRPC.GetLatency RPC to given worker
func getLatency(w Worker, url string) (latency int) {
	wIpPort := getWorkerIpPort(w)
	req := LatencyReq{url, samplesPerWorker}
	client, err := rpc.Dial("tcp", wIpPort)
	checkError("rpc.Dial in getLatency()", err, true)
	err = client.Call("WorkerRPC.GetLatency", req, &latency)
	checkError("client.Call(WorkerRPC.GetLatency: ", err, true)
	err = client.Close()
	checkError("client.Close() in getLatency call: ", err, true)
	return
}

// Returns the ip:port that given worker is listening to RPCs on
func getWorkerIpPort(w Worker) (s string) {
	s = w.Ip + ":" + workerRPCPort
	return
}

// Parses the command line arguments to server.go
func ParseArguments() (err error) {
	arguments := os.Args[1:]
	if len(arguments) == 2 {
		workerIncomingIpPort = arguments[0]
		clientIncomingIpPort = arguments[1]
	} else {
		err = fmt.Errorf("Usage: {go run server.go [worker-incoming ip:port] [client-incoming ip:port]}")
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
