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
	"strconv"
	"strings"
	// "time"
)

var (
	workerIncomingIpPort string
	clientIncomingIpPort string
	samplesPerWorker 	 int = 5
	workerRPCPort        int = 20000
	workers              []Worker
	domainWorkerMap      map[string]Worker
)

type Worker struct {
	Ip string
}

type WorkerLatency struct {
	Worker Worker
	Latency int
}

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


type WorkerRPC int 

type LatencyReq struct {
	URL string
	Samples int
}

type CrawlPageReq struct {
	Domain string
	URL string
	Depth int
}


func main() {

	err := ParseArguments()
	if err != nil {
		panic(err)
	}
	fmt.Println("workerIncomingIpPort:", workerIncomingIpPort, "clientIncomingIpPort:", clientIncomingIpPort)

	domainWorkerMap = make(map[string]Worker)

	go listenWorkers()
	go listenRpcWorkers()

	listenClients()

}

func (p *MServer) GetWorkers(req GetWorkersReq, resp *GetWorkersRes) error {
	resp.WorkerIPsList = getWorkersIpList()
	return nil
}

func (p *MServer) Crawl(req CrawlReq, resp *CrawlRes) error {
	workerOwnerIp := crawl(req)
	*resp = CrawlRes{workerOwnerIp}
	return nil
}

func (p *MServer) Domains(req DomainsReq, resp *DomainsRes) error {
	domains := getDomains(req)
	*resp = DomainsRes{domains}
	return nil
}

func getDomains(req DomainsReq) (domainsList []string) {
	wIpPort := getWorkerIpPort(Worker{req.WorkerIP})
	getDomReq := true
	client, err := rpc.Dial("tcp", wIpPort)
	checkError("rpc.Dial in getDomains()", err, true)
	err = client.Call("WorkerRPC.GetDomains", getDomReq, &domainsList)
	checkError("client.Call(WorkerRPC.GetDomains in getDomains(): ", err, true)	
	err = client.Close()
	checkError("client.Close() in getDomains(): ", err, true)
	return
}

func getWorkersIpList() (list []string) {
	for _, worker := range workers {
		list = append(list, worker.Ip)
	}
	return
}

func crawl(req CrawlReq) (workerIp string) {
	domain := getDomain(req.URL)
	fmt.Println("domain:", domain)
	schemeAndDomain := getSchemeAndDomain(req.URL)
	fmt.Println("schemeAndDomain:", schemeAndDomain)
	worker := findClosestWorker(schemeAndDomain)
	fmt.Println("url:", req.URL)
	domainWorkerMap[domain] = worker
	fmt.Println("domainWorkerMap:", domainWorkerMap)
	go crawlPage(worker, domain, req)
	workerIp = worker.Ip
	return
} 

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

func getDomain(uri string) (domain string) {
	u, err := url.Parse(uri)
    checkError("Error in getDomain(), url.Parse():", err, true)
	domain = u.Host
	return 
}

func getSchemeAndDomain(uri string) (schemeAndDomain string) {
	u, err := url.Parse(uri)
    checkError("Error in getSchemeAndDomain(), url.Parse():", err, true)
    schemeAndDomain = u.Scheme + "://" + u.Host
    return
}

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

func closestWorker(workerLatencies []WorkerLatency) (worker Worker) {
	workerLatency := workerLatencies[0]
	for i:=1; i<len(workerLatencies); i++ {
		if workerLatencies[i].Latency < workerLatency.Latency {
			workerLatency = workerLatencies[i]	
		}
	}
	return workerLatency.Worker
} 


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

func joinWorker(conn net.Conn) {
	workerIpPort := conn.RemoteAddr().String()
	fmt.Println("joining Workers ip:", workerIpPort)

	workerIp := workerIpPort[:strings.Index(workerIpPort, ":")]

	workers = append(workers, Worker{workerIp})
	
	fmt.Fprintf(conn, strconv.Itoa(workerRPCPort) + " " + getIpPortForRpcFromWorkers() + "\n")
}

func listenClients() {
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

func listenRpcWorkers() {
	mServer := rpc.NewServer()
	m := new(MServer)
	mServer.Register(m)
	// l, err := net.Listen("tcp", clientIncomingIpPort)
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

func getIpPortForRpcFromWorkers() (ipPort string) {
	ipAndPort := strings.Split(workerIncomingIpPort, ":")
	ipPort = ipAndPort[0] +":"+ strconv.Itoa(workerRPCPort+1)
	return
}

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

func getWorkerIpPort(w Worker) (s string) {
	s = w.Ip + ":" + strconv.Itoa(workerRPCPort)
	return
}

func ParseArguments() (err error) {
	arguments := os.Args[1:]
	if len(arguments) == 2 {
		workerIncomingIpPort = arguments[0]
		clientIncomingIpPort = arguments[1]
		// for testing TODO eliminate
		// website = arguments[2]
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
