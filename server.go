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
	"time"
)

var (
	workerIncomingIpPort string
	clientIncomingIpPort string
	samplesPerWorker 	 int = 5
	workerRPCPort        int = 20000
	workers              []Worker
	domainWorkerMap      map[string]Worker
	// for testing TODO eliminate
	website string
)

type Worker struct {
	Ip string
}

type WorkerLatency struct {
	Worker Worker
	Latency int
}

type WorkerRPC int 

type LatencyReq struct {
	URI string
	Samples int
}

type CrawlPageReq struct {
	Domain string
	Url string
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

	test()
}

func test() {
	// for testing without client
	for {
		if len(workers) > 1 {
			time.Sleep(2 * time.Second)
			break
		}
		// to allow azure to work...
		time.Sleep(1 * time.Second)
	}

	fmt.Println("Calling crawl(", website,"0)")
	workerIp := crawl(website, 0)

	fmt.Println("Used worker:", workerIp, "to crawl:", website)

	fmt.Println("Bye bye...")
}

func crawl(url string, depth int) (workerIp string) {
	worker := findClosestWorker(website)
	fmt.Println("The closest worker is:", worker)
	fmt.Println("url:", url)
	domain := getDomain(url)
	fmt.Println("domain:", domain)
	domainWorkerMap[domain] = worker
	fmt.Println("domainWorkerMap:", domainWorkerMap)
	crawlPage(worker, domain, url, depth)
	workerIp = worker.Ip
	return
} 

func crawlPage(worker Worker, domain string, url string, depth int) {
	wIpPort := getWorkerIpPort(worker)
	req := CrawlPageReq{domain, url, depth}
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
	
	// TODO change to not require space delimiter
	// send to socket
	fmt.Fprintf(conn, strconv.Itoa(workerRPCPort)+" ")
}

// func listenClient() {
// 	mServer := rpc.NewServer()
// 	m := new(MServer)
// 	mServer.Register(m)
// 	l, err := net.Listen("tcp", clientIncomingIpPort)
// 	if err != nil {
// 		panic(err)
// 	}
// 	for {
// 		conn, err := l.Accept()
// 		if err != nil {
// 			panic(err)
// 		}
// 		go mServer.ServeConn(conn)
// 	}
// }

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
	if len(arguments) == 3 {
		workerIncomingIpPort = arguments[0]
		clientIncomingIpPort = arguments[1]
		// for testing TODO eliminate
		website = arguments[2]
	} else {
		err = fmt.Errorf("Usage: {go run server.go [worker-incoming ip:port] [client-incoming ip:port] [website]}")
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
