package main

import (
	"bufio"
	// "crypto/md5"
	"fmt"
	// "io/ioutil"
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
	portForRPC string
	serverIpPort     string
	domains []Domain
)

type Domain struct {
	Name string
	Pages []Page
}

type Page struct {
	Name string
	Links []Page
}

type CrawlServer int

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
	fmt.Println("serverIpPort:", serverIpPort)

	join()

	fmt.Println("Successfully joined. Server:", serverIpPort, "RPCport:", portForRPC)

	// listen on own ip and port provided by server
	listen(":" + portForRPC)
}

func (p *WorkerRPC) GetLatency(req LatencyReq, latency *int) error {
	fmt.Println("received call to GetLatency()")
	*latency = getLatency(req)
	return nil
}

func (p *WorkerRPC) CrawlPage(req CrawlPageReq, success *bool) error {
	fmt.Println("received call to CrawlPage()")
	crawlPage(req)
	*success = true
	return nil
}

func crawlPage(req CrawlPageReq) {
	page := setNewDomain(req)
	crawl(page, req.Depth)
	return
}

func crawl(page Page, depth int) {
	fmt.Println("Crawling:", page, "to depth:", depth)
}

func setNewDomain(req CrawlPageReq) (page Page) {
	fmt.Println("domains:", domains)
	var pages []Page
	pages = append(pages, Page{req.Url, nil})
	domains = append(domains, Domain{req.Domain, pages})
	fmt.Println("domains after adding new domain:", domains)
	// first page of latest domain added to domains
	page = domains[len(domains) - 1].Pages[0]
	return
}

func getLatency(req LatencyReq) (latency int) {
	var latencyList []int

	for i := 0; i < req.Samples; i++ {
		latency := pingSite(req.URI)
		latencyList = append(latencyList, latency)
	}

	fmt.Println("latencyList before sorting:", latencyList)

	sort.Ints(latencyList)

	fmt.Println("latencyList after sorting:", latencyList)
	
	latency = latencyList[0]
	return
}

func pingSite(uri string) (latency int) {
	start := time.Now()
	_, err := http.Get(uri)
	elapsed := time.Since(start)

	checkError("Error in pingSiteOnce(), http.Get():", err, true)
	
	latency = int(elapsed / time.Millisecond)

	return latency
}


func listen(ipPort string) {
	wServer := rpc.NewServer()
	w := new(WorkerRPC)
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

	portForRPC = strings.Trim(port, " ")
	fmt.Println("My portForWorkerRPC is:", portForRPC)
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
