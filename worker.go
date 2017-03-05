package main

import (
	"bufio"
	// "crypto/md5"
	"fmt"
	"golang.org/x/net/html"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

var (
	// for listening to RPCs
	portForRPC string
	// for joining with the server
	serverIpPort string
	// for sending RPCs to the server
	serverRpcPort string
	//  maps domains to urls which map to page data
	// TODO make sure thread safe as is map
	domains map[string]map[string]Page
)

// A webpage
type Page struct {
	// -1 if not crawled, 0 if all links set,
	// greater depending on how far crawled.
	DepthCrawled int
	// Urls that page links to
	Links []string
}

// The server RPC type
type MServer int

// The worker RPC type
type WorkerRPC int

// Request that server sends in WorkerRPC.GetLatency RPC call
type LatencyReq struct {
	URL     string // Top level domain page
	Samples int    // Number of latency samples to take
}

// Request that server sends in WorkerRPC.CrawlPage RPC call
type CrawlPageReq struct {
	Domain string // Domain of URL
	URL    string // Page to crawl
	Depth  int    // Depth to crawl
}

// Request that worker sends in RPC call to MServer.Crawl
type CrawlReq struct {
	URL   string // URL of the website to crawl
	Depth int    // Depth to crawl to from URL
}

// Response to MServer.Crawl
type CrawlRes struct {
	WorkerIP string // workerIP
}

func main() {

	err := ParseArguments()
	if err != nil {
		panic(err)
	}

	domains = make(map[string]map[string]Page)

	join()

	fmt.Println("Successfully joined. Server:", serverIpPort, "RPCport:", portForRPC, "serverRpcPort:", serverRpcPort)

	// listen on own ip and port provided by server
	listen(":" + portForRPC)
}

// Returns min latency from worker to given domain
func (p *WorkerRPC) GetLatency(req LatencyReq, latency *int) error {
	fmt.Println("received call to GetLatency()")
	*latency = getLatency(req)
	return nil
}

// Crawls page to given depth
func (p *WorkerRPC) CrawlPage(req CrawlPageReq, success *bool) error {
	fmt.Println("received call to CrawlPage() with req:", req)
	initCrawl(req)
	*success = true
	return nil
}

// Verifies that page is in domains map
func initCrawl(req CrawlPageReq) {
	fmt.Println("domains before initCrawl() processes:", domains)

	// make sure domain exists
	_, ok := domains[req.Domain]
	if !ok {
		domains[req.Domain] = make(map[string]Page)
	}
	// make sure page exists
	_, ok = domains[req.Domain][req.URL]
	if !ok {
		domains[req.Domain][req.URL] = Page{-1, nil}
	}

	fmt.Println("domains after initCrawl() processes:", domains)
	crawlPage(req)
}

// Crawls page and links to given depth
// requires entry in domains[req.Domain][req.Url] : Page{x, y}
func crawlPage(req CrawlPageReq) {
	page := domains[req.Domain][req.URL]
	// need to crawl deeper
	if req.Depth > page.DepthCrawled {
		// never crawled, so links unknown
		if page.DepthCrawled == -1 {
			page.Links = parseLinks(req.URL)
		}
		// previously crawled but to lesser depth
		page.DepthCrawled = req.Depth

		// set page with updated depth and correct links
		// TODO mutex??
		domains[req.Domain][req.URL] = page
		fmt.Println("domains after crawling page:", req.URL, "are:", domains)
		// process links (at least add them to domains, if not crawling)
		// every link string should appear as an entry in its domain map
		// by some worker.
		for _, link := range page.Links {
			linkDomain := getDomain(link)
			if !isMyDomain(linkDomain) {
				serverCrawl(link, req.Depth-1)
			} else {
				initCrawl(CrawlPageReq{linkDomain, link, req.Depth - 1})
			}
		}
		// already crawled deep enough
	} else {
		return
	}
}

// Calls server to initiate a crawl of a page in a domain that
// this worker currently does not own.
func serverCrawl(url string, depth int) {
	req := CrawlReq{url, depth}
	var resp CrawlRes

	client, err := rpc.Dial("tcp", getServerRpcIpPort())

	checkError("rpc.Dial in serverCrawl()", err, true)
	err = client.Call("MServer.Crawl", req, &resp)
	checkError("client.Call(MServer.Crawl: ", err, true)
	fmt.Println("Server responded to MServer.Crawl() with:", resp)
	err = client.Close()
	checkError("client.Close() in serverCrawl(): ", err, true)
}

// Returns ip:port of server listening to worker RPCs
// TODO set as global variable instead of recomputing.
func getServerRpcIpPort() (ipPort string) {
	ipAndPort := strings.Split(serverIpPort, ":")
	ipPort = ipAndPort[0] + ":" + serverRpcPort
	return
}

// Returns true if domain is in domains map
func isMyDomain(domain string) bool {
	_, ok := domains[domain]
	return ok
}

// Returns the domain of uri
func getDomain(uri string) (domain string) {
	u, err := url.Parse(uri)
	checkError("Error in getDomain(), url.Parse():", err, true)
	domain = u.Host
	return
}

// Returns a list of valid urls linked from given uri
func parseLinks(uri string) (links []string) {
	htmlString := getHtmlString(uri)
	urls := getAllLinks(htmlString)
	fmt.Println("Urls returned from getAllLinks() in parseLinks():", urls)
	links = fixRelativeUrls(uri, urls)
	fmt.Println("Urls after fixed with fixRelativeUrls():", links)
	return
}

// Turns any relative urls into complete urls
func fixRelativeUrls(baseUrl string, relativeUrls []string) (urls []string) {
	base, err := url.Parse(baseUrl)
	checkError("Error in fixRelativeUrls(), url.Parse("+baseUrl+"):", err, true)
	for _, uri := range relativeUrls {
		u, err := url.Parse(uri)
		checkError("Error in fixRelativeUrls(), url.Parse("+uri+"):", err, true)
		urls = append(urls, base.ResolveReference(u).String())
	}
	return
}

// Returns the values of all <a/> link html tags
func getAllLinks(htmlString string) (urls []string) {
	doc, err := html.Parse(strings.NewReader(htmlString))
	checkError("Error in setAllLinks(), html.Parse():", err, true)

	// based on https://godoc.org/golang.org/x/net/html#example-Parse
	var f func(*html.Node)
	f = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, a := range n.Attr {
				if a.Key == "href" {
					urls = append(urls, a.Val)
					break
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			f(c)
		}
	}
	f(doc)
	return
}

// Returns the complete html of a given uri
func getHtmlString(uri string) (htmlString string) {
	res, err := http.Get(uri)
	checkError("Error in getHtmlString(), http.Get():", err, true)
	html, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	checkError("Error in getHtmlString(), ioutil.ReadAll()", err, true)
	htmlString = string(html[:])
	return
}

// Pings a site samples times and returns the minimum
func getLatency(req LatencyReq) (latency int) {
	var latencyList []int

	for i := 0; i < req.Samples; i++ {
		latency := pingSite(req.URL)
		latencyList = append(latencyList, latency)
	}

	sort.Ints(latencyList)
	fmt.Println("latencyList after sorting:", latencyList)

	latency = latencyList[0]
	return
}

// Pings a http.GET call and returns the time it takes in milliseconds
func pingSite(uri string) (latency int) {
	start := time.Now()
	_, err := http.Get(uri)
	elapsed := time.Since(start)

	checkError("Error in pingSiteOnce(), http.Get():", err, true)

	latency = int(elapsed / time.Millisecond)

	return latency
}

// Listens for RPC calls from the server
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

// Calls the server to join the system, gets ports to use
// as RPC server and to contact the server through RPC calls
func join() {

	conn, err := net.Dial("tcp", serverIpPort)
	checkError("Error in join(), net.Dial()", err, true)

	// TODO make more elegant than space delimiter...
	port, err := bufio.NewReader(conn).ReadString('\n')
	checkError("Error in join(), bufio.NewReader(conn).ReadString()", err, true)

	// need to split
	message := strings.Split(port, " ")

	// portForRPC = strings.Trim(port, " ")
	portForRPC = message[0]
	serverRpcPort = message[1]
}

// Parses the command line arguments of worker.go
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
