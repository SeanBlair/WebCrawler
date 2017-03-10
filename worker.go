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
	// Contains urls (keys) of pages already visited in
	// DFS traversal of a network of pages.
	visitedForOverlap map[string]bool		// for computeOverlap call
	visitedForIsReachable map[string]bool	// for isReachable call
	// Number of overlaps found so far in call to ComputeOverlap
	numOverlapPages int
	// seconds to wait for http Get response
	httpGetTimeout int = 30
)

// A webpage
type Page struct {
	// 0 if links not crawled,
	// greater depending on how far crawled.
	DepthCrawled int
	// Urls that page links to
	Links []string
}

// A worker
type Worker struct {
	Ip string
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

// Request that server sends in RPC call to WorkerRPC.ComputeOverlap
type ComputeOverlapReq struct {
	FullOverlap bool
	OwnerURL1 Worker 	// Worker to perform overlap computation
	URL1 string 		// Site to start search of URL2's domain from
	OwnerURL2 Worker 	// Owner of domain of URL2
	URL2 string 		// Site to start search for entry point
}						// of URL2's domain accessed through URL1

// Request that worker sends in WorkerRPC.IsReachable RPC call
type IsReachableReq struct {
	StartURL string
	TargetURL string
	Domain string
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

// Returns the number of pages that overlap between 2 domains from given entry-points
func (p *WorkerRPC) ComputeOverlap(req ComputeOverlapReq, numPages *int) error {
	fmt.Println("Received call to ComputeOverlap() with req:", req)
	*numPages = computeOverlap(req)
	return nil
}

// Returns true if TargetURL is reachable from StartURL
func (p *WorkerRPC) IsReachable(req IsReachableReq, answer *bool) error {
	fmt.Println("received call to IsReachable() with req:", req)
	visitedForIsReachable = make(map[string]bool)
	*answer = isReachable(req.StartURL, req.TargetURL, req.Domain)
	return nil
}

// Using DFS starting at URL1, recursively looks for entry points to the domain of URL2
// If found, query worker owner of URL2 if the entry point page is accessible from URL2
// If accessible, increment numPages by one. Continue until all pages accessible
// from URL1 have been checked. Return total number of overlaps found.
func computeOverlap(req ComputeOverlapReq) (numPages int) {
	// set global variables
	visitedForOverlap = make(map[string]bool)
	numOverlapPages = 0
	thisDomain := getDomain(req.URL1)
	targetDomain := getDomain(req.URL2)
	ownerURL2Ip := req.OwnerURL2.Ip
	findEntryPoint(req.URL1, thisDomain, targetDomain, req.URL2, ownerURL2Ip)
	numPages = numOverlapPages
	if req.FullOverlap {
		numPages += computePartialOverlap(req)
	}
	return
}

// Call Worker owner of URL to get a partial overlap 
func computePartialOverlap(req ComputeOverlapReq) (numPages int) {
	request := ComputeOverlapReq{false, req.OwnerURL2, req.URL2, req.OwnerURL1, req.URL1}
	// This worker owns domain of URL2
	if req.OwnerURL1.Ip == req.OwnerURL2.Ip {
		numPages = computeOverlap(request)
	} else {
		client, err := rpc.Dial("tcp", req.OwnerURL2.Ip + ":" + portForRPC)
		checkError("rpc.Dial in computePartialOverlap()", err, true)
		err = client.Call("WorkerRPC.ComputeOverlap", request, &numPages)
		checkError("client.Call(WorkerRPC.ComputeOverlap in computePartialOverlap(): ", err, true)
		err = client.Close()
		checkError("client.Close() in computeOverlap(), computePartialOverlap(): ", err, true)
	}
	return
}

// Recursively traverses thisDomain starting at url1 in search of a link from targetDomain.
// If found, determines if link is reachable from url2, by asking ownerURL2.
// Updates the visited map and the numOverlapPages global variables.
func findEntryPoint(url1 string, thisDomain string, targetDomain string, url2 string, ownerUrl2Ip string) {
	visitedForOverlap[url1] = true
	thisPage := domains[thisDomain][url1]
	for _, link := range thisPage.Links {
		if isVisited(link, visitedForOverlap) {
			continue
		} else {
			linkDomain := getDomain(link)
			if linkDomain == targetDomain {
				if link == url2 {
					numOverlapPages++
				} else {
					if isLinkReachableFrom(url2, link, targetDomain, ownerUrl2Ip) {
					numOverlapPages++
					}
				}
			} else {
				if linkDomain == thisDomain {
					findEntryPoint(link, thisDomain, targetDomain, url2, ownerUrl2Ip)
				}
			}
		}
	}
	return
}

// Returns true if targetUrl is reachable from startUrl, either by RPC if not own domain or locally otherwise
func isLinkReachableFrom(startUrl string, targetUrl string, domain string, domainOwnerIp string) bool {
	if isMyDomain(domain) {
		visitedForIsReachable = make(map[string]bool)
		return isReachable(startUrl, targetUrl, domain)
	} else {
		return isReachableRPC(startUrl, targetUrl, domain, domainOwnerIp)
	}
}

func isReachableRPC(startUrl string, targetUrl string, domain string, domainOwnerIp string) (isReachable bool) {
	req := IsReachableReq{startUrl, targetUrl, domain}
	client, err := rpc.Dial("tcp", domainOwnerIp + ":" + portForRPC)
	checkError("rpc.Dial in isReachableRPC()", err, true)
	err = client.Call("WorkerRPC.IsReachable", req, &isReachable)
	checkError("client.Call(WorkerRPC.IsReachable) in isReachableRPC(): ", err, true)
	err = client.Close()
	checkError("client.Close() in isReachableRPC(): ", err, true)
	return
}

// Using recursive DFS, traverses network until either finding targetUrl
// and returning true or returning false if targetUrl not in network
func isReachable(startUrl string, targetUrl string, domain string) bool {
	if startUrl == targetUrl {
		return true
	} else {
		visitedForIsReachable[startUrl] = true
		page := domains[domain][startUrl]
		for _, link := range page.Links {
			if isVisited(link, visitedForIsReachable) {
				continue
			} else {
				if isReachable(link, targetUrl, domain) {
				return true
				}
			}
		}
		return false
	}
}

// Returns true if visited contains key url
func isVisited(url string, visited map[string]bool) bool {
	_, ok := visited[url]
	return ok
}

// Verifies that page is in domains map, then crawls the page
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
		domains[req.Domain][req.URL] = Page{0, nil}
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
		// links unknown
		if page.DepthCrawled == 0 {
			links, err := parseLinks(req.URL)
			// Error reading page, no use to crawl deeper
			if err != nil {
				domains[req.Domain][req.URL] = page	
				return
			}
			page.Links = links
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
			if isMyDomain(linkDomain) {
				initCrawl(CrawlPageReq{linkDomain, link, req.Depth - 1})
			} else {
				serverCrawl(link, req.Depth-1)
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
func parseLinks(uri string) (links []string, err error) {
	htmlString, err := getHtmlString(uri)
	if err != nil {
		return nil, err
	} else {
		urls := getAllLinks(htmlString)
		fmt.Println("Urls returned from getAllLinks() in parseLinks():", urls)
		allLinks := fixRelativeUrls(uri, urls)
		validLinks := filterHttpAndHtmlLinks(allLinks)
		links = eliminateDuplicates(validLinks)
		fmt.Println("Urls after fixed with fixRelativeUrls():", links)
		return	
	}
}

// Returns a list of urls without duplicates 
func eliminateDuplicates(urlList []string) (linksSet []string) {
	nonDuplicates := make(map[string]bool)
	for _, url := range urlList {
		nonDuplicates[url] = true
	}
	for key := range nonDuplicates {
		linksSet = append(linksSet, key)
	}
	return
}

// Returns only http urls that end in .html 
func filterHttpAndHtmlLinks(urlList []string) (filteredUrls []string) {
	for _, uri := range urlList {
		if isHttp(uri) && isDotHtml(uri) {
			filteredUrls = append(filteredUrls, uri)
		}
	}
	return
}

// Returns true if given uri ends in ".html"
func isDotHtml(uri string) bool {
	return strings.HasSuffix(uri, ".html")
}

// Returns true if given uri's scheme is http
func isHttp(uri string) bool {
	u, err := url.Parse(uri)
	checkError("Error in isHttp(), url.Parse():", err, true)
	return u.Scheme == "http"
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

// Returns the complete html of a given uri or err if failed Get or Read call
func getHtmlString(uri string) (htmlString string, err error) {
	timeout := time.Duration(time.Duration(httpGetTimeout) * time.Second)
	client := http.Client{
		Timeout: timeout,
	}
	res, err := client.Get(uri)
	checkError("Error in getHtmlString(), http.Get():", err, false)
	if err != nil {
		return "", err
	}
	html, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	checkError("Error in getHtmlString(), ioutil.ReadAll()", err, false)
	if err != nil {
		return "", err
	} else {
		htmlString = string(html[:])	
	}
	return
}

// Pings a site samples times and returns the minimum
func getLatency(req LatencyReq) (latency int) {
	var latencyList []int

	for i := 0; i < req.Samples; i++ {
		latency := pingSite(req.URL)
		latencyList = append(latencyList, latency)
		// detected a timeout or other error
		// consider url unresponsive, don't re-call
		if latency == -1 {
			break
		}
	}

	sort.Ints(latencyList)
	fmt.Println("latencyList after sorting:", latencyList)

	latency = latencyList[0]
	return
}

// Pings a http.GET call and returns the time it takes in milliseconds
// If get call returns an error of any type, including timeout 
// returns -1 and site is assumed unresponsive
func pingSite(uri string) (latency int) {
	timeout := time.Duration(time.Duration(httpGetTimeout) * time.Second)
	client := http.Client{
		Timeout: timeout,
	}
	start := time.Now()
	_, err := client.Get(uri)
	
	elapsed := time.Since(start)
	checkError("Error in pingSiteOnce(), http.Get():", err, false)
	if err != nil {
		latency = -1
	} else {
		latency = int(elapsed / time.Millisecond)		
	}
	return
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
