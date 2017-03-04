package main

import (
	"bufio"
	// "crypto/md5"
	"fmt"
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
	"golang.org/x/net/html"
)

var (
	portForRPC string
	serverIpPort     string
	serverRpcIpPort string
	//  domain : url : Page
	// TODO make sure thread safe as is map
	domains map[string]map[string]Page
)

type Page struct {
	DepthCrawled int
	Links []string
}

type MServer int

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

type CrawlReq struct {
	URL string
	Depth int
}


func main() {

	err := ParseArguments()
	if err != nil {
		panic(err)
	}

	domains = make(map[string]map[string]Page)

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
	fmt.Println("received call to CrawlPage() with req:", req)
	go initCrawl(req)
	*success = true
	return nil
}

func initCrawl(req CrawlPageReq) {
	fmt.Println("domains before initCrawl() processes:", domains)

	// make sure domain exists
	_, ok := domains[req.Domain]
	// TODO mutex??
	if !ok {
		domains[req.Domain] = make(map[string]Page)
	}
	// make sure page exists
	_, ok = domains[req.Domain][req.URL]
	// TODO mutex??
	if !ok {
		domains[req.Domain][req.URL] = Page{-1, nil}
	}

	fmt.Println("domains after initCrawl() processes:", domains)
	crawlPage(req)
}

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
				// TODO implement
				// serverCrawl(link, req.Depth - 1)
				fmt.Println("TODO, need to call serverCrawl for url:", link)
			} else {
				initCrawl(CrawlPageReq{linkDomain, link, req.Depth - 1})
			}
		}
		// already crawled deep enough
	} else {
		return
	}
}

func serverCrawl(url string, depth int) {
	// need to call serverRpcIpPort
	fmt.Println("Calling CrawlServer.Crawl using serverRpcIpPort:", serverRpcIpPort)

	req := CrawlReq{url, depth}
	var resp bool
	client, err := rpc.Dial("tcp", serverRpcIpPort)
	checkError("rpc.Dial in serverCrawl()", err, true)
	err = client.Call("MServer.Crawl", req, &resp)
	checkError("client.Call(MServer.Crawl: ", err, true)
	err = client.Close()
	checkError("client.Close() in serverCrawl(): ", err, true)
	return
}

func isMyDomain(domain string) bool {
	_, ok := domains[domain]
	return ok
}

func getDomain(uri string) (domain string) {
	u, err := url.Parse(uri)
    checkError("Error in getDomain(), url.Parse():", err, true)
	domain = u.Host
	return 
}

func parseLinks(uri string) (links []string) {
	htmlString := getHtmlString(uri)
	urls := getAllLinks(htmlString)
	fmt.Println("Urls returned from getAllLinks() in parseLinks():", urls)
	// TODO 
	// links = fixRelativeUrls(urls)
	links = urls
	return
}

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

func getHtmlString(uri string) (htmlString string) {
	res, err := http.Get(uri)
	checkError("Error in getHtmlString(), http.Get():", err, true)
	html, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	checkError("Error in getHtmlString(), ioutil.ReadAll()", err, true)
	htmlString = string(html[:])
	return
}


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

	// TODO make more elegant than space delimiter...
	port, err := bufio.NewReader(conn).ReadString('\n')
	checkError("Error in join(), bufio.NewReader(conn).ReadString()", err, true)

	// need to split
	message := strings.Split(port, " ")

	// portForRPC = strings.Trim(port, " ")
	portForRPC = message[0]
	serverRpcIpPort = message[1]
	fmt.Println("Successful in joining server")
	fmt.Println("My portForWorkerRPC is:", portForRPC, "and my serverRpcIpPort is:", serverRpcIpPort)
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
