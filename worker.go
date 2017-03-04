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
	// domains []Domain
	//  domain : url : Page
	domains map[string]map[string]Page
)

// type Domain struct {
// 	Name string
// 	Pages []Page
// }

type Page struct {
	DepthCrawled int
	Links []string
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

	domains = new(map[string]map[string]Page)

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
	// crawlPage(req)
	initCrawl(req)
	*success = true
	return nil
}

func initCrawl(req CrawlPageReq) {
	// make sure domain exists
	_, ok := domains[req.Domain]
	if !ok {
		domains[req.Domain] = new(map[string]Page)
	}
	// make sure page exists
	_, ok = domains[req.Domain][req.Url]
	if !ok {
		domains[req.Domain][req.Url] = Page{-1, nil}
	}
	crawlPage(req)
}

// requires entry in domains[req.Domain][req.Url] : Page{x, y}
func crawlPage(req CrawlPageReq) {
	page := domains[req.Domain][req.Url]
	// need to crawl deeper 
	if req.Depth > page.DepthCrawled {
		// never crawled, so links unknown
		if page.Depth == -1 {
			page.Links = parseLinks(req.Url)
		}
		// previously crawled but to lesser depth
		page.DepthCrawled = req.Depth

		for _, link := range page.Links {
			linkDomain := getDomain(link)
			if !myDomain(linkDomain) {
				serverCrawl(link, req.Depth--)
			} else {
				initCrawl(CrawlPageReq{linkDomain, link, req.Depth--})
			}
		}
		// already crawled deep enough
	} else {
		return
	}
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
	links = fixRelativeUrls(urls)
}

// func crawlPage(req CrawlPageReq) {
// 	page := setNewDomain(req)
// 	fmt.Println("setNewDomain() in crawlPage() returned:", page)
// 	htmlString := getHtmlString(req.Url)
// 	setLinks(page, htmlString)
// 	return
// }

func getHtmlString(uri string) (htmlString string) {
	res, err := http.Get(uri)
	checkError("Error in getHtmlString(), http.Get():", err, true)
	html, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	checkError("Error in getHtmlString(), ioutil.ReadAll()", err, true)
	htmlString = string(html[:])
	return
}

func setLinks(page Page, htmlStr string) {
	fmt.Println("Setting links of page:", page)
	var links []Page 

	doc, err := html.Parse(strings.NewReader(htmlStr))
	checkError("Error in setLinks(), html.Parse():", err, true)
	
	// based on https://godoc.org/golang.org/x/net/html#example-Parse	
	var f func(*html.Node)
	f = func(n *html.Node) {
    	if n.Type == html.ElementNode && n.Data == "a" {
        	for _, a := range n.Attr {
            	if a.Key == "href" {
                	fmt.Println(a.Val)
                	links = append(links, Page{a.Val, nil})
                	break
            	}
        	}
    	}
    	for c := n.FirstChild; c != nil; c = c.NextSibling {
        	f(c)
    	}
	}
	f(doc)

	page.Links = links
	fmt.Println("Page after setting links:", page)
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
