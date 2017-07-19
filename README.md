Distributed Web Crawler System
=
Description
-
Collects the web graph of linking relationships between websites starting at certain root sites provided 
by a client. The system stores this graph in a distributed manner and responds to several kinds of queries about this graph.
Detailed assignment description: http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/assign5/index.html

**Summary** A server node serves RPC calls from a client node and coordinates multiple worker nodes that perform all
the storage and computations on the web graph.

Dependencies
-
Developed and tested with Go version 1.7.4 linux/amd64 on a Linux ubuntu 14.04 LTS machine

Running instructions:
-
- Open a terminal and clone the repo: git clone https://github.com/SeanBlair/WebCrawler.git
- Enter the folder: cd WebCrawler
- Run server: go run server.go localhost:1111 localhost:2222

Server arguments [ip:port that workers use to connect to the system] [ip:port that serves RPC calls from clients]
- Open a new terminal, navigate to the WebCrawler directory and run a Worker: go run worker.go localhost:1111

Worker arguments [ip:port that the server node is listening for workers on]

Note: the system is designed to have various worker nodes running on different machines, but it only supports one
worker running on the same machine. It was tested on MS Azure virtual machines set up in different geographical regions
worldwide. If have access to multiple machines to test the system, provide the server.go program with its machine's public 
IP as its first argument, and use that IP as the argument for any worker.go program on a different machine.

- Open a new terminal, navigate to the WebCrawler directory and run a Client with the following commands:

**GetWorkers:**

go run client.go -g [server ip:port]  

Returns the list of worker IPs connected to the server

*Example:* go run client.go -g localhost:2222



**Crawl:**

go run client.go -c [server ip:port] [url] [depth]

Instructs the system to crawl the given url (which must start with "http://" since it only supports the http scheme). The
worker who has the lowest latency to the given url is assigned ownership of this domain and its IP is returned after the 
system has crawled to the appropriate depth. If depth is greater than one, the system recursively crawls any http links found
on the given page to the appropriate depth and stores the resulting web graph with each worker owning domains that are closest
to it in terms of network latency.

*Example* go run client.go -c localhost:2222 http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/assign5/index.html 3



**Domains:**

go run client.go -d [server ip:port] [workerIP]

Returns the list of domains that the worker with given workerIP owns.

*Example* go run client.go -d localhost:2222 127.0.0.1


**Overlap:**

go run client.go -o [server ip:port] [url1] [url2]

Returns the number of pages in the overlap of the worker domain page graphs rooted at url1 and url2.

*Example* go run client.go -o localhost:2222 http://www.cs.ubc.ca/~bestchai/teaching/cs416_2016w2/assign5/index.html  http://infolab.stanford.edu/~backrub/google.html

