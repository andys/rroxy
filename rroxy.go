package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis" // for RedisBackend only
)

var Rev string
var config_path string
var logger *log.Logger
var DebugMode = true
var StabilityWait time.Duration = 5 * time.Second

type BackendTarget struct {
	TargetAddr      *net.TCPAddr
	Target          string
	Mutex           sync.Mutex
	Disabled        bool
	Master          bool
	Up              bool
	LastStateChange time.Time
	LastPing        time.Time
}

type Cluster struct {
	Endpoint       string
	BackendTargets []*BackendTarget
	Mutex          sync.Mutex
	Type           string
	Paused         bool
	CurrentMaster  *BackendTarget
	MasterSince    time.Time
}

type ClusterType interface {
	Pause()
	Resume()
	Listen()
	GetMaster()
	Manager()
	Drain()
	Close()
	Proxy()
	IsPaused()

	//Connect()
}

type BackendType interface {
	Ping()
	HasRecentLastPing()
}

type Connection struct {
	// net.Conn
	Cluster ClusterType
	// done   chan struct{}
}

func (b *BackendTarget) HasRecentLastPing() bool {
	return time.Since(b.LastPing) <= (90 * time.Second)
}

func (c *Cluster) Manager() {
	for _, b := range c.BackendTargets {
		debug("Launching BackendManager(%v) %p, %p", b.Target, c, b)
		go c.BackendManager(b)
	}

	for {
		// TODO: pass a channel to backend manager, make this listen for changes instead of sleep
		c.GetMaster()
		debug("GetMaster: %v @%v", c.CurrentMaster, c.MasterSince)

		time.Sleep(1 * time.Second)
	}
}

func (c *Cluster) BackendManager(b *BackendTarget) {
	debug("%p.BackendManager(%p)", c, b)
	// TODO: catch panics
	for {
		success, role := b.Ping()
		master := (role == "master")
		debug("Ping returned for %v: %v(%v), %v (%v)", b.Target, success, b.Up, role, master)

		c.Mutex.Lock()

		if b.Up != success || b.Master != master {
			b.Up = success
			b.Master = master
			b.LastStateChange = time.Now()
		}

		if success {
			b.LastPing = time.Now()
			debug("%p.BackendManager(%p) Setting b.LastPing = %v", c, b, b.LastPing)
		}

		c.Mutex.Unlock()
		
		time.Sleep(1 * time.Second)
	}
}

func (b *BackendTarget) Ping() (bool, string) {
	redisConn, err := redis.Dial("tcp", b.Target)
	if err != nil {
		debug("Ping(): cannot connect to %v (%v)", b.Target, err)
		return false, ""
	}
	defer redisConn.Close()

	reply, err := redis.Values(redisConn.Do("ROLE"))
	if err != nil {
		debug("Ping(): cannot execute ROLE on %v (%v)", b.Target, err)
		return false, ""

	}

	var role string
	if _, err := redis.Scan(reply, &role); err != nil {
		debug("Ping(): cannot scan reply to ROLE on  %v (%v)", b.Target, err)
		return false, ""
	}

	return true, role
}

func (c *Cluster) GetMaster() {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	var master *BackendTarget

	for _, b := range c.BackendTargets {
		debug("GetMaster: looking at %v(%v) %v", b.Target, b.HasRecentLastPing(), b.Master)
		if b.Master && b.HasRecentLastPing() {
			if master == nil {
				master = b
			} else {
				// TWO MASTERS! ABORT!
				debug("GetMaster: TWO MASTERS! setting master=none")
				c.CurrentMaster = nil
				return
			}
		}
	}

	if c.CurrentMaster != nil && !c.CurrentMaster.HasRecentLastPing() {
		c.CurrentMaster = nil
	}

	if c.CurrentMaster != master {
		c.CurrentMaster = master
		c.MasterSince = time.Now()
	}
}

func (b *BackendTarget) Connect() *net.TCPConn {
	// Connect to master.

	rConn, err := net.DialTCP("tcp", nil, b.TargetAddr)
	if err != nil {
		panic(err)
	}
	return rConn
}

func (c *Cluster) Pause() {
	c.Mutex.Lock()
	c.Paused = true
	c.Mutex.Unlock()
}

func (c *Cluster) Resume() {
	c.Mutex.Lock()
	c.Paused = false
	c.Mutex.Unlock()
}

func (c *Cluster) IsPaused() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	if c.Paused {
		debug("IsPaused: 1")
		return c.Paused
	}
	if c.CurrentMaster != nil && time.Since(c.MasterSince) <= StabilityWait {
		debug("IsPaused: 2  %p %v %v", c, c.MasterSince, StabilityWait)
		return true
	}
	return false
}

func (c *Cluster) Proxy() {
	addr, err := net.ResolveTCPAddr("tcp", c.Endpoint)
	if err != nil {
		panic(err)
	}

	// listen on all interfaces
	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}

	for {
		conn, err := ln.AcceptTCP()
		if err != nil {
			panic(err)
		}

		go c.ProxyOne(conn)
	}
}

func (c *Cluster) ProxyOne(conn net.Conn) {
	debug("ProxyOne starting new connection")
	for c.IsPaused() {
		debug("ProxyOne: paused(1)")
    time.Sleep(1 * time.Second)
  }
	c.Mutex.Lock()
	masterBackend := c.CurrentMaster
	c.Mutex.Unlock()

	if masterBackend == nil {
		debug("ProxyOne got no master! aborting")
	  return
	}

	debug("ProxyOne got master %v", masterBackend.Target)

	rConn := masterBackend.Connect()
	if rConn == nil {
		return
	}

	defer func() { // note: this doesnt cover the goroutine below!
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("error=%q", r)
			}
			debug("ProxyOne panic: %v", err.Error())
		}
		conn.Close()
		rConn.Close()
	}()

	clientReader := bufio.NewReaderSize(conn, 500)
	clientWriter := bufio.NewWriterSize(conn, 500)

	// Request loop - writing data from client to redis server
	go func() {
		defer func() { // note: this doesnt cover the goroutine below!
			if r := recover(); r != nil {
				err, ok := r.(error)
				if !ok {
					err = fmt.Errorf("error=%q", r)
				}
				debug("ProxyOne panic2: %v", err.Error())
			}
			conn.Close()
			rConn.Close()
		}()

		writer := bufio.NewWriterSize(rConn, 500)

		for {
			message, err := clientReader.ReadString('\n')

			log.Printf("sending: %v\n", message)
			if err != nil {
				panic(err)
			}

			// This is also where the 'return immediate error' short-circuit feature
			// would take place, where we write back to the client instead of sending to server

			for c.IsPaused() {
				time.Sleep(1 * time.Second)
			}

			if masterBackend != c.CurrentMaster {
				debug("master changed!!!!!!!!!!!! %v -> %v", masterBackend, c.CurrentMaster)
				panic("Master changed!")
			}

			writer.WriteString(message)
			writer.Flush()
		}
	}()

	// Response loop - writing data from redis server to client
	for {
		reader := bufio.NewReader(rConn)

		for {
			for c.IsPaused() {
				time.Sleep(1 * time.Second)
			}

			if masterBackend != c.CurrentMaster {
			  debug("master changed: %v -> %v", masterBackend, c.CurrentMaster)
				panic("Master changed!")
			}

			// TODO: while blocking for IO, also be able to change masters and reconnect if necessary
			message, err := reader.ReadString('\n')
			if err != nil {
				panic(err)
			}

			log.Printf("received: %v\n", message)
			if err != nil {
				panic(err)
			}

			clientWriter.WriteString(message)

			// Parse and copy "bulk strings"
			if message[0:1] == "$" {
				size, err := strconv.ParseInt(string(message[1:len(message)-2]), 10, 64)
				if err != nil {
					panic(err)
				}
				log.Printf("bulk reading %d\n", size)

				bulk := make([]byte, size)
				b2 := bulk
				var n int
				for len(b2) > 0 {
					n, err = reader.Read(b2)
					if err != nil {
						panic(err)
					}
					b2 = b2[n:]
				}

				if _, err = clientWriter.Write(bulk); err != nil {
					panic(err)
				}
			}

			clientWriter.Flush()
		}
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Lmicroseconds)
	logger = log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds)
	log.Println("Starting up! Revision: " + Rev + "-" + runtime.Version())

	rAddr, err := net.ResolveTCPAddr("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}
	rAddr2, err2 := net.ResolveTCPAddr("tcp", "localhost:6377")
	if err2 != nil {
		panic(err2)
	}

	cluster := &Cluster{Endpoint: ":6378", Type: "redis", BackendTargets: []*BackendTarget{&BackendTarget{Target: "localhost:6379", TargetAddr: rAddr}, &BackendTarget{Target: "localhost:6377", TargetAddr: rAddr2}}}

	go cluster.Manager()

	go func() {
		for {
			time.Sleep(6 * time.Second)
			log.Println("Pausing")
			cluster.Pause()

			time.Sleep(3 * time.Second)
			log.Println("Resuming")
			cluster.Resume()
		}
	}()

	cluster.Proxy()
}

// first attempt
func binaryproxyConnection(conn *net.TCPConn) {
	rAddr, err := net.ResolveTCPAddr("tcp", "localhost:6379")
	if err != nil {
		panic(err)
	}

	rConn, err := net.DialTCP("tcp", nil, rAddr)
	if err != nil {
		panic(err)
	}

	defer rConn.Close()

	// Request loop
	go func() {
		for {
			data := make([]byte, 1024*1024)
			n, err := conn.Read(data)
			if err != nil {
				panic(err)
			}
			rConn.Write(data[:n])
			log.Printf("sent:\n%v", hex.Dump(data[:n]))
		}
	}()

	// Response loop
	for {
		data := make([]byte, 1024*1024)
		n, err := rConn.Read(data)
		if err != nil {
			panic(err)
		}
		conn.Write(data[:n])
		log.Printf("received:\n%v", hex.Dump(data[:n]))
	}
}

func debug(msg string, args ...interface{}) {
	if DebugMode {
		log.Printf(msg+"\n", args...)
	}
}
