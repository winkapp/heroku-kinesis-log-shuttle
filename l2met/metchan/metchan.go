// An internal metrics channel.
// l2met internal components can publish their metrics
// here and they will be outletted.
package metchan

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/winkapp/log-shuttle/l2met/bucket"
	"github.com/winkapp/log-shuttle/l2met/metrics"
)

type Channel struct {
	// The time by which metchan will aggregate internal metrics.
	FlushInterval time.Duration
	// The Channel is thread-safe.
	sync.Mutex
	token      string
	verbose    bool
	quiet      bool
	Enabled    bool
	Buffer     map[string]*bucket.Bucket
	outbox     chan *bucket.Metric
	source     string
	appName    string
	numOutlets int
}

// Returns an initialized Metchan Channel.
// Creates a new HTTP client for direct access upstream.
// This channel is orthogonal with other outlet http clients in l2met.
// If a blank URL is given, no metric posting attempt will be made.
// If verbose is set to true, the metric will be printed to STDOUT
// regardless of whether the metric is sent upstream.
func New(verbose bool, quiet bool, token string, ccu int, buffsize int, appName string, hostName string) *Channel {
	c := new(Channel)

	// This will enable writting to a logger.
	c.verbose = verbose
	c.quiet   = quiet

	// If the url is nil, then it wasn't initialized
	// by the conf pkg. If it is not nil, we will
	// enable the Metchan.
	//if metchan_url != nil {
	//	c.url = metchan_url
	//	if c.url.User != nil {
	//		c.username = c.url.User.Username()
	//		c.password, _ = c.url.User.Password()
	//		c.url.User = nil
	//	}
	//	c.Enabled = true
	//	if c.verbose {
	//		log.Printf("Channel url:      %v\n", c.url)
	//		log.Printf("Channel Username: %v\n", c.username)
	//		log.Printf("Channel Password: %v\n", c.password)
	//		log.Printf("Channel Enabled:  %v\n", c.Enabled)
	//	}
	//}
    c.Enabled = true
    c.token = token
	c.numOutlets = ccu

	// Internal Datastructures.
	c.Buffer = make(map[string]*bucket.Bucket)
	c.outbox = make(chan *bucket.Metric, buffsize)

	// Default flush interval.
	c.FlushInterval = time.Second * 5

	c.source = hostName
	c.appName = appName
    if c.verbose {
        log.Printf("MetChan token:         %v\n", c.token)
	    log.Printf("MetChan source:        %v\n", c.source)
	    log.Printf("MetChan appName:       %v\n", c.appName)
	    log.Printf("MetChan numOutlets:    %v\n", c.numOutlets)
        log.Printf("MetChan FlushInterval: %v\n", c.FlushInterval)
    }
	return c
}

func (c *Channel) Start() {
	if c.Enabled {
		go c.scheduleFlush()
		for i := 0; i < c.numOutlets; i++ {
			go c.outlet()
		}
	}
}

// Provide the time at which you started your measurement.
// Places the measurement in a buffer to be aggregated and
// eventually flushed upstream.
func (c *Channel) Time(name string, t time.Time) {
	elapsed := time.Since(t) / time.Millisecond
	c.Measure(name, float64(elapsed))
}

func (c *Channel) Measure(name string, v float64) {
	if c.verbose {
		if v > 0 {
			log.Printf("source=%s measure#%s=%f\n", c.source, name, v)
		}
	}
	if !c.Enabled {
		return
	}
	id := &bucket.Id{
		Resolution: c.FlushInterval,
		Name:       c.appName + "." + name,
		Units:      "ms",
		Source:     c.source,
		Type:       "measurement",
	}
	b := c.getBucket(id)
	b.Append(v)
}

func (c *Channel) CountReq(user string) {
	if !c.Enabled {
		return
	}
	usr := strings.Replace(user, "@", "_at_", -1)
	id := &bucket.Id{
		Resolution: c.FlushInterval,
		Name:       c.appName + "." + "receiver.requests",
		Units:      "requests",
		Source:     usr,
		Type:       "counter",
	}
	b := c.getBucket(id)
	b.Incr(1)
}

func (c *Channel) getBucket(id *bucket.Id) *bucket.Bucket {
	c.Lock()
	defer c.Unlock()
	key := id.Name + ":" + id.Source
	b, ok := c.Buffer[key]
	if !ok {
		b = &bucket.Bucket{Id: id}
		b.Vals = make([]float64, 1, 10000)
		c.Buffer[key] = b
	}
	// Instead of creating a new bucket struct with a new Vals slice
	// We will re-use the old bucket and reset the slice. This
	// dramatically decreases the amount of arrays created and thus
	// led to better memory utilization.
	latest := time.Now().Truncate(c.FlushInterval)
	if b.Id.Time != latest {
		b.Id.Time = latest
		b.Reset()
	}
	return b
}

func (c *Channel) scheduleFlush() {
	for range time.Tick(c.FlushInterval) {
		c.flush()
	}
}

func (c *Channel) flush() {
	c.Lock()
	defer c.Unlock()
	for _, b := range c.Buffer {
		for _, m := range b.Metrics() {
			select {
			case c.outbox <- m:
			default:
				if !c.quiet {
					log.Println("error=metchan-drop")
				}
			}
		}
	}
}

func (c *Channel) outlet() {
	for met := range c.outbox {
		//if c.verbose {
		//	log.Println("-----------------------------------------------")
		//	log.Printf("Name:      %v\n", met.Name)
		//	log.Printf("Time:      %v\n", met.Time)
		//	if met.IsComplex {
		//		log.Printf("Count:     %v\n", *met.Count)
		//		log.Printf("Sum:       %v\n", *met.Sum)
		//		log.Printf("Max:       %v\n", *met.Max)
		//		log.Printf("Min:       %v\n", *met.Min)
		//	} else {
		//		log.Printf("Val:       %v\n", *met.Val)
		//	}
		//	log.Printf("Source:    %v\n", met.Source)
		//	log.Printf("Auth:      %v\n", met.Auth)
		//	log.Printf("Attr.Min   %v\n", met.Attr.Min)
		//	log.Printf("Attr.Units %v\n", met.Attr.Units)
		//	log.Printf("IsComplex: %v\n", met.IsComplex)
		//	log.Println("-----------------------------------------------")
		//}
		if err := c.post(met); err != nil {
			if !c.quiet {
				log.Printf("at=metchan-post error=%s\n", err)
			}
		}
	}
}

func (c *Channel) post(m *bucket.Metric) error {
	// FIXME: hardcoded to push to datadog, should be configurable?
	dd := metrics.DataDogConverter{Src: m}
	return dd.Post(c.token)
}

func (c *Channel) Token() string {
	return c.token
}
