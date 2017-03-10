// An internal metrics channel.
// l2met internal components can publish their metrics
// here and they will be outletted.
package metchan

import (
    "strings"
    "sync"
    "time"

    "github.com/winkapp/log-shuttle/l2met/bucket"
    "github.com/winkapp/log-shuttle/l2met/metrics"
    "github.com/op/go-logging"
)

var logger = logging.MustGetLogger("log-shuttle")

type Channel struct {
    // The time by which metchan will aggregate internal metrics.
    FlushInterval time.Duration
    // The Channel is thread-safe.
    sync.Mutex
    token      string
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
func New(token string, ccu int, buffsize int, appName string, hostName string) *Channel {
    c := new(Channel)

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

    logger.Debugf("MetChan token:         %s", logging.Redact(c.token))
    logger.Debugf("MetChan source:        %s", c.source)
    logger.Debugf("MetChan appName:       %s", c.appName)
    logger.Debugf("MetChan numOutlets:    %d", c.numOutlets)
    logger.Debugf("MetChan FlushInterval: %v", c.FlushInterval)

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
    if v > 0 {
        logger.Debugf("source=%s measure#%s=%f", c.source, name, v)
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
                logger.Error("error=metchan-drop")
            }
        }
    }
}

func (c *Channel) outlet() {
    for met := range c.outbox {
        var ignore = strings.Split(met.Name, ".")[1]
        if ignore == "datadog-outlet" || ignore == "reader" || ignore == "receiver" {

        } else {
            logger.Debug("-----------------------------------------------")
            logger.Debugf("Name:       %v", met.Name)
            logger.Debugf("Time:       %v", met.Time)
            if met.IsComplex {
                logger.Debugf("Count:      %v", *met.Count)
                logger.Debugf("Sum:        %v", *met.Sum)
                logger.Debugf("Max:        %v", *met.Max)
                logger.Debugf("Min:        %v", *met.Min)
            } else {
                logger.Debugf("Val:        %v", *met.Val)
            }
            logger.Debugf("Source:     %v", met.Source)
            logger.Debugf("Auth:       %v", logging.Redact(met.Auth))
            logger.Debugf("Attr.Min:   %v", met.Attr.Min)
            logger.Debugf("Attr.Units: %v", met.Attr.Units)
            logger.Debugf("IsComplex:  %v", met.IsComplex)
            logger.Debug("-----------------------------------------------")
        }
        if err := c.post(met); err != nil {
            logger.Errorf("at=metchan-post error=%s", err)
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
