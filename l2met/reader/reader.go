// The reader pkg is responsible for reading data from
// the store, building buckets from the data, and placing
// the buckets into a user-supplied channel.
package reader

import (
    "time"
    "github.com/winkapp/log-shuttle/l2met/bucket"
    "github.com/winkapp/log-shuttle/l2met/metchan"
    "github.com/winkapp/log-shuttle/l2met/store"
    "github.com/winkapp/log-shuttle"
    "github.com/op/go-logging"
)

var logger = logging.MustGetLogger("log-shuttle")

type Reader struct {
    str          store.Store
    scanInterval time.Duration
    numOutlets   int
    Inbox        chan *bucket.Bucket
    Outbox       chan *bucket.Bucket
    Mchan        *metchan.Channel
    verbose      bool
    quiet        bool
}

// Sets the scan interval to 1s.
func New(cfg shuttle.Config, st store.Store) *Reader {
    rdr := new(Reader)
    rdr.Inbox = make(chan *bucket.Bucket, cfg.L2met_BufferSize)
    rdr.numOutlets = cfg.L2met_Concurrency
    rdr.scanInterval = cfg.L2met_OutletInterval
    rdr.str = st
    rdr.verbose = cfg.Verbose
    rdr.quiet = cfg.Quiet
    return rdr
}

func (r *Reader) Start(out chan *bucket.Bucket) {
    r.Outbox = out
    go r.scan()
    for i := 0; i < r.numOutlets; i++ {
        go r.outlet()
    }
}

func (r *Reader) scan() {
    for range time.Tick(r.scanInterval) {
        startScan := time.Now()
        buckets, err := r.str.Scan(r.str.Now().Truncate(time.Second))
        if err != nil {
            logger.Errorf("at=bucket.scan error=%s", err)
            continue
        }
        i := 0
        for b := range buckets {
            r.Inbox <- b
            i++
        }
        r.Mchan.Time("reader.scan", startScan)
    }
}

func (r *Reader) outlet() {
    for b := range r.Inbox {
        startGet := time.Now()
        r.str.Get(b)
        r.Outbox <- b
        r.Mchan.Time("reader.get", startGet)
    }
}
