package shuttle

import (
    "bufio"
    "io"
    "sync"
    "time"
    "regexp"
    "github.com/rcrowley/go-metrics"
    "github.com/winkapp/log-shuttle/l2met/receiver"
    "github.com/op/go-logging"
)

var logger = logging.MustGetLogger("log-shuttle")

// LogLineReader performs the reading of lines from an io.ReadCloser, encapsulating
// lines into a LogLine and emitting them on outbox
type LogLineReader struct {
    input             io.ReadCloser // The input to read from
    out               chan<- Batch  // Where to send batches
    close             chan struct{}
    batchSize         int           // size of new batches
    timeOut           time.Duration // batch timeout
    timer             *time.Timer   // timer to actually enforce timeout
    drops             *Counter
    drop              bool // Should we drop or block

    linesRead         metrics.Counter
    linesBatchedCount metrics.Counter
    linesDroppedCount metrics.Counter
    batchFillTime     metrics.Timer

    mu                sync.Mutex // protects access to below
    b                 Batch

    receiver          *receiver.Receiver
    hostname          string
    tags              string
    appName           string
    listenProtocol    Protocol
    listenPort        int
    maxLineLength     int
}

// NewLogLineReader constructs a new reader with it's own Outbox.
func NewLogLineReader(input io.ReadCloser, s *Shuttle) *LogLineReader {
    t := time.NewTimer(time.Second)
    t.Stop() // we only need a timer running when we actually have log lines in the batch

    ll := LogLineReader{
        input:     input,
        out:       s.Batches,
        close:     make(chan struct{}),
        batchSize: s.config.BatchSize,
        timeOut:   s.config.WaitDuration,
        timer:     t,
        drops:     s.Drops,
        drop:      s.config.Drop,

        linesRead:         metrics.GetOrRegisterCounter("lines.read", s.MetricsRegistry),
        linesBatchedCount: metrics.GetOrRegisterCounter("lines.batched", s.MetricsRegistry),
        linesDroppedCount: metrics.GetOrRegisterCounter("lines.dropped", s.MetricsRegistry),
        batchFillTime:     metrics.GetOrRegisterTimer("batch.fill", s.MetricsRegistry),

        b: NewBatch(s.config.BatchSize),

        receiver: receiver.NewReceiver(s.config.L2met_BufferSize,
            s.config.L2met_FlushInterval,
            s.config.L2met_Concurrency,
            s.config.L2met_ReceiverDeadline,
            s.store,
            s.metchan),
        hostname: s.config.Hostname,
        tags: s.config.L2met_Tags,
        appName: s.config.Appname,
        listenProtocol: s.config.Protocol,
        listenPort: s.config.Port,
        maxLineLength: s.config.MaxLineLength,
    }
    ll.receiver.Start()
    go ll.expireBatches()

    return &ll
}

func (rdr *LogLineReader) expireBatches() {
    for {
        select {
        case <-rdr.close:
            return

        case <-rdr.timer.C:
            rdr.mu.Lock()
            rdr.deliverOrDropCurrent(rdr.timeOut)
            rdr.mu.Unlock()
        }
    }
}

//Close the reader for input
func (rdr *LogLineReader) Close() error {
    //close(rdr.close)
    return rdr.input.Close()
}

// ReadLines from the input created for. Return any errors
// blocks until the underlying reader is closed
func (rdr *LogLineReader) ReadLines() error {
    now := time.Now()

    rdrIo := bufio.NewReader(rdr.input)
    for {

        line, err := rdrIo.ReadBytes('\n')

        if len(line) > 0 {
            currentLogTime := time.Now()
            rdr.linesRead.Inc(1)
            rdr.mu.Lock()

            logger.Debugf("Read Line: %q", line)
            re, reerr := regexp.Compile(`(c(?:ount)?|s(?:ample)?|m(?:easure)?)#(\w[^=\s]*)=(\d+(?:\.\d+)?)([a-zA-Z]*)`)
            var matches [][][]byte
            if reerr != nil {
                logger.Errorf("RegExp Compile Error: %s", reerr.Error())
            } else {
                matches = re.FindAllSubmatch(line, -1)
            }

            if len(matches) > 0 {

                tags_re, tags_reerr := regexp.Compile(`tags=((?:[^:\s]+:[^,\s]+,?)+)`)
                var tags_matches [][][]byte
                if tags_reerr != nil {
                    logger.Errorf("RegExp Compile Error: %s", tags_reerr.Error())
                } else {
                    tags_matches = tags_re.FindAllSubmatch(line, -1)
                }

                var tags_opt = []string{}
                if len(rdr.tags) > 0 {
                    tags_opt = append(tags_opt, rdr.tags)
                }
                if len(rdr.appName) > 0 {
                    tags_opt = append(tags_opt, "app:" + rdr.appName)
                }
                for mm := range tags_matches  {
                    tags_opt = append(tags_opt, string(tags_matches[mm][1]))
                }

                for m := range matches  {
                    logger.Debugf("Match %d: %s", m, matches[m][0])
                    logger.Debugf("  Metric Type: %s", matches[m][1])
                    logger.Debugf("  Metric Name: %s", matches[m][2])
                    logger.Debugf("  Metric Value: %s", matches[m][3])
                    logger.Debugf("  Metric Units: %s", matches[m][4])
                    for mm := range tags_matches  {
                        logger.Debugf("  Metric Tags: %s", tags_matches[mm][1])
                    }
                }

                opts := map[string][]string{
                    // TODO: Is it a good idea to prepend the app name here?
                    //"prefix": []string{rdr.appName},
                    "source": []string{rdr.hostname},
                    "tags":   tags_opt,
                }
                logger.Debugf("opts: %+v", opts)
                rdr.receiver.Receive(line, opts)
            }

            if full := rdr.b.Add(LogLine{line, currentLogTime}); full {
                rdr.deliverOrDropCurrent(time.Since(now))
            }
            if rdr.b.MsgCount() == 1 { // First line so restart the timer
                now = time.Now()
                rdr.timer.Reset(rdr.timeOut)
            }
            rdr.mu.Unlock()
        }

        if err != nil {
            rdr.mu.Lock()
            rdr.deliverOrDropCurrent(time.Since(now))
            rdr.mu.Unlock()
            return err
        }
    }
}

// Should only be called when rdr.mu is held
func (rdr *LogLineReader) deliverOrDropCurrent(d time.Duration) {
    rdr.timer.Stop()
    // There is the possibility of a new batch being expired while this is happening.
    // so guard against queueing up an empty batch
    if c := rdr.b.MsgCount(); c > 0 {
        if rdr.drop {
            select {
            case rdr.out <- rdr.b:
                rdr.linesBatchedCount.Inc(int64(c))
            default:
                rdr.linesDroppedCount.Inc(int64(c))
                rdr.drops.Add(c)
            }
        } else {
            rdr.out <- rdr.b
            rdr.linesBatchedCount.Inc(int64(c))
        }

        rdr.batchFillTime.Update(d)
        rdr.b = NewBatch(rdr.batchSize)
    }
}
