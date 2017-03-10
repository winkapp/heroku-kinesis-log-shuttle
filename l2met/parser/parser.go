// The parser is responsible for reading the body
// of the HTTP request and returning buckets of data.
package parser

import (
    "strconv"
    "strings"
    "time"
    "github.com/winkapp/log-shuttle/l2met/bucket"
    "github.com/winkapp/log-shuttle/l2met/metchan"
    "github.com/op/go-logging"
)

var logger = logging.MustGetLogger("log-shuttle")

type options map[string][]string

var (
    measurePrefix = "measure#"
    samplePrefix  = "sample#"
    counterPrefix = "count#"
)

type parser struct {
    out   chan *bucket.Bucket
    body  []byte
    ld    *logData
    opts  options
    mchan *metchan.Channel
}

func BuildBuckets(body []byte, opts options, m *metchan.Channel) <-chan *bucket.Bucket {
    p := new(parser)
    p.mchan = m
    p.opts = opts
    p.out = make(chan *bucket.Bucket)
    p.body = body
    p.ld = NewLogData()
    go p.parse()
    return p.out
}

func (p *parser) parse() {
    defer close(p.out)
    p.ld.Reset()
    if err := p.ld.Read(p.body); err != nil {
        logger.Errorf("error=%s", err)
    }
    for _, t := range p.ld.Tuples {
        p.handleCounters(t)
        p.handleSamples(t)
        p.handlMeasurements(t)
    }
}

func (p *parser) handleSamples(t *tuple) error {
    if !strings.HasPrefix(t.Name(), samplePrefix) {
        return nil
    }
    id := new(bucket.Id)
    p.buildId(id, t)
    id.Type = "sample"
    val, err := t.Float64()
    if err != nil {
        logger.Errorf("error=%v", err)
        return err
    }
    p.out <- &bucket.Bucket{Id: id, Vals: []float64{val}}
    return nil
}

func (p *parser) handleCounters(t *tuple) error {
    if !strings.HasPrefix(t.Name(), counterPrefix) {
        return nil
    }
    id := new(bucket.Id)
    p.buildId(id, t)
    id.Type = "counter"
    val, err := t.Float64()
    if err != nil {
        logger.Errorf("error=%v", err)
        return err
    }
    p.out <- &bucket.Bucket{Id: id, Vals: []float64{val}}
    return nil
}

func (p *parser) handlMeasurements(t *tuple) error {
    if !strings.HasPrefix(t.Name(), measurePrefix) {
        return nil
    }
    id := new(bucket.Id)
    p.buildId(id, t)
    id.Type = "measurement"
    val, err := t.Float64()
    if err != nil {
        logger.Errorf("error=%v", err)
        return err
    }
    p.out <- &bucket.Bucket{Id: id, Vals: []float64{val}}
    return nil
}

func (p *parser) buildId(id *bucket.Id, t *tuple) {
    id.Resolution = p.Resolution()
    id.Time = p.Time()
    id.Auth = p.Auth()
    id.ReadyAt = id.Time.Add(id.Resolution).Truncate(id.Resolution)
    id.Name = p.Prefix(t.Name())
    id.Units = t.Units()
    id.Source = p.SourcePrefix(p.Source())
    id.Tags = p.Tags()

    //log.Printf("Header.Name: %v\n", string(p.lr.Header().Name))
    //log.Printf("Header.Procid: %v\n", string(p.lr.Header().Procid))
    //log.Printf("Header.Msgid: %v\n", string(p.lr.Header().Msgid))
    return
}

func (p *parser) SourcePrefix(suffix string) string {
    pre, present := p.opts["source-prefix"]
    if !present {
        return suffix
    }
    if len(suffix) > 0 {
        return pre[0] + "." + suffix
    }
    return pre[0]
}

func (p *parser) Prefix(suffix string) string {
    //Remove measure. from the name if present.
    if strings.HasPrefix(suffix, measurePrefix) {
        suffix = suffix[len(measurePrefix):]
    }
    if strings.HasPrefix(suffix, counterPrefix) {
        suffix = suffix[len(counterPrefix):]
    }
    if strings.HasPrefix(suffix, samplePrefix) {
        suffix = suffix[len(samplePrefix):]
    }
    pre, present := p.opts["prefix"]
    if !present {
        return suffix
    }
    return pre[0] + "." + suffix
}

func (p *parser) Auth() string {
    pre, present := p.opts["auth"]
    if !present {
        return p.mchan.Token()
    }
    return pre[0]
}

func (p *parser) Tags() string {
    pre, present := p.opts["tags"]
    if !present {
        return ""
    }
    return strings.Join(pre, ",")
}

func (p *parser) Time() time.Time {
    d := p.Resolution()
    return time.Unix(0, int64((time.Duration(time.Now().UnixNano())/d)*d))
}

func (p *parser) Resolution() time.Duration {
    resTmp, present := p.opts["resolution"]
    if !present {
        resTmp = []string{"60"}
    }

    res, err := strconv.Atoi(resTmp[0])
    if err != nil {
        return time.Minute
    }

    return time.Second * time.Duration(res)
}

func (p *parser) Source() string {
    pre, present := p.opts["source"]
    if !present {
        return p.ld.Source()
    }
    return pre[0]
}
