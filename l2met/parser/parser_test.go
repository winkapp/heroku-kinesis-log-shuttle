package parser

import (
    "testing"

    "github.com/winkapp/log-shuttle/l2met/bucket"
    "github.com/winkapp/log-shuttle/l2met/metchan"
    "github.com/op/go-logging"
    "os"
)

type testCase struct {
    tname   string
    in      []byte
    opts    options
    names   []string
    metrics []string
}

var parseTest = []testCase{
    {
        "simple",
        []byte(`88 <174>1 2013-07-22T00:06:26-00:00 somehost name test - measure#hello=1ms count#world=1 sample#foo=3\n`),
        options{"auth": []string{"abc123"}},
        []string{"hello", "world", "foo"},
        []string{},
    },
    {
        "simple_short",
        []byte(`88 <174>1 2013-07-22T00:06:26-00:00 somehost name test - m#hello=1ms c#world=2 s#foo=3\n`),
        options{"auth": []string{"abc123"}},
        []string{"hello", "world", "foo"},
        []string{},
    },
}

func TestBuildBuckets(t *testing.T) {
    for _, tc := range parseTest {
        mchan := new(metchan.Channel)
        mchan.Enabled = true
        mchan.Buffer = make(map[string]*bucket.Bucket)
        buckets := make([]*bucket.Bucket, 0)
        for b := range BuildBuckets(tc.in, tc.opts, mchan) {
            buckets = append(buckets, b)
        }
        if len(tc.names) > 0 {
            testNames(t, buckets, tc)
        }
        if len(tc.metrics) > 0 {
            testMetrics(t, mchan, tc)
        }
    }
}

func testMetrics(t *testing.T, mc *metchan.Channel, tc testCase) {
    for _, met := range tc.metrics {
        found := false
        for _, b := range mc.Buffer {
            if b.String() == met {
                found = true
            }
        }
        if !found {
            t.Fatalf("actual-metrics=%v expected-metrics=%v\n",
                mc.Buffer, tc.metrics)
        }
    }
}

func testNames(t *testing.T, b []*bucket.Bucket, tc testCase) {
    if len(b) != len(tc.names) {
        t.Fatalf("test=%s actual-len=%d expected-len=%d\n",
            tc.tname, len(b), len(tc.names))
    }
    for i := range tc.names {
        if b[i].Id.Name != tc.names[i] {
            t.Fatalf("test=%s actual-name=%s expected-name=%s\n",
                tc.tname, tc.names[i], b[i].Id.Name)
        }
    }
}

func TestMain(m *testing.M) {
    logging.SetLevel(logging.INFO, "")
	os.Exit(m.Run())
}
