package metrics

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	//"log"
	"io/ioutil"
	"net/http"

	"github.com/winkapp/log-shuttle/l2met/bucket"
)

var DataDogUrl = "https://app.datadoghq.com/api/v1/series"

type DataDogRequest struct {
	Series []*DataDog `json:"series"`
}

type point [2]float64

type DataDog struct {
	Metric string   `json:"metric"`
	Host   string   `json:"host,omitempty"`
	Tags   []string `json:"tags,omitempty"`
	Type   string   `json:"type"`
	Auth   string   `json:"-"`
	Points []point  `json:"points"`
}

// Create a datadog metric for a metric and the requested metric type
func DataDogComplexMetric(m *bucket.Metric, mtype string, tags []string) *DataDog {
	d := &DataDog{
		Type: "gauge",
		Auth: m.Auth,
		Host: m.Source,
		Tags: tags,
	}
	switch mtype {
	case "min":
		d.Metric = m.Name + ".min"
		d.Points = []point{{float64(m.Time), *m.Min}}
	case "max":
		d.Metric = m.Name + ".max"
		d.Points = []point{{float64(m.Time), *m.Max}}
	case "sum":
		// XXX: decided that sum would be the 'default' metric name; is this right?
		d.Metric = m.Name
		d.Points = []point{{float64(m.Time), *m.Sum}}
	case "count":
		// FIXME: "counts as counts"?
		d.Metric = m.Name + ".count"
		d.Points = []point{{float64(m.Time), float64(*m.Count)}}
	}
	return d
}

type DataDogConverter struct {
	Src     *bucket.Metric
	Tags    []string
}

// Convert a metric into one or more datadog metrics.  Metrics marked as
// complex actually map to 4 datadog metrics as there's no "complex" type
// in the datadog API.
func (d DataDogConverter) Convert() []*DataDog {
	var metrics []*DataDog
	var m = d.Src
	if m.IsComplex {
		metrics = make([]*DataDog, 0, 4)
		metrics = append(metrics, DataDogComplexMetric(m, "min", d.Tags))
		metrics = append(metrics, DataDogComplexMetric(m, "max", d.Tags))
		metrics = append(metrics, DataDogComplexMetric(m, "sum", d.Tags))
		metrics = append(metrics, DataDogComplexMetric(m, "count", d.Tags))
	} else {
		d := &DataDog{
			Metric: m.Name,
			Type:   "gauge",
			Auth:   m.Auth,
			Host:   m.Source,
			Points: []point{{float64(m.Time), *m.Val}},
			Tags:   d.Tags,
		}
		metrics = []*DataDog{d}
	}
	return metrics

}

func (d DataDogConverter) Post(url, api_key string) error {
	metrics := d.Convert()
	if len(metrics) == 0 {
		return errors.New("empty-metrics-error")
	}

	//for m := range metrics {
	//	log.Println("-----------------------------------------------")
	//	log.Printf("m.Metric:  %v\n", metrics[m].Metric)
	//	log.Printf("m.Host:    %v\n", metrics[m].Host)
	//	log.Printf("m.Tags:    %v\n", metrics[m].Tags)
	//	log.Printf("m.Type:    %v\n", metrics[m].Type)
	//	log.Printf("m.Auth:    %v\n", metrics[m].Auth)
	//	log.Printf("m.Points:  %v\n", metrics[m].Points)
	//	log.Println("-----------------------------------------------")
	//}


	ddReq := &DataDogRequest{metrics}

	body, err := json.Marshal(ddReq)

	if err != nil {
		return fmt.Errorf("at=json error=%s key=%s\n", err, api_key)
	}

	req, err := DataDogCreateRequest(url, api_key, body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return DataDogHandleResponse(resp, body)
}

func DataDogCreateRequest(url, api_key string, body []byte) (*http.Request, error) {
	b := bytes.NewBuffer(body)
	req, err := http.NewRequest("POST", url+"?api_key="+api_key, b)
	if err != nil {
		return req, err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "l2met/1.0")
	req.Header.Add("Connection", "Keep-Alive")
	return req, nil
}

func DataDogHandleResponse(resp *http.Response, reqBody []byte) error {
	if resp.StatusCode/100 != 2 {
		var m string
		s, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			m = fmt.Sprintf("error=failed-request code=%d", resp.StatusCode)
		} else {
			m = fmt.Sprintf("error=failed-request code=%d resp=body=%s req-body=%s",
				resp.StatusCode, s, reqBody)
		}
		return errors.New(m)
	}
	return nil
}
