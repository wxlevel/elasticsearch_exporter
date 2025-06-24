package collector

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus-community/elasticsearch_exporter/pkg/clusterinfo"
)

type IndexDocs struct {
	Index string `json:"index"`
	Count string `json:"docs.count"`
}

type DocsCount struct {
	logger          *slog.Logger
	client          *http.Client
	url             *url.URL
	clusterInfoCh   chan *clusterinfo.Response
	lastClusterInfo *clusterinfo.Response
	includedIndices map[string]bool

	metrics             *prometheus.GaugeVec
	jsonParseFailures   prometheus.Counter
}

/*
接收指定的索引集合 includedIndices []string, 如未指定，则采集全部索引；
支持 Prometheus 注册与 clusterinfo 通信
按索引维度打文档数指标
加入 cluster 维度，兼容多集群部署
*/

func NewDocsCount(logger *slog.Logger, client *http.Client, url *url.URL, included []string) *DocsCount {
	includeMap := make(map[string]bool)
	for _, idx := range included {
		includeMap[idx] = true
	}

	d := &DocsCount{
		logger: logger,
		client: client,
		url:    url,
		includedIndices: includeMap,

		metrics: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prometheus.BuildFQName(namespace, "index", "docs_count"),
				Help: "Number of documents per index.",
			},
			[]string{"index", "cluster"},
		),
		jsonParseFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: prometheus.BuildFQName(namespace, "index", "json_parse_failures"),
			Help: "Number of JSON parse failures while collecting docs count.",
		}),
		clusterInfoCh: make(chan *clusterinfo.Response),
		lastClusterInfo: &clusterinfo.Response{
			ClusterName: "unknown_cluster",
		},
	}

	go func() {
		logger.Debug("starting cluster info receive loop")
		for ci := range d.clusterInfoCh {
			if ci != nil {
				logger.Debug("received cluster info update", "cluster", ci.ClusterName)
				d.lastClusterInfo = ci
			}
		}
		logger.Debug("exiting cluster info receive loop")
	}()

	return d
}

func (d *DocsCount) ClusterLabelUpdates() *chan *clusterinfo.Response {
	return &d.clusterInfoCh
}

func (d *DocsCount) String() string {
	return namespace + "_docs_count"
}

func (d *DocsCount) Describe(ch chan<- *prometheus.Desc) {
	d.metrics.Describe(ch)
	ch <- d.jsonParseFailures.Desc()
}

func (d *DocsCount) Collect(ch chan<- prometheus.Metric) {
	defer func() {
		ch <- d.jsonParseFailures
	}()

	u := *d.url
	u.Path = path.Join(u.Path, "/_cat/indices")
	q := u.Query()
	q.Set("format", "json")
	q.Set("h", "index,docs.count")
	u.RawQuery = q.Encode()

	resp, err := d.client.Get(u.String())
	if err != nil {
		d.logger.Warn("failed to fetch index stats", "err", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		d.logger.Warn("non-200 response", "status", resp.StatusCode)
		return
	}

	var data []IndexDocs
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		d.jsonParseFailures.Inc()
		d.logger.Warn("failed to parse JSON", "err", err)
		return
	}

	for _, idx := range data {
		if len(d.includedIndices) > 0 && !d.includedIndices[idx.Index] {
			continue
		}
		cnt, err := strconv.ParseFloat(idx.Count, 64)
		if err != nil {
			continue
		}
		d.metrics.WithLabelValues(idx.Index, d.lastClusterInfo.ClusterName).Set(cnt)
	}
	d.metrics.Collect(ch)
}
