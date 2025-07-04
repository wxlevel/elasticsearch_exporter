// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// IndicesSettings information struct
type IndicesSettings struct {
	logger *slog.Logger
	client *http.Client
	url    *url.URL

	readOnlyIndices prometheus.Gauge

	metrics []*indicesSettingsMetric
}

var (
	defaultIndicesTotalFieldsLabels = []string{"index"}
	defaultTotalFieldsValue         = 1000 // es default configuration for total fields
	defaultDateCreation             = 0    // es index default creation date
)

type indicesSettingsMetric struct {
	Type  prometheus.ValueType
	Desc  *prometheus.Desc
	Value func(indexSettings Settings) float64
}

// NewIndicesSettings defines Indices Settings Prometheus metrics
func NewIndicesSettings(logger *slog.Logger, client *http.Client, url *url.URL) *IndicesSettings {
	return &IndicesSettings{
		logger: logger,
		client: client,
		url:    url,

		readOnlyIndices: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(namespace, "indices_settings_stats", "read_only_indices"),
			Help: "Current number of read only indices within cluster",
		}),

		metrics: []*indicesSettingsMetric{
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "indices_settings", "total_fields"),
					"index mapping setting for total_fields",
					defaultIndicesTotalFieldsLabels, nil,
				),
				Value: func(indexSettings Settings) float64 {
					val, err := strconv.ParseFloat(indexSettings.IndexInfo.Mapping.TotalFields.Limit, 64)
					if err != nil {
						return float64(defaultTotalFieldsValue)
					}
					return val
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "indices_settings", "replicas"),
					"index setting number_of_replicas",
					defaultIndicesTotalFieldsLabels, nil,
				),
				Value: func(indexSettings Settings) float64 {
					val, err := strconv.ParseFloat(indexSettings.IndexInfo.NumberOfReplicas, 64)
					if err != nil {
						return float64(defaultTotalFieldsValue)
					}
					return val
				},
			},
			{
				Type: prometheus.GaugeValue,
				Desc: prometheus.NewDesc(
					prometheus.BuildFQName(namespace, "indices_settings", "creation_timestamp_seconds"),
					"index setting creation_date",
					defaultIndicesTotalFieldsLabels, nil,
				),
				Value: func(indexSettings Settings) float64 {
					val, err := strconv.ParseFloat(indexSettings.IndexInfo.CreationDate, 64)
					if err != nil {
						return float64(defaultDateCreation)
					}
					return val / 1000.0
				},
			},
		},
	}
}

// Describe add Snapshots metrics descriptions
func (cs *IndicesSettings) Describe(ch chan<- *prometheus.Desc) {
	ch <- cs.readOnlyIndices.Desc()

	for _, metric := range cs.metrics {
		ch <- metric.Desc
	}
}

func (cs *IndicesSettings) getAndParseURL(u *url.URL, data interface{}) error {
	res, err := cs.client.Get(u.String())
	if err != nil {
		return fmt.Errorf("failed to get from %s://%s:%s%s: %s",
			u.Scheme, u.Hostname(), u.Port(), u.Path, err)
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			cs.logger.Warn(
				"failed to close http.Client",
				"err", err,
			)
		}
	}()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP Request failed with code %d", res.StatusCode)
	}

	bts, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(bts, data); err != nil {
		return err
	}
	return nil
}

func (cs *IndicesSettings) fetchAndDecodeIndicesSettings() (IndicesSettingsResponse, error) {
	u := *cs.url
	u.Path = path.Join(u.Path, "/_all/_settings")
	var asr IndicesSettingsResponse
	err := cs.getAndParseURL(&u, &asr)
	if err != nil {
		return asr, err
	}

	return asr, err
}

// Collect gets all indices settings metric values
func (cs *IndicesSettings) Collect(ch chan<- prometheus.Metric) {
	asr, err := cs.fetchAndDecodeIndicesSettings()
	if err != nil {
		cs.readOnlyIndices.Set(0)
		cs.logger.Warn(
			"failed to fetch and decode cluster settings stats",
			"err", err,
		)
		return
	}

	var c int
	for indexName, value := range asr {
		if value.Settings.IndexInfo.Blocks.ReadOnly == "true" {
			c++
		}
		for _, metric := range cs.metrics {
			ch <- prometheus.MustNewConstMetric(
				metric.Desc,
				metric.Type,
				metric.Value(value.Settings),
				indexName,
			)
		}
	}
	cs.readOnlyIndices.Set(float64(c))

	ch <- cs.readOnlyIndices
}
