package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// dualCounter increments both legacy synapse_sidecar_* and sugar_glider_* series.
// Desc/Write/Describe/Collect exist only so the value satisfies prometheus.Counter
// (used by Snapshot via Write); the dual wrapper itself is never registered.
type dualCounter struct {
	legacy prometheus.Counter
	modern prometheus.Counter
}

func (d dualCounter) Desc() *prometheus.Desc                         { return d.legacy.Desc() }
func (d dualCounter) Write(out *dto.Metric) error                    { return d.legacy.Write(out) }
func (d dualCounter) Describe(ch chan<- *prometheus.Desc)            { d.legacy.Describe(ch) }
func (d dualCounter) Collect(ch chan<- prometheus.Metric)            { d.legacy.Collect(ch) }
func (d dualCounter) Inc()                                           { d.legacy.Inc(); d.modern.Inc() }
func (d dualCounter) Add(v float64)                                  { d.legacy.Add(v); d.modern.Add(v) }

// dualGauge updates both legacy and modern gauges. Same interface note as dualCounter.
type dualGauge struct {
	legacy prometheus.Gauge
	modern prometheus.Gauge
}

func (d dualGauge) Desc() *prometheus.Desc              { return d.legacy.Desc() }
func (d dualGauge) Write(out *dto.Metric) error         { return d.legacy.Write(out) }
func (d dualGauge) Describe(ch chan<- *prometheus.Desc) { d.legacy.Describe(ch) }
func (d dualGauge) Collect(ch chan<- prometheus.Metric) { d.legacy.Collect(ch) }
func (d dualGauge) Set(v float64)                       { d.legacy.Set(v); d.modern.Set(v) }
func (d dualGauge) Inc()                                { d.legacy.Inc(); d.modern.Inc() }
func (d dualGauge) Dec()                                { d.legacy.Dec(); d.modern.Dec() }
func (d dualGauge) Add(v float64)                       { d.legacy.Add(v); d.modern.Add(v) }
func (d dualGauge) Sub(v float64)                       { d.legacy.Sub(v); d.modern.Sub(v) }
func (d dualGauge) SetToCurrentTime() {
	d.legacy.SetToCurrentTime()
	d.modern.SetToCurrentTime()
}
