package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry owns a dedicated Prometheus registry exposed over /metrics.
type Registry struct {
	reg *prometheus.Registry
}

// NewRegistry creates a Prometheus registry with Go runtime and process collectors.
func NewRegistry() (*Registry, error) {
	reg := prometheus.NewRegistry()
	if err := reg.Register(collectors.NewGoCollector()); err != nil {
		return nil, fmt.Errorf("register go collector: %w", err)
	}
	if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, fmt.Errorf("register process collector: %w", err)
	}
	return &Registry{reg: reg}, nil
}

// Prometheus returns the underlying registry so future milestones can register domain metrics.
func (r *Registry) Prometheus() *prometheus.Registry {
	return r.reg
}

// Handler returns an HTTP handler serving Prometheus exposition format.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{
		Registry: r.reg,
	})
}
