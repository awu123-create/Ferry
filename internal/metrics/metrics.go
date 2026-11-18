package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry 封装通过 /metrics 暴露的独立 Prometheus 指标注册表。
type Registry struct {
	reg *prometheus.Registry
}

// NewRegistry 创建已注册 Go 运行时与进程采集器的 Prometheus 注册表。
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

// Prometheus 返回底层注册表，供后续 Milestone 注册业务指标。
func (r *Registry) Prometheus() *prometheus.Registry {
	return r.reg
}

// Handler 返回提供 Prometheus 指标导出格式的 HTTP Handler。
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{
		Registry: r.reg,
	})
}
