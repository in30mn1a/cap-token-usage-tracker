package main

import (
	"net/url"
	"os"
	"testing"
	"time"
)

func TestPerfProbe(t *testing.T) {
	if os.Getenv("PERF_PROBE") == "" {
		t.Skip()
	}
	started := time.Now()
	runtime, now := seedDashboardBenchmark(t, 60, 100000)
	t.Logf("seed: %v", time.Since(started))
	end := now.Add(1500 * time.Millisecond)
	for _, span := range []time.Duration{5 * time.Hour, 30 * 24 * time.Hour, 60 * 24 * time.Hour} {
		query := url.Values{"range": {"custom"}, "start": {end.Add(-span).Format(time.RFC3339Nano)}, "end": {end.Format(time.RFC3339Nano)}}
		paged := url.Values{"offset": {"0"}, "limit": {"100"}}
		for key, value := range query {
			paged[key] = value
		}
		deep := url.Values{"offset": {"20000"}, "limit": {"100"}, "source": {"team-key"}}
		for key, value := range query {
			deep[key] = value
		}
		for _, endpoint := range []struct {
			name  string
			path  string
			query url.Values
		}{
			{"initial", runtime.routes.resourceStatsInitialPath, query},
			{"trends", runtime.routes.resourceStatsTrendPath, query},
			{"groups", runtime.routes.resourceStatsGroupsPath, paged},
			{"requests", runtime.routes.resourceRequestsPath, paged},
			{"req-deep", runtime.routes.resourceRequestsPath, deep},
			{"costs", runtime.routes.resourceCostsPath, query},
			{"costs2", runtime.routes.resourceCostsPath, query},
		} {
			started = time.Now()
			body := benchmarkCall(t, runtime, endpoint.path, endpoint.query).Body
			t.Logf("%v %-9s %8.1fms %8d bytes", span, endpoint.name, float64(time.Since(started).Microseconds())/1000, len(body))
		}
	}
}
