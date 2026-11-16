package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	bolt "go.etcd.io/bbolt"
)

// seedDashboardBenchmark fills a store with a deterministic workload shaped
// like a busy proxy: several models, sources and reasoning efforts spread over
// the retained window.
func seedDashboardBenchmark(tb testing.TB, days, requests int) (*pluginRuntime, time.Time) {
	tb.Helper()
	config := Config{
		DataPath:        tb.TempDir() + "/usage.db",
		RetentionDays:   365,
		FlushInterval:   time.Hour,
		FlushMaxRecords: 1 << 20,
	}
	store, err := openStore(config)
	if err != nil {
		tb.Fatal(err)
	}
	models := []string{"gpt-5.2", "gpt-5.2-codex", "claude-opus-4-5", "claude-sonnet-4-5", "gemini-3-pro", "gemini-3-flash", "glm-4.7", "kimi-k2"}
	providers := []string{"codex", "codex", "claude", "claude", "gemini", "gemini", "openai-compatible-zhipu", "kimi"}
	sources := []string{"alice@example.com", "bob@example.com", "https://api.example.com/v1", "team-key"}
	efforts := []string{"", "low", "medium", "high"}
	random := rand.New(rand.NewSource(1))
	now := time.Now().UTC()
	span := time.Duration(days) * 24 * time.Hour
	for index := 0; index < requests; index++ {
		model := random.Intn(len(models))
		input, output := uint64(1000+random.Intn(40000)), uint64(100+random.Intn(4000))
		cached := uint64(random.Intn(2)) * input / 2
		usage := normalizedUsage{
			Dimensions: Dimensions{
				Provider: providers[model], ExecutorType: providers[model], Model: models[model], Alias: models[model],
				Source: sources[random.Intn(len(sources))], AuthType: "oauth", ReasoningEffort: efforts[random.Intn(len(efforts))],
				Failed: random.Intn(50) == 0,
			},
			RequestedAt: now.Add(-time.Duration(random.Int63n(int64(span)))),
			LatencyNS:   uint64(2+random.Intn(30)) * uint64(time.Second),
			TTFTNS:      uint64(300+random.Intn(2000)) * uint64(time.Millisecond),
			Counters:    Counters{Requests: 1, InputTokens: input, OutputTokens: output, CachedTokens: cached, CacheReadTokens: cached, TotalTokens: input + output},
		}
		if err := store.Record(usage); err != nil {
			tb.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		tb.Fatal(err)
	}
	// A real database has been collecting since before its oldest request.
	// Reopening also exercises the startup load the dashboard depends on.
	db, err := bolt.Open(config.DataPath, 0o600, nil)
	if err != nil {
		tb.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(metaBucket).Put(sinceKey, encodeInt64(now.Add(-span).UnixNano()))
	}); err != nil {
		tb.Fatal(err)
	}
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
	if store, err = openStore(config); err != nil {
		tb.Fatal(err)
	}
	runtime := &pluginRuntime{store: store, config: config}
	tb.Cleanup(func() { _ = runtime.shutdown() })
	registration, err := json.Marshal(pluginapi.ManagementRegistrationRequest{ResourceBasePath: "/v0/resource/plugins/test"})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := runtime.registerManagement(registration); err != nil {
		tb.Fatal(err)
	}
	return runtime, now
}

func benchmarkCall(tb testing.TB, runtime *pluginRuntime, path string, query url.Values) pluginapi.ManagementResponse {
	tb.Helper()
	raw, err := json.Marshal(pluginapi.ManagementRequest{Method: http.MethodGet, Path: path, Query: query, Headers: http.Header{"Accept-Encoding": {"gzip"}}})
	if err != nil {
		tb.Fatal(err)
	}
	response, err := runtime.handleManagement(raw)
	if err != nil {
		tb.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		tb.Fatalf("%s = %d %s", path, response.StatusCode, response.Body)
	}
	return response
}

// BenchmarkDashboardLoad measures the requests one dashboard open issues, with
// the second-precision custom range the page actually sends.
func BenchmarkDashboardLoad(b *testing.B) {
	runtime, now := seedDashboardBenchmark(b, 60, 100000)
	for _, window := range []struct {
		name string
		span time.Duration
	}{{"5h", 5 * time.Hour}, {"30d", 30 * 24 * time.Hour}, {"all", 60 * 24 * time.Hour}} {
		end := now.Add(1500 * time.Millisecond)
		query := url.Values{"range": {"custom"}, "start": {end.Add(-window.span).Format(time.RFC3339Nano)}, "end": {end.Format(time.RFC3339Nano)}}
		paged := url.Values{"offset": {"0"}, "limit": {"100"}}
		for key, value := range query {
			paged[key] = value
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
			{"costs", runtime.routes.resourceCostsPath, query},
		} {
			b.Run(fmt.Sprintf("%s/%s", window.name, endpoint.name), func(b *testing.B) {
				var bytes int
				for b.Loop() {
					bytes = len(benchmarkCall(b, runtime, endpoint.path, endpoint.query).Body)
				}
				b.ReportMetric(float64(bytes), "resp-bytes")
			})
		}
	}
}
