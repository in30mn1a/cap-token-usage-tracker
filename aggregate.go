package main

import (
	"cmp"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

type Dimensions struct {
	Provider         string `json:"provider"`
	ExecutorType     string `json:"executor_type"`
	Model            string `json:"model"`
	Alias            string `json:"alias"`
	Source           string `json:"source"`
	APIKey           string `json:"api_key,omitempty"`
	APIKeyHash       string `json:"api_key_hash,omitempty"`
	APIKeyGeneration uint64 `json:"api_key_generation,omitempty"`
	APIKeyRef        string `json:"api_key_ref,omitempty"`
	APIKeyStatus     string `json:"api_key_status,omitempty"`
	AuthType         string `json:"auth_type"`
	ServiceTier      string `json:"service_tier"`
	ReasoningEffort  string `json:"reasoning_effort"`
	Failed           bool   `json:"failed"`
	FailureStatus    int    `json:"failure_status"`
}

// usageFilter scopes every analytics surface to the same persisted dimensions.
// Empty fields intentionally mean no restriction, which preserves legacy callers.
type usageFilter struct {
	Source           string
	APIKeyHash       string
	APIKeyGeneration uint64
	APIKeyRefSet     string
	Model            string
}

func newUsageFilter(source, apiKeyIdentity string) usageFilter {
	if apiKeyIdentity == "" {
		return usageFilter{Source: normalizeDimension(source)}
	}
	return newUsageFilterFromIdentities(source, []string{apiKeyIdentity})
}

func newUsageFilterFromIdentities(source string, identities []string) usageFilter {
	filter := usageFilter{Source: normalizeDimension(source)}
	seen := make(map[string]struct{})
	refs := make([]string, 0, len(identities))
	for _, identity := range identities {
		identity = normalizeDimension(identity)
		if identity == "" {
			continue
		}
		if generation, hash, ok := parseAPIKeyRef(identity); ok {
			ref := apiKeyRef(generation, hash)
			if _, exists := seen[ref]; exists {
				continue
			}
			seen[ref] = struct{}{}
			refs = append(refs, ref)
			continue
		}
		filter.APIKeyHash = identity
	}
	if len(refs) > 0 {
		sort.Strings(refs)
		filter.APIKeyRefSet = strings.Join(refs, "\n")
	}
	return filter
}

// matchesModelFilter accepts either the upstream model name or the alias
// clients used to reach it. The dashboard labels and groups its model views by
// alias, so the value it sends back as a model filter is the alias; matching
// only the upstream name makes every such selection return nothing.
func matchesModelFilter(model, alias, want string) bool {
	if want == "" {
		return true
	}
	return compactModelName(model) == want || strings.TrimSpace(alias) == want
}

func (f usageFilter) matches(dimensions Dimensions) bool {
	if f.Source != "" && dimensions.Source != f.Source {
		return false
	}
	if !matchesModelFilter(dimensions.Model, dimensions.Alias, f.Model) {
		return false
	}
	if f.APIKeyRefSet != "" {
		ref := apiKeyRef(dimensions.APIKeyGeneration, dimensions.APIKeyHash)
		if ref == "" {
			return false
		}
		for _, candidate := range strings.Split(f.APIKeyRefSet, "\n") {
			if candidate == ref {
				return true
			}
		}
		return false
	}
	return (f.APIKeyHash == "" || dimensions.APIKeyHash == f.APIKeyHash) &&
		(f.APIKeyGeneration == 0 || dimensions.APIKeyGeneration == f.APIKeyGeneration)
}

func (d *Dimensions) Redact() {
	d.APIKey = ""
	d.APIKeyHash = ""
	d.APIKeyGeneration = 0
	d.APIKeyRef = ""
	d.APIKeyStatus = ""
}

func (d *Dimensions) Reveal(decrypt DecryptFunc) {
	if d.APIKeyHash == "" || d.APIKeyGeneration == 0 {
		return
	}
	d.APIKeyRef = apiKeyRef(d.APIKeyGeneration, d.APIKeyHash)
	plaintext, status := decrypt(d.APIKey, d.APIKeyHash, d.APIKeyGeneration)
	d.APIKey = plaintext
	d.APIKeyStatus = status
}

type Counters struct {
	Requests            uint64 `json:"requests"`
	FailedRequests      uint64 `json:"failed_requests"`
	InputTokens         uint64 `json:"input_tokens"`
	OutputTokens        uint64 `json:"output_tokens"`
	ReasoningTokens     uint64 `json:"reasoning_tokens"`
	CachedTokens        uint64 `json:"cached_tokens"`
	CacheReadTokens     uint64 `json:"cache_read_tokens"`
	CacheCreationTokens uint64 `json:"cache_creation_tokens"`
	TotalTokens         uint64 `json:"total_tokens"`
	TotalLatencyNS      uint64 `json:"total_latency_ns"`
	TotalTTFTNS         uint64 `json:"total_ttft_ns"`
	LatencySamples      uint64 `json:"latency_samples"`
	TTFTSamples         uint64 `json:"ttft_samples"`
}

func (c *Counters) add(other Counters) {
	c.Requests = saturatingAdd(c.Requests, other.Requests)
	c.FailedRequests = saturatingAdd(c.FailedRequests, other.FailedRequests)
	c.InputTokens = saturatingAdd(c.InputTokens, other.InputTokens)
	c.OutputTokens = saturatingAdd(c.OutputTokens, other.OutputTokens)
	c.ReasoningTokens = saturatingAdd(c.ReasoningTokens, other.ReasoningTokens)
	c.CachedTokens = saturatingAdd(c.CachedTokens, other.CachedTokens)
	c.CacheReadTokens = saturatingAdd(c.CacheReadTokens, other.CacheReadTokens)
	c.CacheCreationTokens = saturatingAdd(c.CacheCreationTokens, other.CacheCreationTokens)
	c.TotalTokens = saturatingAdd(c.TotalTokens, other.TotalTokens)
	c.TotalLatencyNS = saturatingAdd(c.TotalLatencyNS, other.TotalLatencyNS)
	c.TotalTTFTNS = saturatingAdd(c.TotalTTFTNS, other.TotalTTFTNS)
	c.LatencySamples = saturatingAdd(c.LatencySamples, other.LatencySamples)
	c.TTFTSamples = saturatingAdd(c.TTFTSamples, other.TTFTSamples)
}

func (c Counters) averageLatencyNS() uint64 {
	if c.LatencySamples == 0 {
		return 0
	}
	return c.TotalLatencyNS / c.LatencySamples
}

func (c Counters) averageTTFTNS() uint64 {
	if c.TTFTSamples == 0 {
		return 0
	}
	return c.TotalTTFTNS / c.TTFTSamples
}

func countersForUsage(usage normalizedUsage) Counters {
	result := usage.Counters
	if usage.LatencyNS > 0 {
		result.TotalLatencyNS = usage.LatencyNS
		result.LatencySamples = 1
	}
	if usage.TTFTNS > 0 {
		result.TotalTTFTNS = usage.TTFTNS
		result.TTFTSamples = 1
	}
	return result
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

type aggregateKey struct {
	Hour       int64
	Dimensions Dimensions
}

type GroupStats struct {
	Dimensions
	Counters
	AverageLatencyNS uint64 `json:"average_latency_ns"`
	AverageTTFTNS    uint64 `json:"average_ttft_ns"`
}

// ModelStats is the compact per-model aggregate used by the first dashboard
// response. It intentionally omits the unused dimension fields in GroupStats.
type ModelStats struct {
	Model string `json:"model"`
	// Alias is the client-facing name requests used to reach Model. The
	// dashboard labels its aggregate views by alias, so the compact rollup has
	// to carry it even though it drops the other dimension fields.
	Alias string `json:"alias,omitempty"`
	Counters
	AverageLatencyNS uint64 `json:"average_latency_ns"`
	AverageTTFTNS    uint64 `json:"average_ttft_ns"`
}

type SeriesPoint struct {
	Hour string `json:"hour"`
	Counters
}

// ModelSeriesPoint preserves the time-bucketed model split needed by the dashboard for
// stacked trends, model drill-down, and cost calculations without retaining
// individual prompt contents.
type ModelSeriesPoint struct {
	Hour  string `json:"hour"`
	Model string `json:"model"`
	Counters
}

type StatsResponse struct {
	SchemaVersion uint32             `json:"schema_version"`
	GeneratedAt   time.Time          `json:"generated_at"`
	Range         string             `json:"range"`
	RetainedSince time.Time          `json:"retained_since"`
	LastUsed      time.Time          `json:"last_used"`
	Summary       Counters           `json:"summary"`
	Groups        []GroupStats       `json:"groups"`
	Series        []SeriesPoint      `json:"series"`
	ModelSeries   []ModelSeriesPoint `json:"model_series"`
	Sources       []string           `json:"sources"`
	APIKeys       []APIKeyOption     `json:"api_keys,omitempty"`
}

// InitialStatsResponse excludes dimension rows and per-model time points so the
// dashboard can render its first screen without parsing the largest payloads.
type InitialStatsResponse struct {
	SchemaVersion uint32         `json:"schema_version"`
	GeneratedAt   time.Time      `json:"generated_at"`
	Range         string         `json:"range"`
	RetainedSince time.Time      `json:"retained_since"`
	LastUsed      time.Time      `json:"last_used"`
	BucketSeconds uint64         `json:"bucket_seconds"`
	Summary       Counters       `json:"summary"`
	Models        []ModelStats   `json:"models"`
	Series        []SeriesPoint  `json:"series"`
	Sources       []string       `json:"sources"`
	APIKeys       []APIKeyOption `json:"api_keys,omitempty"`
}

type StatsTrendResponse struct {
	SchemaVersion uint32             `json:"schema_version"`
	GeneratedAt   time.Time          `json:"generated_at"`
	Range         string             `json:"range"`
	BucketSeconds uint64             `json:"bucket_seconds"`
	ModelSeries   []ModelSeriesPoint `json:"model_series"`
}

type GroupStatsPage struct {
	SchemaVersion uint32       `json:"schema_version"`
	GeneratedAt   time.Time    `json:"generated_at"`
	Range         string       `json:"range"`
	Items         []GroupStats `json:"items"`
	Total         int          `json:"total"`
}

type APIKeyOption struct {
	Ref        string `json:"ref"`
	Hash       string `json:"hash"`
	Generation uint64 `json:"generation"`
	Key        string `json:"key,omitempty"`
	Status     string `json:"status,omitempty"`
}

func (s *StatsResponse) Redact() {
	for i := range s.Groups {
		s.Groups[i].Dimensions.Redact()
	}
	s.APIKeys = nil
}

func (s *StatsResponse) Reveal(decrypt DecryptFunc) {
	for i := range s.Groups {
		s.Groups[i].Dimensions.Reveal(decrypt)
	}
	for i := range s.APIKeys {
		plaintext, status := decrypt(s.APIKeys[i].Key, s.APIKeys[i].Hash, s.APIKeys[i].Generation)
		s.APIKeys[i].Key = plaintext
		s.APIKeys[i].Status = status
	}
}

func (s *InitialStatsResponse) Redact() { s.APIKeys = nil }

func (s *InitialStatsResponse) Reveal(decrypt DecryptFunc) {
	for i := range s.APIKeys {
		plaintext, status := decrypt(s.APIKeys[i].Key, s.APIKeys[i].Hash, s.APIKeys[i].Generation)
		s.APIKeys[i].Key = plaintext
		s.APIKeys[i].Status = status
	}
}

func (p *GroupStatsPage) Redact() {
	for i := range p.Items {
		p.Items[i].Dimensions.Redact()
	}
}

func (p *GroupStatsPage) Reveal(decrypt DecryptFunc) {
	for i := range p.Items {
		p.Items[i].Dimensions.Reveal(decrypt)
	}
}

type usageRange struct {
	Name  string
	Start time.Time
	End   time.Time
}

func buildStats(data map[aggregateKey]Counters, since, lastUsed time.Time, requestedRange string, now time.Time) (StatsResponse, error) {
	queryRange, err := presetUsageRange(requestedRange, now)
	if err != nil {
		return StatsResponse{}, err
	}
	return buildStatsForRange(data, since, lastUsed, queryRange, "", now), nil
}

func buildStatsForRange(data map[aggregateKey]Counters, since, lastUsed time.Time, queryRange usageRange, source string, now time.Time) StatsResponse {
	return buildStatsForRangeWithFilter(data, since, lastUsed, queryRange, newUsageFilter(source, ""), now, nil)
}

func mapView(data map[aggregateKey]Counters, queryRange usageRange) aggregateView {
	return aggregateView{index: aggregateIndexFromMap(data), window: bucketWindow(queryRange)}
}

func buildStatsForRangeWithFilter(data map[aggregateKey]Counters, since, lastUsed time.Time, queryRange usageRange, filter usageFilter, now time.Time, apiKeyCiphertexts map[string]string) StatsResponse {
	return mapView(data, queryRange).stats(since, lastUsed, queryRange, filter, now, apiKeyCiphertexts)
}

func compactBucketSeconds(queryRange usageRange, since, now time.Time) uint64 {
	switch queryRange.Name {
	case "24h":
		return uint64((5 * time.Minute) / time.Second)
	case "7d":
		return uint64(time.Hour / time.Second)
	case "30d":
		return uint64((6 * time.Hour) / time.Second)
	}
	start := queryRange.Start
	if start.IsZero() || start.Before(since) {
		// The "All" preset asks for a range that reaches back long before any
		// data exists. Sizing the buckets off that span degrades every point to
		// a weekly bucket, which leaves the minute and hour trends with nothing
		// to draw. Only the retained window can actually hold data.
		start = since
	}
	if start.IsZero() || !start.Before(now) {
		return uint64(time.Hour / time.Second)
	}
	for _, width := range []uint64{300, 900, 3600, 21600, 86400, 604800} {
		if uint64(now.Sub(start)/time.Second)/width <= 360 {
			return width
		}
	}
	return 604800
}

func compactBucket(unix int64, bucketSeconds uint64) int64 {
	return unix / int64(bucketSeconds) * int64(bucketSeconds)
}

func compactModelName(model string) string {
	if model == "" {
		return "未标记模型"
	}
	return model
}

func sortedCompactSeries(series map[int64]Counters) []SeriesPoint {
	keys := make([]int64, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	points := make([]SeriesPoint, 0, len(keys))
	for _, key := range keys {
		points = append(points, SeriesPoint{Hour: time.Unix(key, 0).UTC().Format(time.RFC3339), Counters: series[key]})
	}
	return points
}

// compactModelAggregate accumulates one upstream model's counters together with
// the alias clients used to reach it. A model reached through two different
// aliases has no single correct label, so it keeps an empty alias and the
// dashboard falls back to the upstream name.
type compactModelAggregate struct {
	Counters
	alias     string
	ambiguous bool
}

func (a *compactModelAggregate) observeAlias(alias string) {
	alias = strings.TrimSpace(alias)
	if alias == "" || a.ambiguous {
		return
	}
	if a.alias == "" {
		a.alias = alias
		return
	}
	if a.alias != alias {
		a.alias = ""
		a.ambiguous = true
	}
}

func compactModelBucket(models map[string]*compactModelAggregate, model string) *compactModelAggregate {
	aggregate := models[model]
	if aggregate == nil {
		aggregate = &compactModelAggregate{}
		models[model] = aggregate
	}
	return aggregate
}

func sortedCompactModels(models map[string]*compactModelAggregate) []ModelStats {
	rows := make([]ModelStats, 0, len(models))
	for model, aggregate := range models {
		counters := aggregate.Counters
		rows = append(rows, ModelStats{Model: model, Alias: aggregate.alias, Counters: counters, AverageLatencyNS: counters.averageLatencyNS(), AverageTTFTNS: counters.averageTTFTNS()})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].TotalTokens != rows[j].TotalTokens {
			return rows[i].TotalTokens > rows[j].TotalTokens
		}
		return rows[i].Model < rows[j].Model
	})
	return rows
}

func sortedStatsAPIKeys(refs map[string]struct{}, ciphertexts map[string]string) []APIKeyOption {
	values := make([]APIKeyOption, 0, len(refs))
	for ref := range refs {
		generation, hash, _ := parseAPIKeyRef(ref)
		values = append(values, APIKeyOption{Ref: ref, Hash: hash, Generation: generation, Key: ciphertexts[ref]})
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Ref < values[j].Ref })
	return values
}

func buildInitialStatsForRange(data map[aggregateKey]Counters, since, lastUsed time.Time, queryRange usageRange, filter usageFilter, now time.Time, apiKeyCiphertexts map[string]string) InitialStatsResponse {
	return mapView(data, queryRange).initialStats(since, lastUsed, queryRange, filter, now, apiKeyCiphertexts)
}

func buildStatsTrendForRange(data map[aggregateKey]Counters, since time.Time, queryRange usageRange, filter usageFilter, now time.Time) StatsTrendResponse {
	return mapView(data, queryRange).trendStats(since, queryRange, filter, now)
}

func queryCutoff(value string, now time.Time) (string, time.Time, error) {
	queryRange, err := presetUsageRange(value, now)
	return queryRange.Name, queryRange.Start, err
}

func presetUsageRange(value string, now time.Time) (usageRange, error) {
	now = now.UTC()
	switch value {
	case "", "24h":
		return usageRange{Name: "24h", Start: now.Add(-24 * time.Hour).Truncate(time.Minute)}, nil
	case "7d":
		return usageRange{Name: "7d", Start: now.Add(-7 * 24 * time.Hour).Truncate(time.Minute)}, nil
	case "30d":
		return usageRange{Name: "30d", Start: now.Add(-30 * 24 * time.Hour).Truncate(time.Minute)}, nil
	case "retention":
		return usageRange{Name: "retention"}, nil
	default:
		return usageRange{}, withStatus(400, "unsupported range %q", value)
	}
}

func usageRangeFromQuery(rangeValue, startValue, endValue string, now time.Time) (usageRange, error) {
	if startValue == "" && endValue == "" {
		return presetUsageRange(rangeValue, now)
	}
	if rangeValue != "" && rangeValue != "custom" {
		return usageRange{}, withStatus(400, "range must be custom when start and end are provided")
	}
	if startValue == "" || endValue == "" {
		return usageRange{}, withStatus(400, "start and end must be provided together")
	}
	start, err := time.Parse(time.RFC3339, startValue)
	if err != nil {
		return usageRange{}, withStatus(400, "invalid start time: %v", err)
	}
	end, err := time.Parse(time.RFC3339, endValue)
	if err != nil {
		return usageRange{}, withStatus(400, "invalid end time: %v", err)
	}
	start = start.UTC()
	end = end.UTC()
	if !start.Before(end) {
		return usageRange{}, withStatus(400, "start must be before end")
	}
	return usageRange{Name: "custom", Start: start, End: end}, nil
}

func (r usageRange) validate() error {
	if r.Name == "" {
		return fmt.Errorf("range name is required")
	}
	if !r.End.IsZero() && (r.Start.IsZero() || !r.Start.Before(r.End)) {
		return fmt.Errorf("invalid time range")
	}
	return nil
}

func compareDimensions(left, right Dimensions) int {
	for _, comparison := range []int{
		cmp.Compare(left.Provider, right.Provider),
		cmp.Compare(left.ExecutorType, right.ExecutorType),
		cmp.Compare(left.Model, right.Model),
		cmp.Compare(left.Alias, right.Alias),
		cmp.Compare(left.Source, right.Source),
		cmp.Compare(left.APIKeyGeneration, right.APIKeyGeneration),
		cmp.Compare(left.APIKeyHash, right.APIKeyHash),
		cmp.Compare(left.AuthType, right.AuthType),
		cmp.Compare(left.ServiceTier, right.ServiceTier),
		cmp.Compare(left.ReasoningEffort, right.ReasoningEffort),
		cmp.Compare(boolInt(left.Failed), boolInt(right.Failed)),
		cmp.Compare(left.FailureStatus, right.FailureStatus),
	} {
		if comparison != 0 {
			return comparison
		}
	}
	return 0
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
