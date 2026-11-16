package main

import (
	"encoding/json"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// The dashboard answers second-precision ranges from minute aggregates, a
// per-minute request count and a per-minute cost memo. These tests hold every
// one of those shortcuts to a plain scan of the request log.

func equivalenceUsage(random *rand.Rand, at time.Time) normalizedUsage {
	models := []string{"alpha", "beta", "gamma", ""}
	aliases := []string{"", "", "front"}
	sources := []string{"cli", "web", "https://api.example.com/v1?key=x", "Codex-user@example.com"}
	input := uint64(random.Intn(400))
	cached := uint64(random.Intn(2)) * input / 2
	usage := normalizedUsage{
		Dimensions: Dimensions{
			Provider: []string{"openai", "anthropic"}[random.Intn(2)], Model: models[random.Intn(len(models))], Alias: aliases[random.Intn(len(aliases))],
			Source: sources[random.Intn(len(sources))], ServiceTier: []string{"", "priority"}[random.Intn(2)], Failed: random.Intn(6) == 0,
		},
		RequestedAt: at,
		Counters:    Counters{Requests: 1, InputTokens: input, OutputTokens: uint64(random.Intn(90)), CacheReadTokens: cached, TotalTokens: input + 7},
	}
	if random.Intn(3) > 0 {
		usage.LatencyNS = uint64(1 + random.Intn(9e9))
		usage.TTFTNS = uint64(1 + random.Intn(9e8))
	}
	return usage
}

func equivalenceRows(t *testing.T, store *Store) []RequestDetail {
	t.Helper()
	// A request query writes any buffered usage before reading.
	if _, err := store.queryRequestPage(usageRange{Name: "retention"}, 0, 1, ""); err != nil {
		t.Fatal(err)
	}
	var rows []RequestDetail
	err := store.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(requestsBucket).ForEach(func(_, value []byte) error {
			var row RequestDetail
			if err := json.Unmarshal(value, &row); err != nil {
				return err
			}
			rows = append(rows, row)
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func inRange(row RequestDetail, queryRange usageRange) bool {
	return (queryRange.Start.IsZero() || !row.Time.Before(queryRange.Start)) && (queryRange.End.IsZero() || row.Time.Before(queryRange.End))
}

// referenceAggregates rebuilds minute aggregates for a range from request rows.
func referenceAggregates(rows []RequestDetail, queryRange usageRange) map[aggregateKey]Counters {
	data := make(map[aggregateKey]Counters)
	for _, row := range rows {
		if !inRange(row, queryRange) {
			continue
		}
		counters := row.Counters
		if row.LatencyNS > 0 {
			counters.TotalLatencyNS, counters.LatencySamples = row.LatencyNS, 1
		}
		if row.TTFTNS > 0 {
			counters.TotalTTFTNS, counters.TTFTSamples = row.TTFTNS, 1
		}
		key := aggregateKey{Hour: row.Time.Truncate(time.Minute).Unix(), Dimensions: row.Dimensions}
		combined := data[key]
		combined.add(counters)
		data[key] = combined
	}
	return data
}

func equivalenceRanges(random *rand.Rand, base time.Time, span time.Duration) []usageRange {
	ranges := []usageRange{
		{Name: "retention"},
		{Name: "24h", Start: base.Add(span / 3).Truncate(time.Minute)},
		{Name: "custom", Start: base.Truncate(time.Minute), End: base.Add(span).Truncate(time.Minute)},
		// Both bounds inside one minute.
		{Name: "custom", Start: base.Add(span / 2).Truncate(time.Minute).Add(7 * time.Second), End: base.Add(span / 2).Truncate(time.Minute).Add(41 * time.Second)},
		// Bounds in adjacent minutes, so there is no whole minute between them.
		{Name: "custom", Start: base.Add(span / 2).Truncate(time.Minute).Add(50 * time.Second), End: base.Add(span / 2).Truncate(time.Minute).Add(70 * time.Second)},
		{Name: "custom", Start: base.Add(-time.Hour), End: base.Add(span + time.Hour + 1500*time.Millisecond)},
	}
	for range 14 {
		start := base.Add(time.Duration(random.Int63n(int64(span))))
		end := start.Add(time.Duration(1 + random.Int63n(int64(span))))
		if random.Intn(3) == 0 {
			start = start.Truncate(time.Minute)
		}
		if random.Intn(3) == 0 {
			end = end.Truncate(time.Minute).Add(time.Minute)
		}
		ranges = append(ranges, usageRange{Name: "custom", Start: start, End: end})
	}
	return ranges
}

func checkStatsEquivalence(t *testing.T, store *Store, rows []RequestDetail, queryRange usageRange, filter usageFilter) {
	t.Helper()
	reference := referenceAggregates(rows, queryRange)
	unbounded := usageRange{Name: queryRange.Name}

	stats, err := store.queryStatsByFilter(queryRange, filter)
	if err != nil {
		t.Fatal(err)
	}
	want := buildStatsForRangeWithFilter(reference, stats.RetainedSince, stats.LastUsed, unbounded, filter, stats.GeneratedAt, nil)
	if !reflect.DeepEqual(stats, want) {
		t.Fatalf("stats for %+v filter %+v:\n got %+v\nwant %+v", queryRange, filter, stats, want)
	}
	var summary Counters
	for _, row := range rows {
		if inRange(row, queryRange) && filter.matches(row.Dimensions) {
			summary.Requests++
			summary.InputTokens += row.InputTokens
			summary.TotalTokens += row.TotalTokens
			summary.TotalLatencyNS += row.LatencyNS
		}
	}
	if stats.Summary.Requests != summary.Requests || stats.Summary.InputTokens != summary.InputTokens || stats.Summary.TotalTokens != summary.TotalTokens || stats.Summary.TotalLatencyNS != summary.TotalLatencyNS {
		t.Fatalf("summary for %+v filter %+v = %+v, want %+v", queryRange, filter, stats.Summary, summary)
	}

	initial, err := store.queryInitialStatsByFilter(queryRange, filter)
	if err != nil {
		t.Fatal(err)
	}
	wantInitial := buildInitialStatsForRange(reference, initial.RetainedSince, initial.LastUsed, unbounded, filter, initial.GeneratedAt, nil)
	wantInitial.BucketSeconds = initial.BucketSeconds
	wantInitial.Series = rebucketSeries(stats.Series, initial.BucketSeconds)
	if !reflect.DeepEqual(initial, wantInitial) {
		t.Fatalf("initial stats for %+v filter %+v:\n got %+v\nwant %+v", queryRange, filter, initial, wantInitial)
	}

	trend, err := store.queryStatsTrendByFilter(queryRange, filter)
	if err != nil {
		t.Fatal(err)
	}
	var trendRequests, statsRequests uint64
	for _, point := range trend.ModelSeries {
		trendRequests += point.Requests
	}
	for _, point := range stats.ModelSeries {
		statsRequests += point.Requests
	}
	if trendRequests != statsRequests || trend.BucketSeconds != initial.BucketSeconds {
		t.Fatalf("trend for %+v filter %+v: requests %d, want %d", queryRange, filter, trendRequests, statsRequests)
	}

	groups, err := store.queryGroupsByFilter(queryRange, filter)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(groups.Items, stats.Groups) && (len(groups.Items) != 0 || len(stats.Groups) != 0) {
		t.Fatalf("groups for %+v filter %+v:\n got %+v\nwant %+v", queryRange, filter, groups.Items, stats.Groups)
	}
}

func rebucketSeries(points []SeriesPoint, bucketSeconds uint64) []SeriesPoint {
	series := make(map[int64]Counters)
	for _, point := range points {
		at, _ := time.Parse(time.RFC3339, point.Hour)
		bucket := compactBucket(at.Unix(), bucketSeconds)
		value := series[bucket]
		value.add(point.Counters)
		series[bucket] = value
	}
	return sortedCompactSeries(series)
}

func checkRequestEquivalence(t *testing.T, random *rand.Rand, store *Store, rows []RequestDetail, queryRange usageRange, filter usageFilter) {
	t.Helper()
	model := []string{"", "", "alpha", "front", "未标记模型"}[random.Intn(5)]
	result := []string{"", "", "success", "failed"}[random.Intn(4)]
	var want []uint64
	for index := len(rows) - 1; index >= 0; index-- {
		row := rows[index]
		if !inRange(row, queryRange) || !matchesModelFilter(row.Model, row.Alias, model) || !filter.matches(row.Dimensions) {
			continue
		}
		if (result == "success" && row.Failed) || (result == "failed" && !row.Failed) {
			continue
		}
		want = append(want, row.Sequence)
	}
	for _, paging := range [][2]int{{0, 5}, {3, 4}, {len(want) / 2, 7}, {max(0, len(want)-2), 5}, {len(want) + 3, 5}, {0, 500}} {
		offset, limit := paging[0], paging[1]
		page, err := store.queryRequestPageByFilter(queryRange, offset, limit, model, filter, result)
		if err != nil {
			t.Fatal(err)
		}
		expected := want[min(offset, len(want)):min(offset+limit, len(want))]
		got := make([]uint64, 0, len(page.Items))
		for _, item := range page.Items {
			got = append(got, item.Sequence)
		}
		if page.Total != len(want) || !reflect.DeepEqual(got, append([]uint64{}, expected...)) {
			t.Fatalf("requests for %+v filter %+v model %q result %q offset %d limit %d:\n total %d, want %d\n got %v\nwant %v", queryRange, filter, model, result, offset, limit, page.Total, len(want), got, expected)
		}
	}
}

func checkCostEquivalence(t *testing.T, store *Store, rows []RequestDetail, queryRange usageRange, filter usageFilter) {
	t.Helper()
	book, err := store.QueryPriceBook()
	if err != nil {
		t.Fatal(err)
	}
	resolver := newModelPriceResolver(book.Prices, book.SyncSettings)
	costs, err := store.queryCostsByFilter(queryRange, filter)
	if err != nil {
		t.Fatal(err)
	}
	type modelKey struct{ Provider, Model string }
	var summary CostAmounts
	models := make(map[modelKey]CostAmounts)
	series := make(map[string]CostAmounts)
	for _, row := range rows {
		if !inRange(row, queryRange) || !filter.matches(row.Dimensions) {
			continue
		}
		cost := estimateRequestCostWithResolver(row, resolver)
		key := modelKey{Provider: row.Provider, Model: compactModelName(row.Model)}
		amounts := models[key]
		addEstimatedCost(&amounts, cost)
		models[key] = amounts
		addEstimatedCost(&summary, cost)
		bucket := time.Unix(compactBucket(row.Time.Truncate(time.Minute).Unix(), costs.BucketSeconds), 0).UTC().Format(time.RFC3339) + "|" + key.Provider + "|" + key.Model
		point := series[bucket]
		addEstimatedCost(&point, cost)
		series[bucket] = point
	}
	same := func(left, right CostAmounts) bool {
		return left.Requests == right.Requests && left.PricedRequests == right.PricedRequests && left.UnpricedRequests == right.UnpricedRequests &&
			math.Abs(left.TotalUSD-right.TotalUSD) < 1e-9 && math.Abs(left.InputUSD-right.InputUSD) < 1e-9 && math.Abs(left.OutputUSD-right.OutputUSD) < 1e-9 && math.Abs(left.CacheReadUSD-right.CacheReadUSD) < 1e-9
	}
	if !same(costs.Summary, summary) {
		t.Fatalf("cost summary for %+v filter %+v = %+v, want %+v", queryRange, filter, costs.Summary, summary)
	}
	if len(costs.Models) != len(models) || len(costs.Series) != len(series) {
		t.Fatalf("cost shape for %+v filter %+v: %d models %d points, want %d and %d", queryRange, filter, len(costs.Models), len(costs.Series), len(models), len(series))
	}
	var missing int
	for _, item := range costs.Models {
		want := models[modelKey{Provider: item.Provider, Model: item.Model}]
		if !same(item.CostAmounts, want) {
			t.Fatalf("model cost %s/%s for %+v = %+v, want %+v", item.Provider, item.Model, queryRange, item.CostAmounts, want)
		}
		if want.UnpricedRequests > 0 {
			missing++
		}
	}
	if len(costs.MissingPrices) != missing {
		t.Fatalf("missing prices for %+v = %+v, want %d rows", queryRange, costs.MissingPrices, missing)
	}
	for _, point := range costs.Series {
		if want := series[point.Hour+"|"+point.Provider+"|"+point.Model]; !same(point.CostAmounts, want) {
			t.Fatalf("cost point %+v for %+v, want %+v", point, queryRange, want)
		}
	}
}

func TestQueriesMatchRequestLogScan(t *testing.T) {
	config := testConfig(t)
	config.FlushMaxRecords = 37
	store, err := openStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if _, err := store.SaveModelPrices(map[string]ModelPrice{
		"alpha": {Input: 3, Output: 15, CacheRead: 0.3, ContextTiers: []ContextPriceTier{{Threshold: 200, Input: 6, Output: 30, CacheRead: 0.6}}},
		"front": {Input: 1, Output: 2, ServiceTiers: map[string]ServiceTierPrice{"priority": {Input: 2, Output: 4}}},
	}); err != nil {
		t.Fatal(err)
	}

	random := rand.New(rand.NewSource(20260808))
	span := 3 * time.Hour
	base := time.Now().UTC().Add(-2 * 24 * time.Hour).Truncate(time.Hour).Add(13 * time.Second)
	record := func(count int) {
		t.Helper()
		for range count {
			// Cluster part of the load so single minutes hold many rows.
			at := base.Add(time.Duration(random.Int63n(int64(span))))
			if random.Intn(4) == 0 {
				at = base.Add(span / 2).Add(time.Duration(random.Int63n(int64(3 * time.Minute))))
			}
			if err := store.Record(equivalenceUsage(random, at)); err != nil {
				t.Fatal(err)
			}
		}
	}
	filters := []usageFilter{{}, newUsageFilter("cli", ""), newUsageFilter("Codex-user@example.com", ""), {Model: "front"}, newUsageFilter("absent", "")}
	check := func() {
		t.Helper()
		rows := equivalenceRows(t, store)
		sort.SliceStable(rows, func(i, j int) bool {
			if !rows[i].Time.Equal(rows[j].Time) {
				return rows[i].Time.Before(rows[j].Time)
			}
			return rows[i].Sequence < rows[j].Sequence
		})
		for _, queryRange := range equivalenceRanges(random, base, span) {
			filter := filters[random.Intn(len(filters))]
			checkStatsEquivalence(t, store, rows, queryRange, filter)
			checkRequestEquivalence(t, random, store, rows, queryRange, filter)
			checkCostEquivalence(t, store, rows, queryRange, filter)
		}
	}

	record(900)
	check()
	// New requests land in minutes the cost memo already holds.
	record(250)
	check()
	if _, err := store.SaveModelPrices(map[string]ModelPrice{"alpha": {Input: 5, Output: 9}, "gamma": {Input: 1, Output: 1}}); err != nil {
		t.Fatal(err)
	}
	record(60)
	check()

	// A restart rebuilds the in-memory index from the database.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if store, err = openStore(config); err != nil {
		t.Fatal(err)
	}
	check()
	record(120)
	check()

	if err := store.Reset(); err != nil {
		t.Fatal(err)
	}
	check()
	record(80)
	check()
}
