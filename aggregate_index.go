package main

import (
	"math"
	"sort"
	"time"
)

// aggregateCell is one dimension row inside one UTC minute.
type aggregateCell struct {
	Counters
	dim uint32
	// records counts the request-log rows behind the cell. It lets request
	// paging total and skip whole minutes without decoding their rows.
	records uint32
	// aggregated is false for a cell that only exists because the request log
	// holds rows the minute aggregates never saw; statistics ignore those.
	aggregated bool
}

// aggregateIndex holds the retained minute aggregates grouped by minute.
// Dimension rows are interned, so a range query walks only the minutes it
// covers and evaluates filters once per distinct dimension row rather than
// once per minute.
type aggregateIndex struct {
	dims []Dimensions
	// clean caches sanitizeDimensionsSource for every interned row.
	clean   []Dimensions
	ids     map[Dimensions]uint32
	minutes map[int64][]aggregateCell
	// unaligned records that a key was not a whole minute, which disables the
	// slot walk in each.
	unaligned bool
}

func newAggregateIndex() *aggregateIndex {
	return &aggregateIndex{ids: make(map[Dimensions]uint32), minutes: make(map[int64][]aggregateCell)}
}

func aggregateIndexFromMap(data map[aggregateKey]Counters) *aggregateIndex {
	index := newAggregateIndex()
	for key, counters := range data {
		index.set(key, counters)
	}
	return index
}

func (x *aggregateIndex) intern(dimensions Dimensions) uint32 {
	if id, ok := x.ids[dimensions]; ok {
		return id
	}
	id := uint32(len(x.dims))
	x.ids[dimensions] = id
	x.dims = append(x.dims, dimensions)
	x.clean = append(x.clean, sanitizeDimensionsSource(dimensions))
	return id
}

func (x *aggregateIndex) find(key aggregateKey) *aggregateCell {
	id, ok := x.ids[key.Dimensions]
	if !ok {
		return nil
	}
	cells := x.minutes[key.Hour]
	for index := range cells {
		if cells[index].dim == id {
			return &cells[index]
		}
	}
	return nil
}

// cell returns the cell for key, creating it when missing. The pointer is only
// valid until the next insertion.
func (x *aggregateIndex) cell(key aggregateKey) *aggregateCell {
	if found := x.find(key); found != nil {
		return found
	}
	if key.Hour%60 != 0 {
		x.unaligned = true
	}
	cells := append(x.minutes[key.Hour], aggregateCell{dim: x.intern(key.Dimensions)})
	x.minutes[key.Hour] = cells
	return &cells[len(cells)-1]
}

func (x *aggregateIndex) get(key aggregateKey) Counters {
	if found := x.find(key); found != nil {
		return found.Counters
	}
	return Counters{}
}

func (x *aggregateIndex) set(key aggregateKey, counters Counters) {
	cell := x.cell(key)
	cell.Counters = counters
	cell.aggregated = true
}

func (x *aggregateIndex) add(key aggregateKey, counters Counters) {
	cell := x.cell(key)
	cell.Counters.add(counters)
	cell.aggregated = true
}

func (x *aggregateIndex) countRecord(key aggregateKey) {
	if cell := x.cell(key); cell.records < math.MaxUint32 {
		cell.records++
	}
}

func (x *aggregateIndex) prune(cutoff int64) {
	for minute := range x.minutes {
		if minute < cutoff {
			delete(x.minutes, minute)
		}
	}
}

// eachMinute calls visit for every stored minute in [from, to).
func (x *aggregateIndex) eachMinute(from, to int64, visit func(minute int64, cells []aggregateCell)) {
	if from >= to {
		return
	}
	if !x.unaligned && from%60 == 0 && from > math.MinInt64/2 && to < math.MaxInt64/2 && (to-from)/60 < int64(len(x.minutes)) {
		for minute := from; minute < to; minute += 60 {
			if cells := x.minutes[minute]; len(cells) > 0 {
				visit(minute, cells)
			}
		}
		return
	}
	for minute, cells := range x.minutes {
		if minute >= from && minute < to {
			visit(minute, cells)
		}
	}
}

// minuteWindow splits a query range into the whole minutes that the in-memory
// aggregates answer and the partial minutes at either end, which only the
// request log can answer exactly.
type minuteWindow struct {
	from, to int64
	edges    [][2]time.Time
}

func wholeMinute(value time.Time) bool {
	return value.Second() == 0 && value.Nanosecond() == 0
}

func windowForRange(queryRange usageRange) minuteWindow {
	window := minuteWindow{from: math.MinInt64, to: math.MaxInt64}
	start, end := queryRange.Start.UTC(), queryRange.End.UTC()
	if !queryRange.Start.IsZero() {
		window.from = start.Truncate(time.Minute).Unix()
		if !wholeMinute(start) {
			window.from += 60
		}
	}
	if !queryRange.End.IsZero() {
		window.to = end.Truncate(time.Minute).Unix()
	}
	if window.from > window.to {
		// Both bounds fall inside the same minute.
		window.to = window.from
		window.edges = [][2]time.Time{{start, end}}
		return window
	}
	if !queryRange.Start.IsZero() && !wholeMinute(start) {
		window.edges = append(window.edges, [2]time.Time{start, time.Unix(window.from, 0).UTC()})
	}
	if !queryRange.End.IsZero() && !wholeMinute(end) {
		window.edges = append(window.edges, [2]time.Time{time.Unix(window.to, 0).UTC(), end})
	}
	return window
}

// bucketWindow reproduces the range test of the map-based helpers, which keep
// a minute bucket when its start lies inside the range.
func bucketWindow(queryRange usageRange) minuteWindow {
	window := windowForRange(queryRange)
	if !queryRange.End.IsZero() && !wholeMinute(queryRange.End.UTC()) && len(window.edges) > 0 {
		window.to = queryRange.End.UTC().Truncate(time.Minute).Unix() + 60
	}
	window.edges = nil
	return window
}

// edgeAggregate is one request-log row inside a partial minute, shaped like an
// aggregate cell.
type edgeAggregate struct {
	minute   int64
	dim      uint32
	counters Counters
}

// aggregateView is everything inside one query range: whole minutes from the
// index plus the rows of its partial minutes.
type aggregateView struct {
	index  *aggregateIndex
	window minuteWindow
	edges  []edgeAggregate
}

func (v aggregateView) each(visit func(minute int64, dim uint32, counters *Counters)) {
	v.index.eachMinute(v.window.from, v.window.to, func(minute int64, cells []aggregateCell) {
		for index := range cells {
			if cells[index].aggregated {
				visit(minute, cells[index].dim, &cells[index].Counters)
			}
		}
	})
	for index := range v.edges {
		visit(v.edges[index].minute, v.edges[index].dim, &v.edges[index].counters)
	}
}

// dimensionMatcher evaluates a predicate once per interned dimension row.
type dimensionMatcher struct {
	clean []Dimensions
	test  func(Dimensions) bool
	state []uint8
}

func (v aggregateView) matcher(test func(Dimensions) bool) *dimensionMatcher {
	return &dimensionMatcher{clean: v.index.clean, test: test, state: make([]uint8, len(v.index.clean))}
}

func (m *dimensionMatcher) match(id uint32) bool {
	switch m.state[id] {
	case 1:
		return true
	case 2:
		return false
	}
	if m.test(m.clean[id]) {
		m.state[id] = 1
		return true
	}
	m.state[id] = 2
	return false
}

// rangeFacets collects the filter options offered for a range. They cover
// every row in the range, including rows the active filter hides.
type rangeFacets struct {
	seen       []bool
	sources    map[string]struct{}
	apiKeyRefs map[string]struct{}
}

func (v aggregateView) facets() *rangeFacets {
	return &rangeFacets{seen: make([]bool, len(v.index.clean)), sources: make(map[string]struct{}), apiKeyRefs: make(map[string]struct{})}
}

func (f *rangeFacets) observe(id uint32, dimensions Dimensions) {
	if f.seen[id] {
		return
	}
	f.seen[id] = true
	if dimensions.Source != "" {
		f.sources[dimensions.Source] = struct{}{}
	}
	if ref := apiKeyRef(dimensions.APIKeyGeneration, dimensions.APIKeyHash); ref != "" {
		f.apiKeyRefs[ref] = struct{}{}
	}
}

func (f *rangeFacets) sortedSources() []string {
	values := make([]string, 0, len(f.sources))
	for value := range f.sources {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

// groupRows merges per-dimension totals into sorted dimension rows. Distinct
// stored rows can share a sanitized form, so they are merged here.
func (v aggregateView) groupRows(totals []Counters, used []bool, apiKeyCiphertexts map[string]string) []GroupStats {
	groups := make(map[Dimensions]Counters)
	for id, isUsed := range used {
		if !isUsed {
			continue
		}
		dimensions := v.index.clean[id]
		group := groups[dimensions]
		group.add(totals[id])
		groups[dimensions] = group
	}
	rows := make([]GroupStats, 0, len(groups))
	for dimensions, counters := range groups {
		if ref := apiKeyRef(dimensions.APIKeyGeneration, dimensions.APIKeyHash); ref != "" {
			dimensions.APIKey = apiKeyCiphertexts[ref]
		}
		rows = append(rows, GroupStats{Dimensions: dimensions, Counters: counters, AverageLatencyNS: counters.averageLatencyNS(), AverageTTFTNS: counters.averageTTFTNS()})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].TotalTokens != rows[j].TotalTokens {
			return rows[i].TotalTokens > rows[j].TotalTokens
		}
		return compareDimensions(rows[i].Dimensions, rows[j].Dimensions) < 0
	})
	return rows
}

type modelBucketKey struct {
	bucket int64
	model  uint32
}

// modelNames interns model names so per-model series key on an integer.
type modelNames struct {
	names []string
	ids   map[string]uint32
	byDim []uint32
}

func (v aggregateView) modelNames(name func(Dimensions) string) *modelNames {
	names := &modelNames{ids: make(map[string]uint32), byDim: make([]uint32, len(v.index.clean))}
	for id, dimensions := range v.index.dims {
		model := name(dimensions)
		modelID, ok := names.ids[model]
		if !ok {
			modelID = uint32(len(names.names))
			names.ids[model] = modelID
			names.names = append(names.names, model)
		}
		names.byDim[id] = modelID
	}
	return names
}

func (n *modelNames) points(series map[modelBucketKey]Counters) []ModelSeriesPoint {
	keys := make([]modelBucketKey, 0, len(series))
	for key := range series {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].bucket != keys[j].bucket {
			return keys[i].bucket < keys[j].bucket
		}
		return n.names[keys[i].model] < n.names[keys[j].model]
	})
	points := make([]ModelSeriesPoint, 0, len(keys))
	for _, key := range keys {
		points = append(points, ModelSeriesPoint{Hour: time.Unix(key.bucket, 0).UTC().Format(time.RFC3339), Model: n.names[key.model], Counters: series[key]})
	}
	return points
}

func (v aggregateView) stats(since, lastUsed time.Time, queryRange usageRange, filter usageFilter, now time.Time, apiKeyCiphertexts map[string]string) StatsResponse {
	matcher := v.matcher(filter.matches)
	facets := v.facets()
	// The legacy response labels its series with the stored model name.
	models := v.modelNames(func(dimensions Dimensions) string {
		if dimensions.Model == "" {
			return "未标记模型"
		}
		return dimensions.Model
	})
	totals := make([]Counters, len(v.index.clean))
	used := make([]bool, len(v.index.clean))
	series := make(map[int64]Counters)
	modelSeries := make(map[modelBucketKey]Counters)
	summary := Counters{}
	v.each(func(minute int64, id uint32, counters *Counters) {
		facets.observe(id, v.index.clean[id])
		if !matcher.match(id) {
			return
		}
		totals[id].add(*counters)
		used[id] = true
		point := series[minute]
		point.add(*counters)
		series[minute] = point
		modelKey := modelBucketKey{bucket: minute, model: models.byDim[id]}
		modelPoint := modelSeries[modelKey]
		modelPoint.add(*counters)
		modelSeries[modelKey] = modelPoint
		summary.add(*counters)
	})
	return StatsResponse{
		SchemaVersion: 1,
		GeneratedAt:   now.UTC(),
		Range:         queryRange.Name,
		RetainedSince: since.UTC(),
		LastUsed:      lastUsed.UTC(),
		Summary:       summary,
		Groups:        v.groupRows(totals, used, apiKeyCiphertexts),
		Series:        sortedCompactSeries(series),
		ModelSeries:   models.points(modelSeries),
		Sources:       facets.sortedSources(),
		APIKeys:       sortedStatsAPIKeys(facets.apiKeyRefs, apiKeyCiphertexts),
	}
}

func (v aggregateView) initialStats(since, lastUsed time.Time, queryRange usageRange, filter usageFilter, now time.Time, apiKeyCiphertexts map[string]string) InitialStatsResponse {
	bucketSeconds := compactBucketSeconds(queryRange, since, now)
	matcher := v.matcher(filter.matches)
	facets := v.facets()
	series := make(map[int64]Counters)
	models := make(map[string]*compactModelAggregate)
	byDim := make([]*compactModelAggregate, len(v.index.clean))
	summary := Counters{}
	v.each(func(minute int64, id uint32, counters *Counters) {
		dimensions := v.index.clean[id]
		facets.observe(id, dimensions)
		if !matcher.match(id) {
			return
		}
		seriesKey := compactBucket(minute, bucketSeconds)
		point := series[seriesKey]
		point.add(*counters)
		series[seriesKey] = point
		aggregate := byDim[id]
		if aggregate == nil {
			aggregate = compactModelBucket(models, compactModelName(dimensions.Model))
			aggregate.observeAlias(dimensions.Alias)
			byDim[id] = aggregate
		}
		aggregate.add(*counters)
		summary.add(*counters)
	})
	return InitialStatsResponse{SchemaVersion: 2, GeneratedAt: now.UTC(), Range: queryRange.Name, RetainedSince: since.UTC(), LastUsed: lastUsed.UTC(), BucketSeconds: bucketSeconds, Summary: summary, Models: sortedCompactModels(models), Series: sortedCompactSeries(series), Sources: facets.sortedSources(), APIKeys: sortedStatsAPIKeys(facets.apiKeyRefs, apiKeyCiphertexts)}
}

func (v aggregateView) trendStats(since time.Time, queryRange usageRange, filter usageFilter, now time.Time) StatsTrendResponse {
	bucketSeconds := compactBucketSeconds(queryRange, since, now)
	matcher := v.matcher(filter.matches)
	models := v.modelNames(func(dimensions Dimensions) string { return compactModelName(dimensions.Model) })
	series := make(map[modelBucketKey]Counters)
	v.each(func(minute int64, id uint32, counters *Counters) {
		if !matcher.match(id) {
			return
		}
		key := modelBucketKey{bucket: compactBucket(minute, bucketSeconds), model: models.byDim[id]}
		point := series[key]
		point.add(*counters)
		series[key] = point
	})
	return StatsTrendResponse{SchemaVersion: 2, GeneratedAt: now.UTC(), Range: queryRange.Name, BucketSeconds: bucketSeconds, ModelSeries: models.points(series)}
}

func (v aggregateView) groupStats(queryRange usageRange, filter usageFilter, now time.Time, apiKeyCiphertexts map[string]string) GroupStatsPage {
	matcher := v.matcher(filter.matches)
	totals := make([]Counters, len(v.index.clean))
	used := make([]bool, len(v.index.clean))
	v.each(func(_ int64, id uint32, counters *Counters) {
		if !matcher.match(id) {
			return
		}
		totals[id].add(*counters)
		used[id] = true
	})
	items := v.groupRows(totals, used, apiKeyCiphertexts)
	return GroupStatsPage{SchemaVersion: 2, GeneratedAt: now.UTC(), Range: queryRange.Name, Items: items, Total: len(items)}
}

// retainedDimensionsSince lists the distinct dimension rows that still have
// aggregates in a minute at or after cutoff.
func (x *aggregateIndex) retainedDimensionsSince(cutoff int64) []Dimensions {
	seen := make([]bool, len(x.dims))
	var rows []Dimensions
	for minute, cells := range x.minutes {
		if minute < cutoff {
			continue
		}
		for index := range cells {
			if id := cells[index].dim; cells[index].aggregated && !seen[id] {
				seen[id] = true
				rows = append(rows, x.dims[id])
			}
		}
	}
	return rows
}

func (x *aggregateIndex) retainedDimensions() []Dimensions {
	return x.retainedDimensionsSince(math.MinInt64)
}
