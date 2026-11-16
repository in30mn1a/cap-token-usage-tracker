package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// EstimatedCost is calculated from one persisted request using the current model price.
type EstimatedCost struct {
	Priced                bool    `json:"priced"`
	Source                string  `json:"source,omitempty"`
	AccountingMode        string  `json:"accounting_mode,omitempty"`
	PriceServiceTier      string  `json:"price_service_tier,omitempty"`
	TierThreshold         uint64  `json:"tier_threshold,omitempty"`
	ContextTokens         uint64  `json:"context_tokens,omitempty"`
	BillableInputTokens   uint64  `json:"billable_input_tokens,omitempty"`
	BilledCacheReadTokens uint64  `json:"billed_cache_read_tokens,omitempty"`
	InputUSD              float64 `json:"input_usd"`
	OutputUSD             float64 `json:"output_usd"`
	CacheReadUSD          float64 `json:"cache_read_usd"`
	CacheCreationUSD      float64 `json:"cache_creation_usd"`
	TotalUSD              float64 `json:"total_usd"`
}

type CostAmounts struct {
	Requests         uint64  `json:"requests"`
	PricedRequests   uint64  `json:"priced_requests"`
	UnpricedRequests uint64  `json:"unpriced_requests"`
	InputUSD         float64 `json:"input_usd"`
	OutputUSD        float64 `json:"output_usd"`
	CacheReadUSD     float64 `json:"cache_read_usd"`
	CacheCreationUSD float64 `json:"cache_creation_usd"`
	TotalUSD         float64 `json:"total_usd"`
}

type ModelCostStats struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
	CostAmounts
}

type CostSeriesPoint struct {
	Hour     string `json:"hour"`
	Provider string `json:"provider"`
	Model    string `json:"model"`
	CostAmounts
}

type MissingPriceStats struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
	Requests uint64 `json:"requests"`
}

type CostResponse struct {
	SchemaVersion     uint32    `json:"schema_version"`
	GeneratedAt       time.Time `json:"generated_at"`
	Range             string    `json:"range"`
	Currency          string    `json:"currency"`
	EstimateBasis     string    `json:"estimate_basis"`
	PriceBookRevision uint64    `json:"price_book_revision"`
	// BucketSeconds is the width of the series buckets. It matches the trend
	// statistics for the same range, so the two line up in the dashboard.
	BucketSeconds uint64              `json:"bucket_seconds"`
	Summary       CostAmounts         `json:"summary"`
	Models        []ModelCostStats    `json:"models"`
	Series        []CostSeriesPoint   `json:"series"`
	MissingPrices []MissingPriceStats `json:"missing_prices"`
}

func (c *CostResponse) Redact() {}

func (c *CostResponse) Reveal(decrypt DecryptFunc) {}

const (
	maxCostCacheEntries = 16
	maxCostScanWorkers  = 4
)

type costQuerySnapshot struct {
	Range         string
	Start         time.Time
	End           time.Time
	GeneratedAt   time.Time
	Prices        map[string]ModelPrice
	PriceSettings PriceSyncSettings
	PriceRevision uint64
	HighWater     uint64
	Generation    uint64
	Filter        usageFilter
	Since         time.Time
}

type costCacheKey struct {
	Range         string
	StartUnix     int64
	EndUnix       int64
	PriceRevision uint64
	HighWater     uint64
	Generation    uint64
	Filter        usageFilter
}

type costFlight struct {
	done     chan struct{}
	response CostResponse
	err      error
}

type resolvedModelPrice struct {
	price     ModelPrice
	ambiguous bool
}

type modelPriceResolver struct {
	exact      map[string]ModelPrice
	settings   PriceSyncSettings
	normalized map[string]resolvedModelPrice
}

// newModelPriceResolver builds the normalized fallback index once per price-book snapshot.
func newModelPriceResolver(prices map[string]ModelPrice, settings PriceSyncSettings) modelPriceResolver {
	resolver := modelPriceResolver{exact: prices}
	normalizedSettings, err := normalizePriceSyncSettings(settings)
	if err != nil {
		return resolver
	}
	resolver.settings = normalizedSettings
	resolver.normalized = make(map[string]resolvedModelPrice, len(prices))

	keys := make([]string, 0, len(prices))
	for key := range prices {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		comparison := comparisonModelName(key, normalizedSettings)
		if comparison == "" {
			continue
		}
		price := prices[key]
		current, exists := resolver.normalized[comparison]
		if !exists {
			resolver.normalized[comparison] = resolvedModelPrice{price: price}
			continue
		}
		// Never choose arbitrarily when equivalent model names carry different billable rates.
		if !sameEditableModelPrice(current.price, price) {
			current.ambiguous = true
			resolver.normalized[comparison] = current
		}
	}
	return resolver
}

func (r modelPriceResolver) resolve(model string) (ModelPrice, bool) {
	// An explicit price key always wins over suffix or mapping fallback candidates.
	if price, ok := r.exact[model]; ok {
		return price, true
	}
	if len(r.normalized) == 0 {
		return ModelPrice{}, false
	}
	comparison := comparisonModelName(model, r.settings)
	match, ok := r.normalized[comparison]
	if !ok || match.ambiguous {
		return ModelPrice{}, false
	}
	return match.price, true
}

func (s *Store) QueryCosts(rangeName string) (CostResponse, error) {
	queryRange, err := presetUsageRange(rangeName, time.Now().UTC())
	if err != nil {
		return CostResponse{}, err
	}
	return s.queryCosts(queryRange)
}

func (s *Store) queryCosts(queryRange usageRange) (CostResponse, error) {
	return s.queryCostsByFilter(queryRange, usageFilter{})
}

func (s *Store) queryCostsBySource(queryRange usageRange, source string) (CostResponse, error) {
	return s.queryCostsByFilter(queryRange, newUsageFilter(source, ""))
}

func (s *Store) queryCostsByFilter(queryRange usageRange, filter usageFilter) (CostResponse, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	if s.closed {
		return CostResponse{}, errors.New("store is closed")
	}
	resp := make(chan costSnapshotResult, 1)
	s.commands <- costSnapshotCommand{queryRange: queryRange, filter: filter, resp: resp}
	result := <-resp
	if result.err != nil {
		return CostResponse{}, result.err
	}
	snapshot := result.snapshot
	key := costCacheKey{
		Range:         snapshot.Range,
		StartUnix:     snapshot.Start.UnixNano(),
		EndUnix:       snapshot.End.UnixNano(),
		PriceRevision: snapshot.PriceRevision,
		HighWater:     snapshot.HighWater,
		Generation:    snapshot.Generation,
		Filter:        snapshot.Filter,
	}

	s.costMu.Lock()
	if cached, ok := s.costCache[key]; ok {
		s.costMu.Unlock()
		return cloneCostResponse(cached), nil
	}
	if flight, ok := s.costFlights[key]; ok {
		s.costMu.Unlock()
		<-flight.done
		return cloneCostResponse(flight.response), flight.err
	}
	flight := &costFlight{done: make(chan struct{})}
	s.costFlights[key] = flight
	hook := s.costScanHook
	s.costMu.Unlock()

	if hook != nil {
		hook()
	}
	response, err := s.scanCosts(snapshot)

	s.costMu.Lock()
	flight.response = cloneCostResponse(response)
	flight.err = err
	if err == nil {
		if _, exists := s.costCache[key]; !exists {
			s.costOrder = append(s.costOrder, key)
		}
		s.costCache[key] = cloneCostResponse(response)
		for len(s.costOrder) > maxCostCacheEntries {
			oldest := s.costOrder[0]
			s.costOrder = s.costOrder[1:]
			delete(s.costCache, oldest)
		}
	}
	delete(s.costFlights, key)
	close(flight.done)
	s.costMu.Unlock()
	return response, err
}

func cloneCostResponse(input CostResponse) CostResponse {
	input.Models = append([]ModelCostStats(nil), input.Models...)
	input.Series = append([]CostSeriesPoint(nil), input.Series...)
	input.MissingPrices = append([]MissingPriceStats(nil), input.MissingPrices...)
	return input
}

// costChanges is what the actor reports to a cost scan: which minutes gained
// requests since the memo was last brought up to date, and the state the
// answer has to agree with.
type costChanges struct {
	Minutes       []int64
	HighWater     uint64
	PriceRevision uint64
	Generation    uint64
	PrunedBefore  int64
}

type costCell struct {
	dim uint32
	CostAmounts
}

// costMemo keeps per-minute cost totals for one price book. Estimating cost
// needs every request row, because context tiers depend on the size of each
// request, so the memo lets a refresh re-read only the minutes that changed
// instead of the whole range.
type costMemo struct {
	revision     uint64
	generation   uint64
	highWater    uint64
	prunedBefore int64
	dims         []Dimensions
	ids          map[Dimensions]uint32
	minutes      map[int64][]costCell
	// covered lists the disjoint, sorted [from, to) minute intervals whose
	// rows are reflected in minutes.
	covered [][2]int64
	// stale holds covered minutes that gained requests since they were read.
	stale map[int64]struct{}
}

func newCostMemo(revision, generation uint64) *costMemo {
	return &costMemo{revision: revision, generation: generation, ids: make(map[Dimensions]uint32), minutes: make(map[int64][]costCell), stale: make(map[int64]struct{})}
}

func (m *costMemo) intern(dimensions Dimensions) uint32 {
	if id, ok := m.ids[dimensions]; ok {
		return id
	}
	id := uint32(len(m.dims))
	m.ids[dimensions] = id
	m.dims = append(m.dims, dimensions)
	return id
}

func addCostCell(cells []costCell, dim uint32, cost EstimatedCost) []costCell {
	for index := range cells {
		if cells[index].dim == dim {
			addEstimatedCost(&cells[index].CostAmounts, cost)
			return cells
		}
	}
	cells = append(cells, costCell{dim: dim})
	addEstimatedCost(&cells[len(cells)-1].CostAmounts, cost)
	return cells
}

func (m *costMemo) isCovered(minute int64) bool {
	for _, interval := range m.covered {
		if minute >= interval[0] && minute < interval[1] {
			return true
		}
	}
	return false
}

// gaps returns the parts of [from, to) that are not covered yet.
func (m *costMemo) gaps(from, to int64) [][2]int64 {
	var result [][2]int64
	for _, interval := range m.covered {
		if interval[1] <= from {
			continue
		}
		if interval[0] >= to {
			break
		}
		if interval[0] > from {
			result = append(result, [2]int64{from, interval[0]})
		}
		from = interval[1]
	}
	if from < to {
		result = append(result, [2]int64{from, to})
	}
	return result
}

func (m *costMemo) cover(from, to int64) {
	if from >= to {
		return
	}
	merged := make([][2]int64, 0, len(m.covered)+1)
	inserted := false
	for _, interval := range m.covered {
		switch {
		case interval[1] < from:
			merged = append(merged, interval)
		case interval[0] > to:
			if !inserted {
				merged = append(merged, [2]int64{from, to})
				inserted = true
			}
			merged = append(merged, interval)
		default:
			from, to = min(from, interval[0]), max(to, interval[1])
		}
	}
	if !inserted {
		merged = append(merged, [2]int64{from, to})
	}
	m.covered = merged
}

func (m *costMemo) prune(cutoff int64) {
	if cutoff <= m.prunedBefore {
		return
	}
	m.prunedBefore = cutoff
	for minute := range m.minutes {
		if minute < cutoff {
			delete(m.minutes, minute)
		}
	}
	for minute := range m.stale {
		if minute < cutoff {
			delete(m.stale, minute)
		}
	}
}

type costRow struct {
	minute     int64
	dimensions Dimensions
	cost       EstimatedCost
}

// costRowDecoder turns request-log values into cost rows. It is not safe for
// concurrent use; each worker owns one.
type costRowDecoder struct {
	highWater uint64
	resolver  modelPriceResolver
	// clean caches sanitizeDimensionsSource, which is far more expensive than
	// a lookup and repeats for every request of the same dimension row.
	clean map[Dimensions]Dimensions
}

func newCostRowDecoder(highWater uint64, resolver modelPriceResolver) *costRowDecoder {
	return &costRowDecoder{highWater: highWater, resolver: resolver, clean: make(map[Dimensions]Dimensions)}
}

func (d *costRowDecoder) decode(unixNano int64, value []byte) (costRow, bool, error) {
	var request RequestDetail
	if err := json.Unmarshal(value, &request); err != nil {
		return costRow{}, false, fmt.Errorf("decode request detail: %w", err)
	}
	if request.Sequence > d.highWater {
		return costRow{}, false, nil
	}
	request.APIKey = ""
	clean, ok := d.clean[request.Dimensions]
	if !ok {
		clean = sanitizeDimensionsSource(request.Dimensions)
		d.clean[request.Dimensions] = clean
	}
	request.Dimensions = clean
	request.EstimatedCost = nil
	key := requestAggregateKey(unixNano, clean)
	return costRow{minute: key.Hour, dimensions: key.Dimensions, cost: estimateRequestCostWithResolver(request, d.resolver)}, true, nil
}

const costScanBatchRows = 512

// scanCostRows estimates every request row in [low, high) that the snapshot
// can see and hands it to add with its sanitized dimension row. Decoding the
// rows dominates the scan, so a large scan spreads it over a few workers while
// the calling goroutine keeps walking the cursor.
func scanCostRows(requests *bolt.Bucket, low, high int64, highWater uint64, resolver modelPriceResolver, workers int, add func(row costRow)) error {
	if workers <= 1 {
		decoder := newCostRowDecoder(highWater, resolver)
		return scanRequests(requests, low, high, func(unixNano int64, value []byte) error {
			row, ok, err := decoder.decode(unixNano, value)
			if err == nil && ok {
				add(row)
			}
			return err
		})
	}

	type rawRow struct {
		unixNano int64
		value    []byte
	}
	batches := make(chan []rawRow, workers)
	results := make(chan []costRow, workers)
	stop := make(chan struct{})
	var stopOnce sync.Once
	var failure error
	var decoding sync.WaitGroup
	for range workers {
		decoding.Add(1)
		go func() {
			defer decoding.Done()
			decoder := newCostRowDecoder(highWater, resolver)
			for batch := range batches {
				rows := make([]costRow, 0, len(batch))
				for _, raw := range batch {
					row, ok, err := decoder.decode(raw.unixNano, raw.value)
					if err != nil {
						stopOnce.Do(func() { failure = err; close(stop) })
						break
					}
					if ok {
						rows = append(rows, row)
					}
				}
				results <- rows
			}
		}()
	}
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		for rows := range results {
			for _, row := range rows {
				add(row)
			}
		}
	}()

	// Values stay valid until the read transaction ends, which outlives this
	// call, so batches reference them without copying.
	batch := make([]rawRow, 0, costScanBatchRows)
	send := func() bool {
		select {
		case batches <- batch:
			batch = make([]rawRow, 0, costScanBatchRows)
			return true
		case <-stop:
			return false
		}
	}
	errStopped := errors.New("cost scan stopped")
	err := scanRequests(requests, low, high, func(unixNano int64, value []byte) error {
		batch = append(batch, rawRow{unixNano: unixNano, value: value})
		if len(batch) == costScanBatchRows && !send() {
			return errStopped
		}
		return nil
	})
	if err == nil && len(batch) > 0 {
		send()
	}
	close(batches)
	decoding.Wait()
	close(results)
	<-collected
	if failure != nil {
		return failure
	}
	if errors.Is(err, errStopped) {
		return nil
	}
	return err
}

func (s *Store) scanCosts(snapshot costQuerySnapshot) (CostResponse, error) {
	queryRange := usageRange{Name: snapshot.Range, Start: snapshot.Start, End: snapshot.End}
	bucketSeconds := compactBucketSeconds(queryRange, snapshot.Since, snapshot.GeneratedAt)
	response := CostResponse{
		SchemaVersion:     1,
		GeneratedAt:       snapshot.GeneratedAt,
		Range:             snapshot.Range,
		Currency:          "USD",
		EstimateBasis:     "current_price_book",
		PriceBookRevision: snapshot.PriceRevision,
		BucketSeconds:     bucketSeconds,
	}
	resolver := newModelPriceResolver(snapshot.Prices, snapshot.PriceSettings)

	s.costScanMu.Lock()
	defer s.costScanMu.Unlock()
	memo := s.costMemo
	if memo == nil || memo.revision != snapshot.PriceRevision || memo.generation != snapshot.Generation {
		memo = newCostMemo(snapshot.PriceRevision, snapshot.Generation)
		s.costMemo = memo
	}
	// The caller holds stateMu, so the command is sent without taking it again.
	resp := make(chan costChangesResult, 1)
	s.commands <- costChangesCommand{since: memo.highWater, resp: resp}
	result := <-resp
	if result.err != nil {
		return CostResponse{}, result.err
	}
	changes := result.changes
	if changes.PriceRevision != snapshot.PriceRevision || changes.Generation != snapshot.Generation {
		// The price book or the data set changed after the snapshot was taken.
		// Answer from the snapshot alone and leave nothing behind for the next
		// query, which starts from the new state.
		memo = newCostMemo(snapshot.PriceRevision, snapshot.Generation)
		s.costMemo = nil
	} else {
		for _, minute := range changes.Minutes {
			if memo.isCovered(minute) {
				memo.stale[minute] = struct{}{}
			}
		}
		memo.prune(changes.PrunedBefore)
	}
	memo.highWater = changes.HighWater

	type modelKey struct{ Provider, Model string }
	type seriesKey struct {
		Bucket int64
		Model  int
	}
	var modelKeys []modelKey
	modelIDs := make(map[modelKey]int)
	var modelByDim []int
	var matched []uint8
	models := make(map[int]CostAmounts)
	series := make(map[seriesKey]CostAmounts)
	collect := func(minute int64, cell costCell) {
		for len(matched) < len(memo.dims) {
			dimensions := memo.dims[len(matched)]
			state := uint8(2)
			if snapshot.Filter.matches(dimensions) {
				state = 1
			}
			matched = append(matched, state)
			model := dimensions.Model
			if model == "" {
				model = "未标记模型"
			}
			key := modelKey{Provider: dimensions.Provider, Model: model}
			id, ok := modelIDs[key]
			if !ok {
				id = len(modelKeys)
				modelIDs[key] = id
				modelKeys = append(modelKeys, key)
			}
			modelByDim = append(modelByDim, id)
		}
		if matched[cell.dim] != 1 {
			return
		}
		model := modelByDim[cell.dim]
		sumCostAmounts(&response.Summary, cell.CostAmounts)
		modelAmounts := models[model]
		sumCostAmounts(&modelAmounts, cell.CostAmounts)
		models[model] = modelAmounts
		sKey := seriesKey{Bucket: compactBucket(minute, bucketSeconds), Model: model}
		seriesAmounts := series[sKey]
		sumCostAmounts(&seriesAmounts, cell.CostAmounts)
		series[sKey] = seriesAmounts
	}

	window := windowForRange(queryRange)
	err := s.db.View(func(tx *bolt.Tx) error {
		requests := tx.Bucket(requestsBucket)
		if requests == nil {
			return errors.New("requests bucket is missing")
		}
		// An open bound only has to reach as far as the request log does.
		cursor := requests.Cursor()
		if first, _ := cursor.First(); len(first) == 16 {
			window.from = max(window.from, requestAggregateKey(decodeInt64(first[:8]), Dimensions{}).Hour)
		}
		if last, _ := cursor.Last(); len(last) == 16 {
			window.to = min(window.to, requestAggregateKey(decodeInt64(last[:8]), Dimensions{}).Hour+60)
		} else {
			window.to = window.from
		}
		fill := func(from, to int64, workers int) error {
			return scanCostRows(requests, from*int64(time.Second), to*int64(time.Second), memo.highWater, resolver, workers, func(row costRow) {
				memo.minutes[row.minute] = addCostCell(memo.minutes[row.minute], memo.intern(row.dimensions), row.cost)
			})
		}
		if window.from < window.to {
			gaps := memo.gaps(window.from, window.to)
			for minute := range memo.stale {
				if minute < window.from || minute >= window.to {
					continue
				}
				delete(memo.minutes, minute)
				delete(memo.stale, minute)
				if err := fill(minute, minute+60, 1); err != nil {
					return err
				}
			}
			for _, gap := range gaps {
				// A gap of a few minutes is the common refresh; only a
				// first load is worth spreading over workers.
				workers := 1
				if gap[1]-gap[0] > 3600 {
					workers = min(runtime.GOMAXPROCS(0), maxCostScanWorkers)
				}
				if err := fill(gap[0], gap[1], workers); err != nil {
					return err
				}
			}
			memo.cover(window.from, window.to)
			for minute, cells := range memo.minutes {
				if minute < window.from || minute >= window.to {
					continue
				}
				for _, cell := range cells {
					collect(minute, cell)
				}
			}
		}
		// Partial minutes at the range bounds are estimated directly.
		for _, edge := range window.edges {
			err := scanCostRows(requests, edge[0].UnixNano(), edge[1].UnixNano(), memo.highWater, resolver, 1, func(row costRow) {
				cell := costCell{dim: memo.intern(row.dimensions)}
				addEstimatedCost(&cell.CostAmounts, row.cost)
				collect(row.minute, cell)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// A partly filled memo would under-count on the next query.
		s.costMemo = nil
		return CostResponse{}, fmt.Errorf("query estimated costs: %w", err)
	}

	response.Models = make([]ModelCostStats, 0, len(models))
	response.MissingPrices = make([]MissingPriceStats, 0)
	for id, amounts := range models {
		key := modelKeys[id]
		response.Models = append(response.Models, ModelCostStats{Provider: key.Provider, Model: key.Model, CostAmounts: amounts})
		if amounts.UnpricedRequests > 0 {
			response.MissingPrices = append(response.MissingPrices, MissingPriceStats{Provider: key.Provider, Model: key.Model, Requests: amounts.UnpricedRequests})
		}
	}
	sort.Slice(response.Models, func(i, j int) bool {
		if response.Models[i].TotalUSD != response.Models[j].TotalUSD {
			return response.Models[i].TotalUSD > response.Models[j].TotalUSD
		}
		if response.Models[i].Model != response.Models[j].Model {
			return response.Models[i].Model < response.Models[j].Model
		}
		return response.Models[i].Provider < response.Models[j].Provider
	})

	response.Series = make([]CostSeriesPoint, 0, len(series))
	for key, amounts := range series {
		response.Series = append(response.Series, CostSeriesPoint{
			Hour:        time.Unix(key.Bucket, 0).UTC().Format(time.RFC3339),
			Provider:    modelKeys[key.Model].Provider,
			Model:       modelKeys[key.Model].Model,
			CostAmounts: amounts,
		})
	}
	sort.Slice(response.Series, func(i, j int) bool {
		if response.Series[i].Hour != response.Series[j].Hour {
			return response.Series[i].Hour < response.Series[j].Hour
		}
		if response.Series[i].Model != response.Series[j].Model {
			return response.Series[i].Model < response.Series[j].Model
		}
		return response.Series[i].Provider < response.Series[j].Provider
	})

	sort.Slice(response.MissingPrices, func(i, j int) bool {
		if response.MissingPrices[i].Requests != response.MissingPrices[j].Requests {
			return response.MissingPrices[i].Requests > response.MissingPrices[j].Requests
		}
		if response.MissingPrices[i].Model != response.MissingPrices[j].Model {
			return response.MissingPrices[i].Model < response.MissingPrices[j].Model
		}
		return response.MissingPrices[i].Provider < response.MissingPrices[j].Provider
	})
	return response, nil
}

func sumCostAmounts(amounts *CostAmounts, other CostAmounts) {
	amounts.Requests += other.Requests
	amounts.PricedRequests += other.PricedRequests
	amounts.UnpricedRequests += other.UnpricedRequests
	amounts.InputUSD += other.InputUSD
	amounts.OutputUSD += other.OutputUSD
	amounts.CacheReadUSD += other.CacheReadUSD
	amounts.CacheCreationUSD += other.CacheCreationUSD
	amounts.TotalUSD += other.TotalUSD
}

func addEstimatedCost(amounts *CostAmounts, cost EstimatedCost) {
	amounts.Requests++
	if cost.Priced {
		amounts.PricedRequests++
	} else {
		amounts.UnpricedRequests++
	}
	amounts.InputUSD += cost.InputUSD
	amounts.OutputUSD += cost.OutputUSD
	amounts.CacheReadUSD += cost.CacheReadUSD
	amounts.CacheCreationUSD += cost.CacheCreationUSD
	amounts.TotalUSD += cost.TotalUSD
}

func estimateRequestCost(request RequestDetail, prices map[string]ModelPrice) EstimatedCost {
	return estimateRequestCostWithResolver(request, newModelPriceResolver(prices, defaultPriceSyncSettings()))
}

func estimateRequestCostWithResolver(request RequestDetail, resolver modelPriceResolver) EstimatedCost {
	model := request.Model
	if model == "" {
		model = "未标记模型"
	}
	price, ok := resolver.resolve(model)
	if !ok {
		// Prices are frequently maintained against the client-facing alias rather
		// than the upstream model name. Fall back to the alias before reporting
		// the request as unpriced.
		if alias := strings.TrimSpace(request.Alias); alias != "" {
			price, ok = resolver.resolve(alias)
		}
	}
	if !ok {
		return EstimatedCost{}
	}

	cacheRead := request.CacheReadTokens
	if cacheRead == 0 {
		cacheRead = request.CachedTokens
	}
	cacheCreation := request.CacheCreationTokens
	mode := price.AccountingMode
	if mode == "" {
		mode = defaultAccountingMode(request.Provider, request.ExecutorType)
	}

	billableInput := request.InputTokens
	contextTokens := request.InputTokens
	if mode == accountingModeInputIncludesCache {
		cacheTokens := saturatingAdd(cacheRead, cacheCreation)
		if billableInput > cacheTokens {
			billableInput -= cacheTokens
		} else {
			billableInput = 0
		}
		contextTokens = saturatingAdd(billableInput, cacheTokens)
	} else {
		contextTokens = saturatingAdd(request.InputTokens, saturatingAdd(cacheRead, cacheCreation))
	}

	rates := price.tokenRates()
	contextTiers := price.ContextTiers
	priceServiceTier := ""
	serviceTier := strings.ToLower(strings.TrimSpace(request.ServiceTier))
	if schedule, exists := price.ServiceTiers[serviceTier]; exists {
		rates = schedule.tokenRates()
		contextTiers = schedule.ContextTiers
		priceServiceTier = serviceTier
	}
	var selectedThreshold uint64
	for _, tier := range contextTiers {
		if contextTokens > tier.Threshold && tier.Threshold >= selectedThreshold {
			rates = tier.tokenRates()
			selectedThreshold = tier.Threshold
		}
	}

	result := EstimatedCost{
		Priced:                true,
		Source:                price.Source,
		AccountingMode:        mode,
		PriceServiceTier:      priceServiceTier,
		TierThreshold:         selectedThreshold,
		ContextTokens:         contextTokens,
		BillableInputTokens:   billableInput,
		BilledCacheReadTokens: cacheRead,
		InputUSD:              tokenCostUSD(billableInput, rates.Input),
		OutputUSD:             tokenCostUSD(request.OutputTokens, rates.Output),
		CacheReadUSD:          tokenCostUSD(cacheRead, rates.CacheRead),
		CacheCreationUSD:      tokenCostUSD(cacheCreation, rates.CacheCreation),
	}
	result.TotalUSD = result.InputUSD + result.OutputUSD + result.CacheReadUSD + result.CacheCreationUSD
	return result
}

func defaultAccountingMode(provider, executor string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	executor = strings.ToLower(strings.TrimSpace(executor))
	if provider == "anthropic" || executor == "claude" {
		return accountingModeInputExcludesCache
	}
	return accountingModeInputIncludesCache
}

func tokenCostUSD(tokens uint64, pricePerMillion float64) float64 {
	return float64(tokens) * pricePerMillion / 1_000_000
}
