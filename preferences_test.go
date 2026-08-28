package main

import (
	"strings"
	"testing"
)

func TestDashboardPreferencesAllowCacheHitRateColumn(t *testing.T) {
	preferences, err := normalizeDashboardPreferences(DashboardPreferences{
		RequestPageSize:        10,
		DimensionPageSize:      10,
		HiddenRequestColumns:   []string{"cache_hit_rate"},
		HiddenDimensionColumns: []string{},
		TimeRangeMode:          "custom",
	})
	if err != nil {
		t.Fatalf("cache hit rate column rejected by preferences: %v", err)
	}
	if len(preferences.HiddenRequestColumns) != 1 || preferences.HiddenRequestColumns[0] != "cache_hit_rate" {
		t.Fatalf("cache hit rate column was not retained: %+v", preferences)
	}
}

// TestDashboardTimeRangeModesMatchDashboardScript keeps the server-side whitelist
// and the dashboard's own whitelist in step. They drifted apart once when the
// "all" preset was added only on the client, which made every preference save
// fail with "time_range_mode is unsupported".
func TestDashboardTimeRangeModesMatchDashboardScript(t *testing.T) {
	const marker = "var modes={"
	start := strings.Index(dashboardHTML, marker)
	if start < 0 {
		t.Fatal("dashboard script no longer declares a time range mode whitelist")
	}
	start += len(marker)
	end := strings.Index(dashboardHTML[start:], "}")
	if end < 0 {
		t.Fatal("time range mode whitelist is not terminated")
	}
	scriptModes := map[string]struct{}{}
	for _, entry := range strings.Split(dashboardHTML[start:start+end], ",") {
		name, _, ok := strings.Cut(entry, ":")
		if !ok {
			continue
		}
		scriptModes[strings.TrimSpace(name)] = struct{}{}
	}
	if len(scriptModes) == 0 {
		t.Fatal("could not parse the dashboard time range mode whitelist")
	}
	for mode := range scriptModes {
		if _, ok := dashboardTimeRangeModes[mode]; !ok {
			t.Fatalf("dashboard offers time range mode %q that the server rejects", mode)
		}
		if _, err := normalizeDashboardPreferences(DashboardPreferences{RequestPageSize: 100, DimensionPageSize: 100, TimeRangeMode: mode}); err != nil {
			t.Fatalf("time range mode %q is rejected: %v", mode, err)
		}
	}
	for mode := range dashboardTimeRangeModes {
		if _, ok := scriptModes[mode]; !ok {
			t.Fatalf("server accepts time range mode %q that the dashboard never offers", mode)
		}
	}
}

// The granularity select is the only place the option values are declared, so a
// new one there has to be accepted by the server or saving it 400s and the
// reader silently loses the choice on the next load.
func TestDashboardGranularitiesMatchDashboardSelect(t *testing.T) {
	const marker = `<select id="granularity"`
	start := strings.Index(dashboardHTML, marker)
	if start < 0 {
		t.Fatal("dashboard no longer declares a granularity select")
	}
	end := strings.Index(dashboardHTML[start:], "</select>")
	if end < 0 {
		t.Fatal("granularity select is not terminated")
	}
	markup := dashboardHTML[start : start+end]
	options := map[string]struct{}{}
	for _, fragment := range strings.Split(markup, `<option value="`)[1:] {
		value, _, ok := strings.Cut(fragment, `"`)
		if !ok {
			continue
		}
		options[value] = struct{}{}
	}
	if len(options) == 0 {
		t.Fatal("could not parse the granularity select options")
	}
	for value := range options {
		if _, ok := dashboardGranularities[value]; !ok {
			t.Fatalf("dashboard offers granularity %q that the server rejects", value)
		}
		if _, err := normalizeDashboardPreferences(DashboardPreferences{RequestPageSize: 100, DimensionPageSize: 100, Granularity: value}); err != nil {
			t.Fatalf("granularity %q is rejected: %v", value, err)
		}
	}
	for value := range dashboardGranularities {
		if _, ok := options[value]; !ok {
			t.Fatalf("server accepts granularity %q that the dashboard cannot offer", value)
		}
	}
	if _, err := normalizeDashboardPreferences(DashboardPreferences{RequestPageSize: 100, DimensionPageSize: 100, Granularity: "fortnight"}); err == nil {
		t.Fatal("an unsupported granularity must be rejected")
	}
	// An unset granularity means "follow the range", not an error.
	if _, err := normalizeDashboardPreferences(DashboardPreferences{RequestPageSize: 100, DimensionPageSize: 100}); err != nil {
		t.Fatalf("an unset granularity must be accepted: %v", err)
	}
}
