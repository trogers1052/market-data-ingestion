package ingestion

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func et(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	return loc
}

func TestNewMarketScheduler(t *testing.T) {
	s := NewMarketScheduler(4, 20, true, true)
	assert.Equal(t, 4, s.marketOpenHour)
	assert.Equal(t, 20, s.marketCloseHour)
	assert.True(t, s.enablePreMarket)
	assert.True(t, s.enableAfterHours)
	assert.NotNil(t, s.location)
}

func TestIsMarketHoursAt(t *testing.T) {
	loc := et(t)
	s := NewMarketScheduler(9, 16, false, false)

	tests := []struct {
		name string
		when time.Time
		want bool
	}{
		// Wednesday 2026-01-07 (not a holiday)
		{"weekday open", time.Date(2026, 1, 7, 10, 0, 0, 0, loc), true},
		{"weekday just before close", time.Date(2026, 1, 7, 15, 59, 0, 0, loc), true},
		{"weekday at close", time.Date(2026, 1, 7, 16, 0, 0, 0, loc), false},
		{"weekday before open", time.Date(2026, 1, 7, 8, 59, 0, 0, loc), false},
		{"weekday at open", time.Date(2026, 1, 7, 9, 0, 0, 0, loc), true},
		// Saturday / Sunday
		{"saturday", time.Date(2026, 1, 10, 10, 0, 0, 0, loc), false},
		{"sunday", time.Date(2026, 1, 11, 10, 0, 0, 0, loc), false},
		// New Year's Day (Thursday 2026-01-01) holiday
		{"new years day", time.Date(2026, 1, 1, 10, 0, 0, 0, loc), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, s.isMarketHoursAt(tt.when))
		})
	}
}

func TestIsMarketHoliday(t *testing.T) {
	loc := et(t)
	s := NewMarketScheduler(9, 16, false, false)

	holidays := []struct {
		name string
		when time.Time
	}{
		{"new years", time.Date(2026, 1, 1, 12, 0, 0, 0, loc)},
		{"juneteenth", time.Date(2026, 6, 19, 12, 0, 0, 0, loc)},
		{"independence", time.Date(2026, 7, 4, 12, 0, 0, 0, loc)},
		{"christmas", time.Date(2026, 12, 25, 12, 0, 0, 0, loc)},
		// MLK Day: 3rd Monday Jan 2026 = Jan 19
		{"mlk", time.Date(2026, 1, 19, 12, 0, 0, 0, loc)},
		// Presidents Day: 3rd Monday Feb 2026 = Feb 16
		{"presidents", time.Date(2026, 2, 16, 12, 0, 0, 0, loc)},
		// Memorial Day: last Monday May 2026 = May 25
		{"memorial", time.Date(2026, 5, 25, 12, 0, 0, 0, loc)},
		// Labor Day: 1st Monday Sep 2026 = Sep 7
		{"labor", time.Date(2026, 9, 7, 12, 0, 0, 0, loc)},
		// Thanksgiving: 4th Thursday Nov 2026 = Nov 26
		{"thanksgiving", time.Date(2026, 11, 26, 12, 0, 0, 0, loc)},
		// Good Friday 2026 = April 3
		{"good friday", time.Date(2026, 4, 3, 12, 0, 0, 0, loc)},
	}
	for _, h := range holidays {
		t.Run(h.name, func(t *testing.T) {
			assert.True(t, s.isMarketHoliday(h.when), "expected holiday")
		})
	}

	// Non-holidays
	assert.False(t, s.isMarketHoliday(time.Date(2026, 1, 7, 12, 0, 0, 0, loc)))
	assert.False(t, s.isMarketHoliday(time.Date(2026, 4, 4, 12, 0, 0, 0, loc)))
}

func TestGetNextMarketOpen(t *testing.T) {
	loc := et(t)
	s := NewMarketScheduler(9, 16, false, false)

	// From Wednesday before open -> same day open
	from := time.Date(2026, 1, 7, 6, 0, 0, 0, loc)
	next := s.getNextMarketOpen(from)
	assert.Equal(t, time.Date(2026, 1, 7, 9, 0, 0, 0, loc), next)

	// From Wednesday after open -> next day (Thursday) open
	from = time.Date(2026, 1, 7, 12, 0, 0, 0, loc)
	next = s.getNextMarketOpen(from)
	assert.Equal(t, time.Date(2026, 1, 8, 9, 0, 0, 0, loc), next)

	// From Friday after open -> Monday open (skip weekend)
	from = time.Date(2026, 1, 9, 12, 0, 0, 0, loc)
	next = s.getNextMarketOpen(from)
	assert.Equal(t, time.Date(2026, 1, 12, 9, 0, 0, 0, loc), next)
}

func TestGetSleepDuration(t *testing.T) {
	s := NewMarketScheduler(9, 16, false, false)
	// Always returns a positive duration regardless of current wall-clock time.
	d := s.GetSleepDuration()
	assert.Greater(t, d, time.Duration(0))
}

func TestGetTradingDaysInRange(t *testing.T) {
	loc := et(t)
	s := NewMarketScheduler(9, 16, false, false)

	// Mon 2026-01-05 to Sat 2026-01-10 (exclusive end).
	// Trading days: Mon 5, Tue 6, Wed 7, Thu 8, Fri 9 = 5
	from := time.Date(2026, 1, 5, 0, 0, 0, 0, loc)
	to := time.Date(2026, 1, 10, 0, 0, 0, 0, loc)
	assert.Equal(t, 5, s.GetTradingDaysInRange(from, to))

	// Range that includes MLK day (Jan 19, Mon): Jan 19 -> Jan 24
	// Mon 19 (holiday, skip), Tue 20, Wed 21, Thu 22, Fri 23 = 4
	from = time.Date(2026, 1, 19, 0, 0, 0, 0, loc)
	to = time.Date(2026, 1, 24, 0, 0, 0, 0, loc)
	assert.Equal(t, 4, s.GetTradingDaysInRange(from, to))

	// Empty range
	assert.Equal(t, 0, s.GetTradingDaysInRange(to, from))
}

func TestGetStatusAndLog(t *testing.T) {
	s := NewMarketScheduler(9, 16, false, false)
	status := s.GetStatus()
	assert.Contains(t, status, "current_time_et")
	assert.Contains(t, status, "day_of_week")
	assert.Contains(t, status, "is_market_hours")
	assert.Equal(t, 9, status["market_open_hour"])
	assert.Equal(t, 16, status["market_close_hour"])
	// Does not panic
	s.LogStatus()
	// IsMarketHours uses real time; just exercise it.
	_ = s.IsMarketHours()
}

func TestNewMarketScheduler_FallbackTimezone(t *testing.T) {
	// We can't easily force LoadLocation to fail, but at minimum confirm a
	// scheduler constructed normally has a usable location.
	s := NewMarketScheduler(9, 16, false, false)
	require.NotNil(t, s.location)
}
