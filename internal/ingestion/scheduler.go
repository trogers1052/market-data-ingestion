package ingestion

import (
	"log"
	"time"
)

// MarketScheduler handles market hours awareness
type MarketScheduler struct {
	marketOpenHour   int  // Hour in ET (e.g., 4 for pre-market, 9 for regular)
	marketCloseHour  int  // Hour in ET (e.g., 20 for after-hours, 16 for regular)
	enablePreMarket  bool // Include 4am-9:30am
	enableAfterHours bool // Include 4pm-8pm
	location         *time.Location
}

// NewMarketScheduler creates a new market scheduler
func NewMarketScheduler(openHour, closeHour int, preMarket, afterHours bool) *MarketScheduler {
	// Load Eastern Time zone
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Printf("Warning: could not load America/New_York timezone, using UTC: %v", err)
		loc = time.UTC
	}

	return &MarketScheduler{
		marketOpenHour:   openHour,
		marketCloseHour:  closeHour,
		enablePreMarket:  preMarket,
		enableAfterHours: afterHours,
		location:         loc,
	}
}

// IsMarketHours returns true if the current time is within market hours
func (s *MarketScheduler) IsMarketHours() bool {
	now := time.Now().In(s.location)
	return s.isMarketHoursAt(now)
}

// isMarketHoursAt checks if a specific time is within market hours
func (s *MarketScheduler) isMarketHoursAt(t time.Time) bool {
	// Check if it's a weekday
	weekday := t.Weekday()
	if weekday == time.Saturday || weekday == time.Sunday {
		return false
	}

	// Check if it's a market holiday (simplified - you could add a full holiday calendar)
	if s.isMarketHoliday(t) {
		return false
	}

	// Check if within trading hours
	hour := t.Hour()
	minute := t.Minute()
	currentMinutes := hour*60 + minute

	openMinutes := s.marketOpenHour * 60
	closeMinutes := s.marketCloseHour * 60

	return currentMinutes >= openMinutes && currentMinutes < closeMinutes
}

// isMarketHoliday checks if a date is a US market holiday
func (s *MarketScheduler) isMarketHoliday(t time.Time) bool {
	// Major US market holidays (simplified)
	// A production system should use a proper holiday calendar
	year := t.Year()
	month := t.Month()
	day := t.Day()

	// Fixed holidays
	holidays := map[string]bool{
		// New Year's Day
		"01-01": true,
		// Juneteenth (June 19)
		"06-19": true,
		// Independence Day (July 4)
		"07-04": true,
		// Christmas (Dec 25)
		"12-25": true,
	}

	dateKey := t.Format("01-02")
	if holidays[dateKey] {
		return true
	}

	// Variable holidays (approximate - should use proper calculation)
	// MLK Day - 3rd Monday of January
	if month == time.January && t.Weekday() == time.Monday && day >= 15 && day <= 21 {
		return true
	}
	// Presidents Day - 3rd Monday of February
	if month == time.February && t.Weekday() == time.Monday && day >= 15 && day <= 21 {
		return true
	}
	// Memorial Day - Last Monday of May
	if month == time.May && t.Weekday() == time.Monday && day >= 25 {
		return true
	}
	// Labor Day - 1st Monday of September
	if month == time.September && t.Weekday() == time.Monday && day <= 7 {
		return true
	}
	// Thanksgiving - 4th Thursday of November
	if month == time.November && t.Weekday() == time.Thursday && day >= 22 && day <= 28 {
		return true
	}

	// Good Friday (approximate - 2 days before Easter)
	// This is a simplification; proper Easter calculation is complex
	goodFridays := map[int]string{
		2024: "03-29",
		2025: "04-18",
		2026: "04-03",
		2027: "03-26",
		2028: "04-14",
		2029: "03-30",
		2030: "04-19",
	}
	if gf, ok := goodFridays[year]; ok && dateKey == gf {
		return true
	}

	return false
}

// GetSleepDuration returns how long to sleep until the next market open
func (s *MarketScheduler) GetSleepDuration() time.Duration {
	now := time.Now().In(s.location)

	// If market is open, return poll interval
	if s.isMarketHoursAt(now) {
		return time.Minute
	}

	// Find next market open
	nextOpen := s.getNextMarketOpen(now)

	duration := nextOpen.Sub(now)
	if duration < 0 {
		duration = time.Minute // Fallback
	}

	return duration
}

// getNextMarketOpen finds the next market open time
func (s *MarketScheduler) getNextMarketOpen(from time.Time) time.Time {
	// Start with today's open time
	openTime := time.Date(
		from.Year(), from.Month(), from.Day(),
		s.marketOpenHour, 0, 0, 0,
		s.location,
	)

	// If we're past today's open, move to tomorrow
	if from.After(openTime) || from.Equal(openTime) {
		openTime = openTime.AddDate(0, 0, 1)
	}

	// Skip weekends and holidays
	for {
		weekday := openTime.Weekday()
		if weekday != time.Saturday && weekday != time.Sunday && !s.isMarketHoliday(openTime) {
			break
		}
		openTime = openTime.AddDate(0, 0, 1)
	}

	return openTime
}

// GetStatus returns the current market status
func (s *MarketScheduler) GetStatus() map[string]interface{} {
	now := time.Now().In(s.location)

	status := map[string]interface{}{
		"current_time_et":   now.Format("2006-01-02 15:04:05 MST"),
		"day_of_week":       now.Weekday().String(),
		"is_market_hours":   s.IsMarketHours(),
		"market_open_hour":  s.marketOpenHour,
		"market_close_hour": s.marketCloseHour,
	}

	if !s.IsMarketHours() {
		nextOpen := s.getNextMarketOpen(now)
		status["next_market_open"] = nextOpen.Format("2006-01-02 15:04:05 MST")
		status["hours_until_open"] = nextOpen.Sub(now).Hours()
	}

	return status
}

// LogStatus logs the current market status
func (s *MarketScheduler) LogStatus() {
	status := s.GetStatus()
	log.Printf("Market Status: %v", status)
}

// GetTradingDaysInRange returns the number of trading days between two dates
func (s *MarketScheduler) GetTradingDaysInRange(from, to time.Time) int {
	count := 0
	current := from.In(s.location)
	end := to.In(s.location)

	for current.Before(end) {
		weekday := current.Weekday()
		if weekday != time.Saturday && weekday != time.Sunday && !s.isMarketHoliday(current) {
			count++
		}
		current = current.AddDate(0, 0, 1)
	}

	return count
}
