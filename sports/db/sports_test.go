package db

import (
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"

	"git.neds.sh/matty/entain/sports/proto/sports"
)

func TestGetRaceClosed(t *testing.T) {
	// Set up the mock database and get a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error creating mock database: %v", err)
	}
	defer db.Close()

	repo := &sportsRepo{db: db}
	expectedTime := time.Date(2022, time.January, 1, 12, 0, 0, 0, time.UTC)

	expectedPTime, _ := ptypes.TimestampProto(expectedTime)
	t.Run("GetEventByID", func(t *testing.T) {
		expectedEvent := &sports.Event{
			Id:                  1,
			EventId:             10,
			SportsType:          "Cricket",
			Name:                "India vs Australia",
			Number:              10,
			AdvertisedStartTime: expectedPTime,
			Status:              "CLOSED",
		}

		rows := sqlmock.NewRows([]string{"id", "event_id", "sports_type", "name", "number", "advertised_start_time"}).
			AddRow(
				expectedEvent.Id,
				expectedEvent.EventId,
				expectedEvent.SportsType,
				expectedEvent.Name,
				expectedEvent.Number,
				expectedTime,
			)

		mock.ExpectQuery("SELECT id, event_id, sports_type, name, number, advertised_start_time FROM sports WHERE id=1").WillReturnRows(rows)

		filter := &sports.GetEventRequest{
			Id: int32(expectedEvent.Id),
		}

		resultEvent, err := repo.Get(filter)

		assert.NoError(t, err)
		assert.NotNil(t, resultEvent)
		assert.Equal(t, expectedEvent, resultEvent)
	})

	// Add more test cases as needed

	// Assert that the expected queries were executed
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetRaceOpen(t *testing.T) {
	// Set up the mock database and get a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error creating mock database: %v", err)
	}
	defer db.Close()

	repo := &sportsRepo{db: db}
	expectedTime := time.Date(2025, time.January, 1, 12, 0, 0, 0, time.UTC)

	expectedPTime, _ := ptypes.TimestampProto(expectedTime)
	t.Run("GetEventByID", func(t *testing.T) {
		expectedEvent := &sports.Event{
			Id:                  1,
			EventId:             10,
			SportsType:          "Cricket",
			Name:                "India vs Australia",
			Number:              10,
			AdvertisedStartTime: expectedPTime,
			Status:              "OPEN",
		}

		rows := sqlmock.NewRows([]string{"id", "event_id", "sports_type", "name", "number", "advertised_start_time"}).
			AddRow(
				expectedEvent.Id,
				expectedEvent.EventId,
				expectedEvent.SportsType,
				expectedEvent.Name,
				expectedEvent.Number,
				expectedTime,
			)

		mock.ExpectQuery("SELECT id, event_id, sports_type, name, number, advertised_start_time FROM sports WHERE id=1").WillReturnRows(rows)

		filter := &sports.GetEventRequest{
			Id: int32(expectedEvent.Id),
		}

		resultEvent, err := repo.Get(filter)

		assert.NoError(t, err)
		assert.NotNil(t, resultEvent)
		assert.Equal(t, expectedEvent, resultEvent)
	})

	// Add more test cases as needed

	// Assert that the expected queries were executed
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestApplyFilter(t *testing.T) {
	tests := []struct {
		name   string
		filter *sports.ListEventsRequestFilter
		query  string
		args   []interface{}
	}{
		{
			name:   "NoFilter",
			filter: nil,
			query:  "SELECT id, event_id, sports_type, name, number, advertised_start_time FROM sports",
			args:   nil,
		},
		{
			name: "FilterWithEventIds",
			filter: &sports.ListEventsRequestFilter{
				EventIds: []int64{1, 2, 3},
			},
			query: "SELECT id, event_id, sports_type, name, number, advertised_start_time FROM sports WHERE event_id IN (?,?,?)",
			args:  []interface{}{int64(1), int64(2), int64(3)},
		},
		{
			name: "FilterWithSortAndOrder",
			filter: &sports.ListEventsRequestFilter{
				SortBy:  "number",
				OrderBy: 1,
			},
			query: "SELECT id, event_id, sports_type, name, number, advertised_start_time FROM sports ORDER BY number DESC",
			args:  nil,
		},
		{
			name: "InvalidSortField",
			filter: &sports.ListEventsRequestFilter{
				SortBy: "invalid_field",
			},
			query: "SELECT id, event_id, sports_type, name, number, advertised_start_time FROM sports",
			args:  nil,
		},
		{
			name: "FilterWithSortAndDefaultOrder",
			filter: &sports.ListEventsRequestFilter{
				SortBy: "name",
			},
			query: "SELECT id, event_id, sports_type, name, number, advertised_start_time FROM sports ORDER BY name",
			args:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &sportsRepo{}
			resultQuery, resultArgs := repo.applyFilter("SELECT id, event_id, sports_type, name, number, advertised_start_time FROM sports", tt.filter)

			if resultQuery != tt.query {
				t.Errorf("Query mismatch. Expected: %s, Got: %s", tt.query, resultQuery)
			}

			if !equal(resultArgs, tt.args) {
				t.Errorf("Arguments mismatch. Expected: %v, Got: %v", tt.args, resultArgs)
			}
		})
	}
}

func equal(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestScanEventsOpen(t *testing.T) {
	// Set up the mock database and get a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error creating mock database: %v", err)
	}
	defer db.Close()

	// Define columns and rows for the mock
	columns := []string{"id", "event_id", "sports_type", "name", "number", "advertised_start_time"}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(columns).AddRow(1, 5, "Football", "Match 1", 10, time.Now().Add(time.Hour)))

	repo := &sportsRepo{}
	rows, err := db.Query("SELECT id, event_id, sports_type, name, number, advertised_start_time FROM sports")
	assert.NoError(t, err)

	t.Run("ScanValidEvent", func(t *testing.T) {
		events, err := repo.scanEvents(rows)

		assert.NoError(t, err)
		assert.Len(t, events, 1)

		// Add assertions for the event properties
		assert.Equal(t, "OPEN", events[0].Status)
	})

	// Assert that the expected queries were executed
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestScanEventsClosed(t *testing.T) {
	// Set up the mock database and get a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error creating mock database: %v", err)
	}
	defer db.Close()

	// Define columns and rows for the mock
	columns := []string{"id", "event_id", "sports_type", "name", "number", "advertised_start_time"}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(columns).AddRow(1, 5, "Football", "Match 1", 10, time.Now().Add(-time.Hour)))

	repo := &sportsRepo{}
	rows, err := db.Query("SELECT id, event_id, sports_type, name, number, advertised_start_time FROM sports")
	assert.NoError(t, err)

	t.Run("ScanValidEvent", func(t *testing.T) {
		events, err := repo.scanEvents(rows)

		assert.NoError(t, err)
		assert.Len(t, events, 1)

		// Add assertions for the event properties
		assert.Equal(t, "CLOSED", events[0].Status)
	})

	// Assert that the expected queries were executed
	assert.NoError(t, mock.ExpectationsWereMet())
}
