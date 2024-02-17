package db

import (
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"

	"git.neds.sh/matty/entain/racing/proto/racing"
)

func TestGetRaceClosed(t *testing.T) {
	// Set up the mock database and get a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error creating mock database: %v", err)
	}
	defer db.Close()

	repo := &racesRepo{db: db}
	expectedTime := time.Date(2022, time.January, 1, 12, 0, 0, 0, time.UTC)

	expectedPTime, _ := ptypes.TimestampProto(expectedTime)
	t.Run("GetRaceByID", func(t *testing.T) {
		expectedRace := &racing.Race{
			Id:                  1,
			MeetingId:           10,
			Name:                "TestRace",
			Number:              123,
			Visible:             true,
			AdvertisedStartTime: expectedPTime,
			Status:              "CLOSED",
		}

		rows := sqlmock.NewRows([]string{"id", "meeting_id", "name", "number", "visible", "advertised_start_time"}).
			AddRow(
				expectedRace.Id,
				expectedRace.MeetingId,
				expectedRace.Name,
				expectedRace.Number,
				expectedRace.Visible,
				expectedTime,
			)

		mock.ExpectQuery("SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races WHERE id=1").WillReturnRows(rows)

		filter := &racing.GetRaceRequest{
			Id: int32(expectedRace.Id),
		}

		resultRace, err := repo.Get(filter)

		assert.NoError(t, err)
		assert.NotNil(t, resultRace)
		assert.Equal(t, expectedRace, resultRace)
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

	repo := &racesRepo{db: db}
	expectedTime := time.Date(2025, time.January, 1, 12, 0, 0, 0, time.UTC)

	expectedPTime, _ := ptypes.TimestampProto(expectedTime)
	t.Run("GetRaceByID", func(t *testing.T) {
		expectedRace := &racing.Race{
			Id:                  1,
			MeetingId:           10,
			Name:                "TestRace",
			Number:              123,
			Visible:             true,
			AdvertisedStartTime: expectedPTime,
			Status:              "OPEN",
		}

		rows := sqlmock.NewRows([]string{"id", "meeting_id", "name", "number", "visible", "advertised_start_time"}).
			AddRow(
				expectedRace.Id,
				expectedRace.MeetingId,
				expectedRace.Name,
				expectedRace.Number,
				expectedRace.Visible,
				expectedTime,
			)

		mock.ExpectQuery("SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races WHERE id=1").WillReturnRows(rows)

		filter := &racing.GetRaceRequest{
			Id: int32(expectedRace.Id),
		}

		resultRace, err := repo.Get(filter)

		assert.NoError(t, err)
		assert.NotNil(t, resultRace)
		assert.Equal(t, expectedRace, resultRace)
	})

	// Add more test cases as needed

	// Assert that the expected queries were executed
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestApplyFilter(t *testing.T) {
	tests := []struct {
		name   string
		filter *racing.ListRacesRequestFilter
		query  string
		args   []interface{}
	}{
		{
			name:   "NoFilter",
			filter: nil,
			query:  "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races",
			args:   nil,
		},
		{
			name: "FilterWithEventIds",
			filter: &racing.ListRacesRequestFilter{
				MeetingIds: []int64{1, 2, 3},
			},
			query: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races WHERE meeting_id IN (?,?,?)",
			args:  []interface{}{int64(1), int64(2), int64(3)},
		},
		{
			name: "FilterWithSortAndOrder",
			filter: &racing.ListRacesRequestFilter{
				SortBy:  "number",
				OrderBy: 1,
			},
			query: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races ORDER BY number DESC",
			args:  nil,
		},
		{
			name: "InvalidSortField",
			filter: &racing.ListRacesRequestFilter{
				SortBy: "invalid_field",
			},
			query: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races",
			args:  nil,
		},
		{
			name: "FilterWithSortAndDefaultOrder",
			filter: &racing.ListRacesRequestFilter{
				SortBy: "name",
			},
			query: "SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races ORDER BY name",
			args:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &racesRepo{}
			resultQuery, resultArgs := repo.applyFilter("SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races", tt.filter)

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

func TestScanRaceOpen(t *testing.T) {
	// Set up the mock database and get a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error creating mock database: %v", err)
	}
	defer db.Close()

	// Define columns and rows for the mock
	columns := []string{"id", "meeting_id", "name", "number", "visible", "advertised_start_time"}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(columns).AddRow(1, 5, "F1 Race", 10, true, time.Now().Add(time.Hour)))

	repo := &racesRepo{}
	rows, err := db.Query("SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races")
	assert.NoError(t, err)

	t.Run("ScanValidRace", func(t *testing.T) {
		races, err := repo.scanRaces(rows)

		assert.NoError(t, err)
		assert.Len(t, races, 1)

		// Add assertions for the event properties
		assert.Equal(t, "OPEN", races[0].Status)
	})

	// Assert that the expected queries were executed
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestScanRaceClosed(t *testing.T) {
	// Set up the mock database and get a mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error creating mock database: %v", err)
	}
	defer db.Close()

	// Define columns and rows for the mock
	columns := []string{"id", "meeting_id", "name", "number", "visible", "advertised_start_time"}
	mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows(columns).AddRow(1, 5, "F1 Race", 10, true, time.Now().Add(-time.Hour)))

	repo := &racesRepo{}
	rows, err := db.Query("SELECT id, meeting_id, name, number, visible, advertised_start_time FROM races")
	assert.NoError(t, err)

	t.Run("ScanValidRace", func(t *testing.T) {
		races, err := repo.scanRaces(rows)

		assert.NoError(t, err)
		assert.Len(t, races, 1)

		// Add assertions for the event properties
		assert.Equal(t, "CLOSED", races[0].Status)
	})

	// Assert that the expected queries were executed
	assert.NoError(t, mock.ExpectationsWereMet())
}
