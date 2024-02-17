package db

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.neds.sh/matty/entain/sports/proto/sports"
	"github.com/golang/protobuf/ptypes"
	_ "github.com/mattn/go-sqlite3"
)

// SportsRepo provides repository access to sports.
type SportsRepo interface {
	// Init will initialise our sports repository.
	Init() error

	// List will return a list of events.
	List(filter *sports.ListEventsRequestFilter) ([]*sports.Event, error)

	// Get will return a single event based on the given ID.
	Get(filter *sports.GetEventRequest) (*sports.Event, error)
}

type sportsRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewSportsRepo creates a new sports repository.
func NewSportsRepo(db *sql.DB) SportsRepo {
	return &sportsRepo{db: db}
}

// Init prepares the race repository dummy data.
func (r *sportsRepo) Init() error {
	var err error

	r.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy events.
		err = r.seed()
	})

	return err
}

func (r *sportsRepo) List(filter *sports.ListEventsRequestFilter) ([]*sports.Event, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getSportsQueries()[sportsList]

	query, args = r.applyFilter(query, filter)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanEvents(rows)
}

func (r *sportsRepo) Get(filter *sports.GetEventRequest) (*sports.Event, error) {
	var (
		err   error
		query string
	)

	query = getSportsQueries()[sportsList]

	if filter == nil {
		return nil, fmt.Errorf("error: no ID passed")
	}
	if filter != nil {
		// apply the ID filter
		query += " WHERE id=" + strconv.Itoa(int(filter.Id))
	}

	rows, err := r.db.Query(query, nil)
	if err != nil {
		return nil, err
	}

	events, err := r.scanEvents(rows)
	if err != nil {
		return nil, err
	}

	// since we are expecting only 1 row, so choose the first one
	return events[0], nil
}

func (r *sportsRepo) applyFilter(query string, filter *sports.ListEventsRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args
	}

	if len(filter.EventIds) > 0 {
		clauses = append(clauses, "event_id IN ("+strings.Repeat("?,", len(filter.EventIds)-1)+"?)")

		for _, eventID := range filter.EventIds {
			args = append(args, eventID)
		}
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	// add sort and order clause in the end
	if filter.SortBy != "" {
		// check for valid fields
		if filter.SortBy == "advertised_start_time" || filter.SortBy == "number" ||
			filter.SortBy == "event_id" || filter.SortBy == "name" || filter.SortBy == "sports_type" {
			sortCondition := " ORDER BY " + filter.SortBy
			query += sortCondition

			if filter.OrderBy == 1 {
				query += " DESC"
			} // note - we don't need to check for 0 since by default the sort order is ASC
		}
	}

	return query, args
}

func (m *sportsRepo) scanEvents(
	rows *sql.Rows,
) ([]*sports.Event, error) {
	var events []*sports.Event

	for rows.Next() {
		var event sports.Event
		var advertisedStart time.Time

		if err := rows.Scan(&event.Id, &event.EventId, &event.SportsType, &event.Name, &event.Number, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		ts, err := ptypes.TimestampProto(advertisedStart)
		if err != nil {
			return nil, err
		}

		event.AdvertisedStartTime = ts
		if event.AdvertisedStartTime.AsTime().Before(time.Now()) {
			event.Status = "CLOSED"
		} else {
			event.Status = "OPEN"
		}

		events = append(events, &event)
	}

	return events, nil
}
