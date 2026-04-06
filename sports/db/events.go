package db

import (
	"database/sql"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	_ "github.com/mattn/go-sqlite3"

	"git.neds.sh/matty/entain/sports/proto/sports"
)

// EventsRepo provides repository access to sport events.
type EventsRepo interface {
	// Init will initialise our events repository.
	Init() error

	// List will return a list of sport events.
	List(filter *sports.ListEventsRequestFilter, orderBy string) ([]*sports.SportEvent, error)
}

type eventsRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewEventsRepo creates a new events repository.
func NewEventsRepo(db *sql.DB) EventsRepo {
	return &eventsRepo{db: db}
}

// Init prepares the events repository with dummy data.
func (r *eventsRepo) Init() error {
	var err error

	r.init.Do(func() {
		err = r.seed()
	})

	return err
}

func (r *eventsRepo) List(filter *sports.ListEventsRequestFilter, orderBy string) ([]*sports.SportEvent, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getEventQueries()[eventsList]

	query, args = r.applyFilter(query, filter)
	query = r.applyOrder(query, orderBy)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanEvents(rows)
}

// allowedOrderByFields limits sorting to known columns to prevent SQL injection.
var allowedOrderByFields = map[string]bool{
	"advertised_start_time": true,
	"id":                    true,
	"name":                  true,
}

// applyOrder adds ORDER BY to the query, defaulting to advertised_start_time.
func (r *eventsRepo) applyOrder(query, orderBy string) string {
	if orderBy == "" || !allowedOrderByFields[orderBy] {
		return query + " ORDER BY datetime(advertised_start_time) ASC"
	}
	if orderBy == "advertised_start_time" {
		return query + " ORDER BY datetime(advertised_start_time) ASC"
	}
	return query + " ORDER BY " + orderBy + " ASC"
}

func (r *eventsRepo) applyFilter(query string, filter *sports.ListEventsRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args
	}

	// if only_visible is true, filter to visible events only
	if filter.OnlyVisible != nil && *filter.OnlyVisible {
		clauses = append(clauses, "visible = 1")
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	return query, args
}

func (r *eventsRepo) scanEvents(rows *sql.Rows) ([]*sports.SportEvent, error) {
	var events []*sports.SportEvent

	for rows.Next() {
		var event sports.SportEvent
		var advertisedStart time.Time

		if err := rows.Scan(&event.Id, &event.Name, &event.SportType, &event.Visible, &advertisedStart); err != nil {
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

		if advertisedStart.Before(time.Now()) {
			event.Status = sports.EventStatus_EVENT_STATUS_CLOSED
		} else {
			event.Status = sports.EventStatus_EVENT_STATUS_OPEN
		}

		events = append(events, &event)
	}

	return events, nil
}
