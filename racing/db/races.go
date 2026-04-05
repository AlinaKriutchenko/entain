package db

import (
	"database/sql"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	_ "github.com/mattn/go-sqlite3"

	"git.neds.sh/matty/entain/racing/proto/racing"
)

// RacesRepo provides repository access to races.
type RacesRepo interface {
	// Init will initialise our races repository.
	Init() error

	// List will return a list of races.
	List(filter *racing.ListRacesRequestFilter, orderBy string) ([]*racing.Race, error)

	// Get will return a single race by ID.
	Get(id int64) (*racing.Race, error)
}

type racesRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewRacesRepo creates a new races repository.
func NewRacesRepo(db *sql.DB) RacesRepo {
	return &racesRepo{db: db}
}

// Init prepares the race repository dummy data.
func (r *racesRepo) Init() error {
	var err error

	r.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy races.
		err = r.seed()
	})

	return err
}

func (r *racesRepo) List(filter *racing.ListRacesRequestFilter, orderBy string) ([]*racing.Race, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getRaceQueries()[racesList]

	query, args = r.applyFilter(query, filter)
	query = r.applyOrder(query, orderBy)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanRaces(rows)
}

// allowedOrderByFields limits sorting to known columns to prevent SQL injection.
var allowedOrderByFields = map[string]bool{
	"advertised_start_time": true,
	"id":                    true,
	"name":                  true,
	"number":                true,
}

// applyOrder adds ORDER BY to the query, falling back to advertised_start_time if not specified.
// datetime() is used for advertised_start_time to ensure correct ordering regardless of timezone format.
func (r *racesRepo) applyOrder(query, orderBy string) string {
	if orderBy == "" || !allowedOrderByFields[orderBy] {
		return query + " ORDER BY datetime(advertised_start_time) ASC"
	}
	if orderBy == "advertised_start_time" {
		return query + " ORDER BY datetime(advertised_start_time) ASC"
	}
	return query + " ORDER BY " + orderBy + " ASC"
}

func (r *racesRepo) applyFilter(query string, filter *racing.ListRacesRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args
	}

	if len(filter.MeetingIds) > 0 {
		clauses = append(clauses, "meeting_id IN ("+strings.Repeat("?,", len(filter.MeetingIds)-1)+"?)")

		for _, meetingID := range filter.MeetingIds {
			args = append(args, meetingID)
		}
	}

	// Only filter by visibility when the caller explicitly sets only_visible=true.
	// Omitting the field (or setting it to false) returns all races.
	if filter.OnlyVisible != nil && *filter.OnlyVisible {
		clauses = append(clauses, "visible = 1")
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	return query, args
}

func (r *racesRepo) Get(id int64) (*racing.Race, error) {
	query := getRaceQueries()[racesList] + " WHERE id = ?"

	rows, err := r.db.Query(query, id)
	if err != nil {
		return nil, err
	}

	races, err := r.scanRaces(rows)
	if err != nil {
		return nil, err
	}

	if len(races) == 0 {
		return nil, nil
	}

	return races[0], nil
}

func (m *racesRepo) scanRaces(
	rows *sql.Rows,
) ([]*racing.Race, error) {
	var races []*racing.Race

	for rows.Next() {
		var race racing.Race
		var advertisedStart time.Time

		if err := rows.Scan(&race.Id, &race.MeetingId, &race.Name, &race.Number, &race.Visible, &advertisedStart); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}

			return nil, err
		}

		ts, err := ptypes.TimestampProto(advertisedStart)
		if err != nil {
			return nil, err
		}

		race.AdvertisedStartTime = ts

		// set status based on whether the race has started
		if advertisedStart.Before(time.Now()) {
			race.Status = racing.RaceStatus_RACE_STATUS_CLOSED
		} else {
			race.Status = racing.RaceStatus_RACE_STATUS_OPEN
		}

		races = append(races, &race)
	}

	return races, nil
}
