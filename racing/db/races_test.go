package db

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"git.neds.sh/matty/entain/racing/proto/racing"
	"google.golang.org/protobuf/proto"
)

// newTestRepo creates an in-memory SQLite DB and seeds it for testing.
func newTestRepo(t *testing.T) RacesRepo {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}

	repo := NewRacesRepo(db)
	if err := repo.Init(); err != nil {
		t.Fatalf("failed to init repo: %v", err)
	}

	return repo
}

func TestListRaces_NoFilter(t *testing.T) {
	repo := newTestRepo(t)

	races, err := repo.List(nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(races) == 0 {
		t.Error("expected races to be returned, got none")
	}
}

func TestListRaces_OnlyVisible_True(t *testing.T) {
	repo := newTestRepo(t)

	races, err := repo.List(&racing.ListRacesRequestFilter{
		OnlyVisible: proto.Bool(true),
	}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, r := range races {
		if !r.Visible {
			t.Errorf("expected only visible races, got race %d with visible=false", r.Id)
		}
	}
}

func TestListRaces_OnlyVisible_False_ReturnsAll(t *testing.T) {
	repo := newTestRepo(t)

	allRaces, err := repo.List(nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	filteredRaces, err := repo.List(&racing.ListRacesRequestFilter{
		OnlyVisible: proto.Bool(false),
	}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// false should behave the same as no filter — return everything
	if len(filteredRaces) != len(allRaces) {
		t.Errorf("expected %d races (all), got %d", len(allRaces), len(filteredRaces))
	}
}

func TestListRaces_DefaultOrder_ByAdvertisedStartTime(t *testing.T) {
	repo := newTestRepo(t)

	races, err := repo.List(nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// checks each race starts no earlier than the previous one
	for i := 1; i < len(races); i++ {
		prev := races[i-1].AdvertisedStartTime.AsTime()
		curr := races[i].AdvertisedStartTime.AsTime()
		if curr.Before(prev) {
			t.Errorf("races not sorted by advertised_start_time: race %d (%v) comes before race %d (%v)", i, curr, i-1, prev)
		}
	}
}

func TestListRaces_OrderBy_Name(t *testing.T) {
	repo := newTestRepo(t)

	races, err := repo.List(nil, "name")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 1; i < len(races); i++ {
		if races[i].Name < races[i-1].Name {
			t.Errorf("races not sorted by name: %q comes before %q", races[i].Name, races[i-1].Name)
		}
	}
}

func TestListRaces_OrderBy_Invalid_FallsBackToDefault(t *testing.T) {
	repo := newTestRepo(t)

	// invalid field should not error, just fall back to default ordering
	races, err := repo.List(nil, "not_a_real_field")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(races) == 0 {
		t.Error("expected races to be returned")
	}
}

func TestListRaces_Status_ClosedWhenInPast(t *testing.T) {
	repo := newTestRepo(t)

	races, err := repo.List(nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, r := range races {
		startTime := r.AdvertisedStartTime.AsTime()
		if startTime.Before(time.Now()) && r.Status != racing.RaceStatus_RACE_STATUS_CLOSED {
			t.Errorf("race %d has past start time but status is not CLOSED", r.Id)
		}
		if !startTime.Before(time.Now()) && r.Status != racing.RaceStatus_RACE_STATUS_OPEN {
			t.Errorf("race %d has future start time but status is not OPEN", r.Id)
		}
	}
}

func TestListRaces_FilterByMeetingID(t *testing.T) {
	repo := newTestRepo(t)

	meetingID := int64(1)
	races, err := repo.List(&racing.ListRacesRequestFilter{
		MeetingIds: []int64{meetingID},
	}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, r := range races {
		if r.MeetingId != meetingID {
			t.Errorf("expected meeting_id %d, got %d", meetingID, r.MeetingId)
		}
	}
}
