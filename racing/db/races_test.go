package db

import (
	"database/sql"
	"testing"

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

	races, err := repo.List(nil)
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
	})
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

	allRaces, err := repo.List(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	filteredRaces, err := repo.List(&racing.ListRacesRequestFilter{
		OnlyVisible: proto.Bool(false),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// false should behave the same as no filter — return everything
	if len(filteredRaces) != len(allRaces) {
		t.Errorf("expected %d races (all), got %d", len(allRaces), len(filteredRaces))
	}
}

func TestListRaces_FilterByMeetingID(t *testing.T) {
	repo := newTestRepo(t)

	meetingID := int64(1)
	races, err := repo.List(&racing.ListRacesRequestFilter{
		MeetingIds: []int64{meetingID},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, r := range races {
		if r.MeetingId != meetingID {
			t.Errorf("expected meeting_id %d, got %d", meetingID, r.MeetingId)
		}
	}
}
