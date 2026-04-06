package db

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"git.neds.sh/matty/entain/sports/proto/sports"
	"google.golang.org/protobuf/proto"
)

func newTestRepo(t *testing.T) EventsRepo {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}

	repo := NewEventsRepo(db)
	if err := repo.Init(); err != nil {
		t.Fatalf("failed to init repo: %v", err)
	}

	return repo
}

func TestListEvents_NoFilter(t *testing.T) {
	repo := newTestRepo(t)

	events, err := repo.List(nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(events) == 0 {
		t.Fatal("expected events, got none")
	}
}

func TestListEvents_OnlyVisible_True(t *testing.T) {
	repo := newTestRepo(t)

	events, err := repo.List(&sports.ListEventsRequestFilter{
		OnlyVisible: proto.Bool(true),
	}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, e := range events {
		if !e.Visible {
			t.Errorf("expected only visible events, got event %d with visible=false", e.Id)
		}
	}
}

func TestListEvents_OnlyVisible_False_ReturnsAll(t *testing.T) {
	repo := newTestRepo(t)

	all, err := repo.List(nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	visible, err := repo.List(&sports.ListEventsRequestFilter{
		OnlyVisible: proto.Bool(true),
	}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(visible) >= len(all) {
		t.Errorf("expected fewer visible events than total, got %d visible vs %d total", len(visible), len(all))
	}
}

func TestListEvents_DefaultOrder_ByAdvertisedStartTime(t *testing.T) {
	repo := newTestRepo(t)

	events, err := repo.List(nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 1; i < len(events); i++ {
		prev := events[i-1].AdvertisedStartTime.AsTime()
		curr := events[i].AdvertisedStartTime.AsTime()
		if curr.Before(prev) {
			t.Errorf("events not ordered by advertised_start_time at index %d", i)
		}
	}
}

func TestListEvents_OrderBy_Name(t *testing.T) {
	repo := newTestRepo(t)

	events, err := repo.List(nil, "name")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 1; i < len(events); i++ {
		if events[i].Name < events[i-1].Name {
			t.Errorf("events not ordered by name at index %d: %q before %q", i, events[i-1].Name, events[i].Name)
		}
	}
}

func TestListEvents_Status(t *testing.T) {
	repo := newTestRepo(t)

	events, err := repo.List(nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, e := range events {
		if e.Status == sports.EventStatus_EVENT_STATUS_UNSPECIFIED {
			t.Errorf("event %d has unspecified status", e.Id)
		}
	}
}
