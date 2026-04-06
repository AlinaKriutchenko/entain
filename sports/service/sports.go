package service

import (
	"git.neds.sh/matty/entain/sports/db"
	"git.neds.sh/matty/entain/sports/proto/sports"
	"golang.org/x/net/context"
)

// sportsService implements the SportsServer interface.
type sportsService struct {
	sports.UnimplementedSportsServer
	eventsRepo db.EventsRepo
}

// NewSportsService sets up and returns a new sports service.
func NewSportsService(eventsRepo db.EventsRepo) sports.SportsServer {
	return &sportsService{eventsRepo: eventsRepo}
}

func (s *sportsService) ListEvents(ctx context.Context, in *sports.ListEventsRequest) (*sports.ListEventsResponse, error) {
	events, err := s.eventsRepo.List(in.Filter, in.OrderBy)
	if err != nil {
		return nil, err
	}

	return &sports.ListEventsResponse{Events: events}, nil
}
