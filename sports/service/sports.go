package service

import (
	"context"

	"git.neds.sh/matty/entain/sports/db"
	"git.neds.sh/matty/entain/sports/proto/sports"
)

type Sports interface {
	// ListEvents will return a collection of events.
	ListEvents(ctx context.Context, in *sports.ListEventsRequest) (*sports.ListEventsResponse, error)

	// GetEvent will return a single event based on the given ID.
	GetEvent(ctx context.Context, in *sports.GetEventRequest) (*sports.Event, error)
}

// sportsService implements the Sports interface.
type sportsService struct {
	sportsRepo db.SportsRepo
}

// NewSportsService instantiates and returns a new sportsService.
func NewSportsService(sportsRepo db.SportsRepo) Sports {
	return &sportsService{sportsRepo}
}

func (s *sportsService) ListEvents(ctx context.Context, in *sports.ListEventsRequest) (*sports.ListEventsResponse, error) {
	events, err := s.sportsRepo.List(in.Filter)
	if err != nil {
		return nil, err
	}

	return &sports.ListEventsResponse{Events: events}, nil
}

func (s *sportsService) GetEvent(ctx context.Context, in *sports.GetEventRequest) (*sports.Event, error) {
	event, err := s.sportsRepo.Get(in)
	if err != nil {
		return nil, err
	}

	return event, nil
}
