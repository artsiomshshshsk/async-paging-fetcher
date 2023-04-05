package util

import (
	"fmt"
	"strings"
	"time"
)

type Event struct {
	SourceIP      string
	DestinationIP string
	Payload       string
	Date          string
	Day           int
}

const (
	dateIndex     = 0
	sourceIndex   = 1
	destIndex     = 2
	payloadIndex  = 3
	numberOfParts = 4
)

func ParseEvent(line string) (Event, error) {

	parts := strings.Split(line, " ")
	if len(parts) != numberOfParts {
		return Event{}, fmt.Errorf("invalid number of parts in event: %d", len(parts))
	}

	t, err := time.Parse(time.RFC3339, parts[dateIndex])
	if err != nil {
		return Event{}, err
	}
	t = t.UTC()

	event := Event{
		Date:          t.Format("2006-01-02"),
		SourceIP:      parts[sourceIndex],
		DestinationIP: parts[destIndex],
		Payload:       parts[payloadIndex],
		Day:           t.Day(),
	}
	return event, nil
}
