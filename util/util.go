package util

import (
	"fmt"
	"net"
	"strings"
	"time"
)

type IPV4 [4]byte

type Event struct {
	SourceIP      IPV4
	DestinationIP IPV4
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


func parseIPV4(ipv4 string) (IPV4, error){
	ip := net.ParseIP(ipv4).To4()
	if ip == nil {
		return IPV4{}, fmt.Errorf("invalid ipv4 address format: %s", ipv4)
	}
	return IPV4{ip[0],ip[1],ip[2],ip[3]}, nil
}


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

	sourseIP,err := parseIPV4(parts[sourceIndex])

	if err != nil {
		return Event{}, err
	}

	destIP,err := parseIPV4(parts[destIndex])

	if err != nil {
		return Event{}, err
	}


	event := Event{
		Date:          t.Format("2006-01-02"),
		SourceIP:      sourseIP,
		DestinationIP: destIP,
		Payload:       parts[payloadIndex],
		Day:           t.Day(),
	}
	return event, nil
}
