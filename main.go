package main

import (
	"errors"
	"fetcher/util"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

const (
	maxConcurrency = 20
)

type FetchResult struct {
	data []byte
	err  error
}

type Future struct {
	FetchResult chan FetchResult
}


var PageAfterLastFetchError = errors.New("fetching page after last one")

func Fetch(url, username, password string) FetchResult {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth(username, password)

	for {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return FetchResult{nil, err}
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNoContent {
			return FetchResult{nil, PageAfterLastFetchError}
		}

		if resp.StatusCode == http.StatusUnauthorized {
			panic("Unauthorized request!!!")
		}

		if resp.StatusCode == http.StatusGatewayTimeout {
			time.Sleep(time.Second)
			// to many requests
			continue
		}

		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return FetchResult{nil, err}
		}
		if len(data) <= 25 {
			//glitch in the matrix
			continue
		}
		fmt.Printf("fetched %s\n", url)
		return FetchResult{data, nil}
	}
}


func SpawnFetchers(urlPattern, username, password string, cancel chan struct{}, syncChan chan Future) {
	// chanel to limit throughput
	concurrencySemaphore := make(chan struct{}, maxConcurrency)
	pageId := 1
	for {
		select {
		case <-cancel:
			// close channel if blank page is reached
			close(syncChan)
			return
		case concurrencySemaphore <- struct{}{}:
			{
				url := fmt.Sprintf(urlPattern, pageId)
				future := make(chan FetchResult)
				// future is container for not computed value
				go func() {
					future <- Fetch(url, username, password)
					<-concurrencySemaphore
				}()
				// chanel to read pages
				syncChan <- Future{future}
			}
		}
		pageId++
	}
}

func AsyncFetch(urlPattern, username, password string, callback func([]byte)) {
	// channel to send futures of FetchResult
	syncChan := make(chan Future, maxConcurrency)

	// channel to notify SpawnFetchers gourutine to stop spawning 
	// new goroutines for parsing pages, beacause empty page already reached
	cancel := make(chan struct{}, maxConcurrency)
	go SpawnFetchers(urlPattern, username, password, cancel, syncChan)

	for future := range syncChan{
		// wait untill result is ready
		curResult := <- future.FetchResult
		if curResult.err != nil {
			if errors.Is(curResult.err, PageAfterLastFetchError) {
				// if there is no content on the page, stop SpawnFetchers goroutine
				cancel <- struct{}{}
			} else {
				panic(curResult.err)
			}
		}
		callback(curResult.data)
	}
}

func TotalUnique(urlPattern string, username string, password string) int {

	// set for storing events that happend within one day
	sameDayEvents := make(map[util.Event]bool)
	// variable to track when day changes, to add totalUnique events and
	// to release space in the set
	startingDay := "-"
	totalUnique := 0

	// async call to parse pages, we pass callback func here,
	AsyncFetch(urlPattern, username, password, func(page []byte) {
		lines := strings.Split(strings.TrimSpace(string(page)), "\n")

		for _, line := range lines {
			event, err := util.ParseEvent(line)
			if err != nil {
				continue
			}
			//if current day is equal to previos, we add it to the set
			if event.Date == startingDay {
				sameDayEvents[event] = true
			} else {
			// new day scenario
				totalUnique += len(sameDayEvents)
				sameDayEvents = make(map[util.Event]bool)
				sameDayEvents[event] = true
				startingDay = event.Date
			}
		}
	})

	totalUnique += len(sameDayEvents)
	return totalUnique

}

func main() {
	startTime := time.Now()
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	urlPattern := os.Getenv("URL_PATTERN")
	username := os.Getenv("USERNAME")
	password := os.Getenv("PASSWORD")

	total := TotalUnique(urlPattern, username, password)
	fmt.Printf("totalUnique:%d\n", total)
	fmt.Printf("time spent: %d ms\n", time.Now().Sub(startTime).Milliseconds())
}
