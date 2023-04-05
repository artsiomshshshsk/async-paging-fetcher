package main

import (
	"errors"
	"fetcher/util"
	"fmt"
	"github.com/joho/godotenv"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	maxConcurrency = 20
)

type FetchResult struct {
	data []byte
	err  error
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

func NewSyncChainSlice() *SyncChanSlice[FetchResult] {
	s := SyncChanSlice[FetchResult]{}
	s.cond = sync.NewCond(&s)
	return &s
}

type SyncChanSlice[T any] struct {
	sync.Mutex
	chSlice []chan T
	cond    *sync.Cond
}

func (s *SyncChanSlice[T]) Append(newChan chan T) {
	s.Lock()
	s.chSlice = append(s.chSlice, newChan)
	s.Unlock()
	s.cond.Broadcast()
}

func (s *SyncChanSlice[T]) Len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.chSlice)
}

func (s *SyncChanSlice[T]) Get(pos int) chan T {
	s.Lock()
	defer s.Unlock()
	for pos >= len(s.chSlice) {
		s.cond.Wait()
	}
	return s.chSlice[pos]
}

func SpawnFetchers(urlPattern, username, password string, cancel chan struct{}, slice *SyncChanSlice[FetchResult]) {
	concurrencySemaphore := make(chan struct{}, maxConcurrency)
	pageId := 1
	for {
		select {
		case <-cancel:
			return
		case concurrencySemaphore <- struct{}{}:
			{
				url := fmt.Sprintf(urlPattern, pageId)
				newFuture := func() chan FetchResult {
					future := make(chan FetchResult)
					go func() {
						future <- Fetch(url, username, password)
						<-concurrencySemaphore
					}()
					return future
				}()
				slice.Append(newFuture)
			}
		}
		pageId++
	}
}

func AsyncFetch(urlPattern, username, password string, callback func([]byte)) {
	syncSlice := NewSyncChainSlice()
	cancel := make(chan struct{}, maxConcurrency)
	go SpawnFetchers(urlPattern, username, password, cancel, syncSlice)
	spawningStopped := false
	i := 0
	for {
		if spawningStopped && i >= syncSlice.Len() {
			break
		}
		curFuture := syncSlice.Get(i)
		curResult := <-curFuture

		if curResult.err != nil {
			if errors.Is(curResult.err, PageAfterLastFetchError) {
				cancel <- struct{}{}
				spawningStopped = true
			} else {
				panic(curResult.err)
			}
		}
		callback(curResult.data)
		i++
	}
}

func TotalUnique(urlPattern string, username string, password string) int {

	sameDayEvents := make(map[util.Event]bool)
	startingDay := "-"
	totalUnique := 0

	AsyncFetch(urlPattern, username, password, func(page []byte) {
		lines := strings.Split(strings.TrimSpace(string(page)), "\n")

		for _, line := range lines {
			event, err := util.ParseEvent(line)
			if err != nil {
				continue
			}
			if event.Date == startingDay {
				sameDayEvents[event] = true
			} else {
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
