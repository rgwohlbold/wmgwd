package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

type TimerEventIngestor struct{}

const MigrationTimeout = 5 * time.Second

type TimerEvent struct {
	Func func() error
}

var lock sync.Mutex
var newTimerChan = make(chan chan TimerEvent)

func QueueTimerEvent(event TimerEvent) {
	lock.Lock()
	defer lock.Unlock()

	vniChan := make(chan TimerEvent)
	go func() {
		<-time.After(MigrationTimeout)
		vniChan <- event
	}()
	newTimerChan <- vniChan
}

func (_ TimerEventIngestor) Ingest(ctx context.Context, _ string, eventChan chan<- TimerEvent, setupChan chan<- struct{}) {
	// the ingestor listens for events instantly, since newTimerChan is blocking
	setupChan <- struct{}{}

	timers := make([]chan TimerEvent, 0)
	for {
		currentTimer := make(chan TimerEvent)
		if len(timers) > 0 {
			currentTimer = timers[0]
		}
		select {
		case <-ctx.Done():
			log.Debug().Msg("timer: context done")
			return
		case newTimer := <-newTimerChan:
			timers = append(timers, newTimer)
			continue
		case event := <-currentTimer:
			eventChan <- event
			lock.Lock()
			timers = timers[1:]
			lock.Unlock()
		}
	}
}
