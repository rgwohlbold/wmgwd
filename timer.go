package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

type TimerEventIngestor struct {
	newTimerChan chan chan TimerEvent
	lock         *sync.Mutex
}

type TimerEvent struct {
	Func func() error
}

func NewTimerEventIngestor() TimerEventIngestor {
	return TimerEventIngestor{
		newTimerChan: make(chan chan TimerEvent, 1), // capacity 1 to avoid blocking, TimerEventIngestor may have exited before last enqueue
		lock:         &sync.Mutex{},
	}
}

func (i TimerEventIngestor) Enqueue(timeout time.Duration, event TimerEvent) {
	i.lock.Lock()
	defer i.lock.Unlock()

	vniChan := make(chan TimerEvent)
	go func() {
		<-time.After(timeout)
		vniChan <- event
	}()
	i.newTimerChan <- vniChan
}

func (i TimerEventIngestor) Ingest(ctx context.Context, _ *Daemon, eventChan chan<- TimerEvent) {
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
		case newTimer := <-i.newTimerChan:
			timers = append(timers, newTimer)
			continue
		case event := <-currentTimer:
			eventChan <- event
			i.lock.Lock()
			timers = timers[1:]
			i.lock.Unlock()
		}
	}
}
