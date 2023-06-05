package main

import (
	"context"
	"sync"
	"time"
)

type TimerEventIngestor struct {
	timers       *[]chan TimerEvent
	lock         *sync.Mutex
	newTimerChan chan struct{}
}

type TimerEvent struct {
	Func func()
}

func NewTimerEventIngestor() TimerEventIngestor {
	timers := make([]chan TimerEvent, 0)
	return TimerEventIngestor{
		lock:         &sync.Mutex{},
		timers:       &timers,
		newTimerChan: make(chan struct{}, 1),
	}
}

func (i TimerEventIngestor) Enqueue(timeout time.Duration, event TimerEvent) {
	vniChan := make(chan TimerEvent, 1)
	go func() {
		<-time.After(timeout)
		vniChan <- event
	}()

	i.lock.Lock()
	defer i.lock.Unlock()

	*i.timers = append(*i.timers, vniChan)
	if len(i.newTimerChan) == 0 {
		i.newTimerChan <- struct{}{}
	}
}

func (i TimerEventIngestor) Ingest(ctx context.Context, d *Daemon, eventChan chan<- TimerEvent) {
	for {
		currentTimer := make(chan TimerEvent)
		if len(*i.timers) > 0 {
			currentTimer = (*i.timers)[0]
		}
		select {
		case <-ctx.Done():
			d.log.Debug().Msg("timer: context done")
			return
		case <-i.newTimerChan:
			continue
		case event := <-currentTimer:
			eventChan <- event
			i.lock.Lock()
			*i.timers = (*i.timers)[1:]
			i.lock.Unlock()
		}
	}
}
