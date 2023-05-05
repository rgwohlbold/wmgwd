package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

const MigrationTimeout = 10 * time.Second

type TimerEvent struct {
	VNI int
}

var lock sync.Mutex
var timers []*chan TimerEvent
var newTimerChan = make(chan struct{})

func AddMigrationTimer(vni int) {
	lock.Lock()
	defer lock.Unlock()

	vniChan := make(chan TimerEvent)
	go func() {
		<-time.After(MigrationTimeout)
		vniChan <- TimerEvent{VNI: vni}
	}()
	timers = append(timers, &vniChan)
	newTimerChan <- struct{}{}
}

func GenerateTimerEvents(ctx context.Context, timerChan chan<- TimerEvent) {
	for {
		currentTimer := make(chan TimerEvent)
		if len(timers) > 0 {
			currentTimer = *timers[0]
		}
		select {
		case <-ctx.Done():
			log.Info().Msg("timer: context done")
			return
		case <-newTimerChan:
			continue
		case event := <-currentTimer:
			timerChan <- event
			lock.Lock()
			timers = timers[1:]
			lock.Unlock()
		}
	}
}
