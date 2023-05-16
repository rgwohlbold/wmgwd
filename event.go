package main

import (
	"context"
	"sync"
)

type EventIngestor[E any] interface {
	Ingest(ctx context.Context, daemon *Daemon, eventChan chan<- E, setupChan chan<- struct{})
}

func runEventIngestor[E any](ctx context.Context, daemon *Daemon, ingestor EventIngestor[E], eventChan chan<- E, wg *sync.WaitGroup) {
	wg.Add(1)
	setupChan := make(chan struct{})
	go func() {
		ingestor.Ingest(ctx, daemon, eventChan, setupChan)
		wg.Done()
	}()
	<-setupChan
}
