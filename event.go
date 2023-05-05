package main

import (
	"context"
	"sync"
)

type EventIngestor[E any] interface {
	Ingest(ctx context.Context, node string, eventChan chan<- E, setupChan chan<- struct{})
}

func RunEventIngestor[E any](ctx context.Context, node string, ingestor EventIngestor[E], eventChan chan<- E, wg *sync.WaitGroup) {
	wg.Add(1)
	setupChan := make(chan struct{})
	go func() {
		ingestor.Ingest(ctx, node, eventChan, setupChan)
		wg.Done()
	}()
	<-setupChan
}
