package main

import (
	"context"
	"errors"
	"testing"
)

func TestGivenStoppedRelay_WhenWaitingDuringShutdown_ThenReturnsSuccess(t *testing.T) {
	// Given
	done := make(chan struct{})
	close(done)

	// When
	err := waitForRelay(context.Background(), done)

	// Then
	if err != nil {
		t.Fatal("stopped relay was not joined")
	}
}

func TestGivenRelayPastShutdownDeadline_WhenWaiting_ThenReturnsContextError(t *testing.T) {
	// Given
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// When
	err := waitForRelay(ctx, make(chan struct{}))

	// Then
	if !errors.Is(err, context.Canceled) {
		t.Fatal("relay shutdown deadline was not enforced")
	}
}
