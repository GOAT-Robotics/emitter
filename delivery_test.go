package emitter

import (
	"sync"
	"testing"
	"time"
)

func waitDelivery(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery or cleanup blocked")
	}
}

func TestBlockedListenerDoesNotLockRegistry(t *testing.T) {
	e := New(1)
	slow := e.On("slow")
	<-e.Emit("slow", "buffered")
	blocked := e.Emit("slow", "pending")
	t.Cleanup(func() { e.Off("slow", slow) })
	healthyDone := make(chan struct{})
	go func() {
		ch := e.On("healthy")
		<-e.Emit("healthy", "ok")
		<-ch
		e.Listeners("slow")
		e.Snapshot()
		e.Off("healthy", ch)
		close(healthyDone)
	}()
	waitDelivery(t, healthyDone)
	offDone := make(chan struct{})
	go func() { e.Off("slow", slow); close(offDone) }()
	waitDelivery(t, offDone)
	waitDelivery(t, blocked)
	if (<-slow).String(0) != "buffered" {
		t.Fatal("lost already buffered event")
	}
	if _, ok := <-slow; ok {
		t.Fatal("listener was not closed")
	}
}

func TestOffCancelsSynchronousBlockedDelivery(t *testing.T) {
	e := New(0)
	entered := make(chan struct{})
	ch := e.On("slow", Sync, func(*Event) { close(entered) })
	done := make(chan struct{})
	go func() { <-e.Emit("slow"); close(done) }()
	waitDelivery(t, entered)
	offDone := make(chan struct{})
	go func() { e.Off("slow", ch); close(offDone) }()
	waitDelivery(t, offDone)
	waitDelivery(t, done)
}

func TestBlockedCallbackCanInspectAndRemoveListener(t *testing.T) {
	e := New(1)
	ch := e.On("topic", Sync, Close)
	var calls int
	e.OnBlocked = func(info BlockInfo) {
		calls++
		if info.Reason != "buffer_full" {
			t.Errorf("reason=%q", info.Reason)
		}
		e.Snapshot()
		e.Listeners("topic")
		e.Off("topic", ch)
	}
	<-e.Emit("topic", 1)
	done := make(chan struct{})
	go func() { <-e.Emit("topic", 2); close(done) }()
	waitDelivery(t, done)
	if calls != 1 {
		t.Fatalf("callback count=%d", calls)
	}
	if len(e.Listeners("topic")) != 0 {
		t.Fatal("overflow listener leaked")
	}
}

func TestNoListenersCallbackCanRegister(t *testing.T) {
	e := New(1)
	var ch <-chan Event
	e.OnBlocked = func(info BlockInfo) {
		if info.Reason != "no_listeners" {
			t.Errorf("reason=%q", info.Reason)
		}
		ch = e.On(info.Topic)
	}
	done := make(chan struct{})
	go func() { <-e.Emit("missing", 42); close(done) }()
	waitDelivery(t, done)
	e.Off("missing", ch)
}

func TestConcurrentOnceDeliversExactlyOneEvent(t *testing.T) {
	e := New(100)
	ch := e.Once("once")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); <-e.Emit("once", i) }(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	waitDelivery(t, done)
	count := 0
	for range ch {
		count++
	}
	if count != 1 {
		t.Fatalf("once delivered %d events", count)
	}
	if len(e.Listeners("once")) != 0 {
		t.Fatal("once listener leaked")
	}
}

func TestConcurrentOffAndEmit(t *testing.T) {
	e := New(2)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ch := e.On("shared", Skip)
				done := e.Emit("shared", j)
				e.Off("shared", ch)
				<-done
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	waitDelivery(t, done)
	if len(e.Listeners("shared")) != 0 {
		t.Fatal("listeners leaked")
	}
}

func TestMiddlewareCanUseRegistry(t *testing.T) {
	e := New(1)
	e.Use("topic", Sync, func(*Event) { e.Snapshot() })
	ch := e.On("topic", func(*Event) { e.Listeners("topic") })
	done := make(chan struct{})
	go func() { <-e.Emit("topic", 1); close(done) }()
	waitDelivery(t, done)
	<-ch
	e.Off("topic", ch)
}

func TestTopicMiddlewareRunsOncePerTopic(t *testing.T) {
	e := New(1)
	calls := 0
	e.Use("topic", Sync, func(*Event) { calls++ })
	e.On("topic")
	e.On("topic")
	<-e.Emit("topic")
	if calls != 1 {
		t.Fatalf("topic middleware ran %d times", calls)
	}
	e.Off("topic")
}

func TestOnceCleanupUsesRegistrationTopic(t *testing.T) {
	e := New(1)
	ch := e.Once("original", Sync, func(ev *Event) { ev.Topic = "modified" })
	<-e.Emit("original")
	if (<-ch).Topic != "modified" {
		t.Fatal("middleware did not modify topic")
	}
	if len(e.Listeners("original")) != 0 {
		t.Fatal("once listener leaked after topic mutation")
	}
}

func TestMetricsStopIsIdempotent(t *testing.T) {
	e := New(0)
	stop := e.StartMetricsLogger(time.Hour, func(string, ...interface{}) {})
	stop()
	stop()
}

func TestBlockedListenerDoesNotBlockPeerDelivery(t *testing.T) {
	e := New(1)
	slow := e.On("shared")
	fast := e.On("shared")
	<-e.Emit("shared", 1)
	<-fast
	pending := e.Emit("shared", 2)
	select {
	case ev := <-fast:
		if ev.Int(0) != 2 {
			t.Fatalf("peer received %v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked listener prevented delivery to healthy peer")
	}
	e.Off("shared", slow)
	waitDelivery(t, pending)
	e.Off("shared", fast)
}
