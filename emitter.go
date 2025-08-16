/*
Package emitter implements channel based pubsub pattern.
The design goals are:
  - fully functional and safety
  - simple to understand and use
  - make the code readable, maintainable and minimalistic
*/
package emitter

import (
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// Flag used to describe what behavior
// do you expect.
type Flag int

const (
	// FlagReset only to clear previously defined flags.
	// Example:
	// ee.Use("*", Reset) // clears flags for this pattern
	FlagReset Flag = 0
	// FlagOnce indicates to remove the listener after first sending.
	FlagOnce Flag = 1 << iota
	// FlagVoid indicates to skip sending.
	FlagVoid
	// FlagSkip indicates to skip sending if channel is blocked.
	FlagSkip
	// FlagClose indicates to drop listener if channel is blocked.
	FlagClose
	// FlagSync indicates to send an event synchronously.
	FlagSync
)

// --- Debug/metrics types ---
// BlockInfo describes a failed/non-blocking send due to a full buffer (or sync cancellation).
type BlockInfo struct {
	When       time.Time
	Topic      string
	ListenerID int64
	Cap        int
	Len        int
	Event      Event
	// Reason explains why send could not proceed: "buffer_full", "listener_closed", "no_listeners", "emitter_done", "skip", "close", "sync-blocked", or "unknown".
	Reason string
}

// OnBlockedFunc is an optional callback invoked when a send cannot proceed
// (e.g., channel buffer full in non-blocking mode).
type OnBlockedFunc func(BlockInfo)

// Middlewares.

// Reset middleware resets flags
func Reset(e *Event) { e.Flags = FlagReset }

// Once middleware sets FlagOnce flag for an event
func Once(e *Event) { e.Flags = e.Flags | FlagOnce }

// Void middleware sets FlagVoid flag for an event
func Void(e *Event) { e.Flags = e.Flags | FlagVoid }

// Skip middleware sets FlagSkip flag for an event
func Skip(e *Event) { e.Flags = e.Flags | FlagSkip }

// Close middleware sets FlagClose flag for an event
func Close(e *Event) { e.Flags = e.Flags | FlagClose }

// Sync middleware sets FlagSync flag for an event
func Sync(e *Event) { e.Flags = e.Flags | FlagSync }

// New returns just created Emitter struct. Capacity argument
// will be used to create channels with given capacity by default. The
// OnWithCap method can be used to get different capacities per listener.
func New(capacity uint) *Emitter {
	return &Emitter{
		Cap:         capacity,
		listeners:   make(map[string][]listener),
		middlewares: make(map[string][]func(*Event)),
		isInit:      true,
	}
}

// Emitter is a struct that allows to emit, receive
// event, close receiver channel, get info
// about topics and listeners
type Emitter struct {
	Cap         uint
	mu          sync.Mutex
	listeners   map[string][]listener
	isInit      bool
	middlewares map[string][]func(*Event)
	// Optional callback invoked when a non-blocking send cannot enqueue
	OnBlocked OnBlockedFunc
}

var nextListenerID int64

func newListener(capacity uint, middlewares ...func(*Event)) listener {
	return listener{
		id:          atomic.AddInt64(&nextListenerID, 1),
		createdAt:   time.Now(),
		ch:          make(chan Event, capacity),
		middlewares: middlewares,
	}
}

type listener struct {
	id          int64
	createdAt   time.Time
	ch          chan Event
	middlewares []func(*Event)
}

func (e *Emitter) init() {
	if !e.isInit {
		e.listeners = make(map[string][]listener)
		e.middlewares = make(map[string][]func(*Event))
		e.isInit = true
	}
}

// Use registers middlewares for the pattern.
func (e *Emitter) Use(pattern string, middlewares ...func(*Event)) {
	e.mu.Lock()
	e.init()
	defer e.mu.Unlock()

	e.middlewares[pattern] = middlewares
	if len(e.middlewares[pattern]) == 0 {
		delete(e.middlewares, pattern)
	}
}

// On returns a channel that will receive events. As optional second
// argument it takes middlewares.
func (e *Emitter) On(topic string, middlewares ...func(*Event)) <-chan Event {
	return e.OnWithCap(topic, e.Cap, middlewares...)
}

// On returns a channel that will receive events with the listener capacity
// specified. As optional second argument it takes middlewares.
func (e *Emitter) OnWithCap(topic string, capacity uint, middlewares ...func(*Event)) <-chan Event {
	e.mu.Lock()
	e.init()
	l := newListener(capacity, middlewares...)
	if listeners, ok := e.listeners[topic]; ok {
		e.listeners[topic] = append(listeners, l)
	} else {
		e.listeners[topic] = []listener{l}
	}
	e.mu.Unlock()
	return l.ch
}

// Once works exactly like On(see above) but with `Once` as the first middleware.
func (e *Emitter) Once(topic string, middlewares ...func(*Event)) <-chan Event {
	return e.On(topic, append(middlewares, Once)...)
}

// Off unsubscribes all listeners which were covered by
// topic, it can be pattern as well.
func (e *Emitter) Off(topic string, channels ...<-chan Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.init()
	match, err := e.matched(topic)
	if err != nil {
		fmt.Printf("[emitter] Off: invalid topic pattern %q: %v\n", topic, err)
	}

	for _, _topic := range match {
		if listeners, ok := e.listeners[_topic]; ok {
			removedCount := 0
			if len(channels) == 0 {
				for i := len(listeners) - 1; i >= 0; i-- {
					close(listeners[i].ch)
					listeners = drop(listeners, i)
					removedCount++
				}

			} else {
				for chi := range channels {
					curr := channels[chi]
					for i := len(listeners) - 1; i >= 0; i-- {
						if curr == listeners[i].ch {
							close(listeners[i].ch)
							listeners = drop(listeners, i)
							removedCount++
						}
					}
				}
			}
			e.listeners[_topic] = listeners

			if removedCount == 0 {
				fmt.Printf("[emitter] Off: no listeners removed for topic=%q (channels specified: %t)\n", _topic, len(channels) != 0)
			} else {
				fmt.Printf("[emitter] Off: removed %d listener(s) for topic=%q\n", removedCount, _topic)
			}
		}
		if len(e.listeners[_topic]) == 0 {
			delete(e.listeners, _topic)
		}
	}
}

// Listeners returns slice of listeners which were covered by
// topic(it can be pattern) and error if pattern is invalid.
func (e *Emitter) Listeners(topic string) []<-chan Event {
	e.mu.Lock()
	e.init()
	defer e.mu.Unlock()
	var acc []<-chan Event
	match, _ := e.matched(topic)

	for _, _topic := range match {
		list := e.listeners[_topic]
		for i := range e.listeners[_topic] {
			acc = append(acc, list[i].ch)
		}
	}

	return acc
}

// Topics returns all existing topics.
func (e *Emitter) Topics() []string {
	e.mu.Lock()
	e.init()
	defer e.mu.Unlock()
	acc := make([]string, len(e.listeners))
	i := 0
	for k := range e.listeners {
		acc[i] = k
		i++
	}
	return acc
}

// Emit emits an event with the rest arguments to all
// listeners which were covered by topic(it can be pattern).
func (e *Emitter) Emit(topic string, args ...interface{}) chan struct{} {
	e.mu.Lock()
	e.init()
	done := make(chan struct{}, 1)

	match, err := e.matched(topic)
	if err != nil {
		fmt.Printf("[emitter] Emit: invalid topic pattern %q: %v\n", topic, err)
	}

	// If nothing matches, surface a single 'no_listeners' report.
	if e.OnBlocked != nil {
		empty := true
		for _, _topic := range match {
			if len(e.listeners[_topic]) > 0 {
				empty = false
				break
			}
		}
		if empty {
			fmt.Printf("[emitter] Emit: no listeners for topic=%q\n", topic)
			// No concrete listeners for this topic / pattern.
			e.OnBlocked(BlockInfo{
				When:       time.Now(),
				Topic:      topic,
				ListenerID: 0,
				Cap:        0,
				Len:        0,
				Event:      Event{Topic: topic, OriginalTopic: topic},
				Reason:     "no_listeners",
			})
		}
	}

	var wg sync.WaitGroup
	var haveToWait bool
	for _, _topic := range match {
		listeners := e.listeners[_topic]
		event := Event{
			Topic:         _topic,
			OriginalTopic: topic,
			Args:          args,
		}

		applyMiddlewares(&event, e.getMiddlewares(_topic))

		// whole topic is skipping
		// if (event.Flags | FlagVoid) == event.Flags {
		// 	continue
		// }

	Loop:
		for i := len(listeners) - 1; i >= 0; i-- {
			lstnr := listeners[i]
			evn := *(&event) // copy the event
			applyMiddlewares(&evn, lstnr.middlewares)

			if (evn.Flags | FlagVoid) == evn.Flags {
				continue Loop
			}

			if (evn.Flags | FlagSync) == evn.Flags {
				_, remove, _ := e.sendWithReport(done, lstnr, &evn)
				if remove {
					defer e.Off(event.Topic, lstnr.ch)
				}
			} else {
				wg.Add(1)
				haveToWait = true
				go func(lstnr listener, event *Event) {
					e.mu.Lock()
					_, remove, _ := e.sendWithReport(done, lstnr, event)
					if remove {
						defer e.Off(event.Topic, lstnr.ch)
					}
					wg.Done()
					e.mu.Unlock()
				}(lstnr, &evn)
			}
		}

	}
	if haveToWait {
		go func(done chan struct{}) {
			defer func() { recover() }()
			wg.Wait()
			close(done)
		}(done)
	} else {
		close(done)
	}

	e.mu.Unlock()
	return done
}

func pushEvent(
	done chan struct{},
	lstnr chan Event,
	event *Event,
) (success, remove bool, reason string) {
	// Unwind flags
	isOnce := (event.Flags | FlagOnce) == event.Flags
	isSkip := (event.Flags | FlagSkip) == event.Flags
	isClose := (event.Flags | FlagClose) == event.Flags

	wait := !(isSkip || isClose)

	sent, canceled, closed := send(done, lstnr, *event, wait)
	success = sent

	switch {
	case closed:
		// Listener channel was closed.
		reason = "listener_closed"
		// If we encountered a closed channel, ensure removal from registry.
		remove = true

	case !sent && canceled:
		// Emit was canceled by 'done' (emitter finished / lifecycle end).
		reason = "emitter_done"
		remove = false

	case !sent && !canceled && !wait:
		// Non-blocking send and buffer was full.
		reason = "buffer_full"
		remove = isClose

	case sent:
		// Successful send: removal depends on Once flag.
		remove = isOnce

	default:
		// Fallback to any reason hinted by flags (e.g., skip/close/sync-blocked).
		if r := reasonFromFlags(event.Flags); r != "" {
			reason = r
		} else {
			reason = "unknown"
		}
		// Removal for non-success already handled above; keep remove=false here.
	}
	return
}

// reasonFromFlags returns a human-readable reason derived from flags when applicable.
func reasonFromFlags(f Flag) string {
	switch {
	case (f | FlagSkip) == f:
		return "skip"
	case (f | FlagClose) == f:
		return "close"
	case (f | FlagSync) == f:
		return "sync-blocked"
	default:
		return ""
	}
}

// sendWithReport wraps pushEvent to invoke OnBlocked when a send could not proceed.
func (e *Emitter) sendWithReport(done chan struct{}, lstnr listener, ev *Event) (success, remove bool, err error) {
	success, remove, reason := pushEvent(done, lstnr.ch, ev)

	if !success && e.OnBlocked != nil {
		ri := reason
		if ri == "" {
			ri = "unknown"
		}
		bi := BlockInfo{
			When:       time.Now(),
			Topic:      ev.Topic,
			ListenerID: lstnr.id,
			Cap:        cap(lstnr.ch),
			Len:        len(lstnr.ch),
			Event:      *ev,
			Reason:     ri,
		}
		fmt.Printf("[emitter] blocked: topic=%q id=%d usage=%d/%d reason=%s\n", bi.Topic, bi.ListenerID, bi.Len, bi.Cap, ri)
		e.OnBlocked(bi)
	}
	return
}

func (e *Emitter) getMiddlewares(topic string) []func(*Event) {
	var acc []func(*Event)
	for pattern, v := range e.middlewares {
		if match, _ := path.Match(pattern, topic); match {
			acc = append(acc, v...)
		} else if match, _ := path.Match(topic, pattern); match {
			acc = append(acc, v...)
		}
	}
	return acc
}

func applyMiddlewares(e *Event, fns []func(*Event)) {
	for i := range fns {
		fns[i](e)
	}
}

func (e *Emitter) matched(topic string) ([]string, error) {
	acc := []string{}
	var err error
	for k := range e.listeners {
		if matched, err := path.Match(topic, k); err != nil {
			fmt.Printf("[emitter] matched: invalid pattern %q vs %q: %v\n", topic, k, err)
			return []string{}, err
		} else if matched {
			acc = append(acc, k)
		} else {
			if matched, _ := path.Match(k, topic); matched {
				acc = append(acc, k)
			}
		}
	}
	return acc, err
}

func drop(l []listener, i int) []listener {
	return append(l[:i], l[i+1:]...)
}

func send(
	done chan struct{},
	ch chan Event,
	e Event, wait bool,
) (sent, canceled, closed bool) {
	defer func() {
		if r := recover(); r != nil {
			// Sending on a closed channel panics; flag as closed.
			closed = true
			canceled = false
			sent = false
		}
	}()

	if !wait {
		select {
		case <-done:
			// Emit lifecycle finished before we could send.
			canceled = true
		case ch <- e:
			sent = true
			return
		default:
			// Non-blocking and channel buffer is full.
			return
		}
	} else {
		select {
		case <-done:
			canceled = true
		case ch <- e:
			sent = true
			return
		}
	}
	return
}

// ListenerStats provides occupancy info for a single listener.
type ListenerStats struct {
	ListenerID int64
	Cap        int
	Len        int
	CreatedAt  time.Time
}

// TopicStats aggregates listener stats per topic.
type TopicStats struct {
	Topic     string
	Count     int
	Listeners []ListenerStats
}

// Snapshot returns a read-only snapshot of current buffer usage for all listeners grouped by topic.
func (e *Emitter) Snapshot() []TopicStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.init()

	out := make([]TopicStats, 0, len(e.listeners))
	for topic, lst := range e.listeners {
		ts := TopicStats{Topic: topic, Count: len(lst), Listeners: make([]ListenerStats, 0, len(lst))}
		for _, l := range lst {
			ts.Listeners = append(ts.Listeners, ListenerStats{
				ListenerID: l.id,
				Cap:        cap(l.ch),
				Len:        len(l.ch),
				CreatedAt:  l.createdAt,
			})
		}
		out = append(out, ts)
	}
	return out
}

// StartMetricsLogger periodically logs channel occupancy using the provided logger.
// It returns a stop function that should be called to stop the logger.
func (e *Emitter) StartMetricsLogger(every time.Duration, logf func(string, ...interface{})) func() {
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				stats := e.Snapshot()
				for _, ts := range stats {
					for _, ls := range ts.Listeners {
						logf("[emitter] topic=%q id=%d usage=%d/%d", ts.Topic, ls.ListenerID, ls.Len, ls.Cap)
					}
				}
			case <-stop:
				return
			}
		}
	}()
	return func() { close(stop) }
}
