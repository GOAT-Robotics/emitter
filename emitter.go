/*
Package emitter implements channel based pubsub pattern.
The design goals are:
  - fully functional and safety
  - simple to understand and use
  - make the code readable, maintainable and minimalistic
*/
package emitter

import (
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
	// Callback for overflow or no matching listeners. Called outside internal locks;
	// set before concurrent use and make it safe for concurrent calls.
	OnBlocked OnBlockedFunc
}

var nextListenerID int64

func newListener(capacity uint, middlewares ...func(*Event)) listener {
	return listener{
		id:          atomic.AddInt64(&nextListenerID, 1),
		createdAt:   time.Now(),
		ch:          make(chan Event, capacity),
		middlewares: append([]func(*Event){}, middlewares...),
		state:       &listenerState{stopped: make(chan struct{})},
	}
}

// Copies of a listener share delivery state. Never hold Emitter.mu while
// acquiring state.mu: a blocked send must not prevent registry operations.
type listenerState struct {
	mu       sync.Mutex
	stopped  chan struct{}
	stopOnce sync.Once
	closed   bool
}

func (l listener) stop() {
	l.state.stopOnce.Do(func() { close(l.state.stopped) })
	l.state.mu.Lock()
	defer l.state.mu.Unlock()
	l.closeLocked()
}

func (l listener) closeLocked() {
	if !l.state.closed {
		l.state.stopOnce.Do(func() { close(l.state.stopped) })
		close(l.ch)
		l.state.closed = true
	}
}

type listener struct {
	state       *listenerState
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

	e.middlewares[pattern] = append([]func(*Event){}, middlewares...)
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
	e.init()
	match, _ := e.matched(topic)
	var removed []listener
	for _, topic := range match {
		var kept []listener
		for _, l := range e.listeners[topic] {
			remove := len(channels) == 0
			for _, ch := range channels {
				if ch == l.ch {
					remove = true
					break
				}
			}
			if remove {
				removed = append(removed, l)
			} else {
				kept = append(kept, l)
			}
		}
		if len(kept) == 0 {
			delete(e.listeners, topic)
		} else {
			e.listeners[topic] = kept
		}
	}
	e.mu.Unlock()
	// Cancel sends before waiting for their per-listener mutex. Other topics and
	// newly registered listeners remain usable while cleanup completes.
	for _, l := range removed {
		l.state.stopOnce.Do(func() { close(l.state.stopped) })
	}
	for _, l := range removed {
		l.stop()
	}
}

// removeListener removes one exact registration, never a replacement listener.
func (e *Emitter) removeListener(topic string, id int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	list := e.listeners[topic]
	for i, l := range list {
		if l.id == id {
			list = drop(list, i)
			if len(list) == 0 {
				delete(e.listeners, topic)
			} else {
				e.listeners[topic] = list
			}
			return
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
	type target struct {
		topic      string
		listeners  []listener
		middleware []func(*Event)
	}
	type delivery struct {
		topic    string
		listener listener
		event    Event
	}
	e.mu.Lock()
	e.init()
	match, _ := e.matched(topic)
	var targets []target
	for _, key := range match {
		targets = append(targets, target{key, append([]listener(nil), e.listeners[key]...), e.getMiddlewares(key)})
	}
	report := e.OnBlocked
	e.mu.Unlock()

	done := make(chan struct{}, 1)
	if len(targets) == 0 && report != nil {
		report(BlockInfo{When: time.Now(), Topic: topic, Event: Event{Topic: topic, OriginalTopic: topic, Args: args}, Reason: "no_listeners"})
	}
	var wg sync.WaitGroup
	haveAsync := false
	for _, target := range targets {
		event := Event{Topic: target.topic, OriginalTopic: topic, Args: args}
		applyMiddlewares(&event, target.middleware)
		for _, l := range target.listeners {
			d := delivery{target.topic, l, event}
			applyMiddlewares(&d.event, d.listener.middlewares)
			if d.event.Flags&FlagVoid != 0 {
				continue
			}
			deliver := func(d delivery) {
				remove, info := d.listener.deliver(done, d.event)
				if remove {
					e.removeListener(d.topic, d.listener.id)
				}
				// Callbacks may use registry methods; neither mutex is held here.
				if info != nil && report != nil {
					report(*info)
				}
			}
			if d.event.Flags&FlagSync != 0 {
				deliver(d)
			} else {
				haveAsync = true
				wg.Add(1)
				go func(d delivery) { defer wg.Done(); deliver(d) }(d)
			}
		}
	}
	finish := func() {
		// The legacy cancellation API also allows callers to close done.
		defer func() { recover() }()
		close(done)
	}
	if haveAsync {
		go func() { wg.Wait(); finish() }()
	} else {
		finish()
	}
	return done
}

// deliver serializes sends and closure for this listener only. Default blocking
// delivery remains lossless until canceled by Off or the Emit cancellation channel.
func (l listener) deliver(done chan struct{}, ev Event) (remove bool, info *BlockInfo) {
	l.state.mu.Lock()
	defer l.state.mu.Unlock()
	if l.state.closed {
		return false, nil
	}
	select {
	case <-l.state.stopped:
		return false, nil
	default:
	}
	select {
	case <-done:
		return false, nil
	default:
	}
	sent := false
	if ev.Flags&(FlagSkip|FlagClose) != 0 {
		select {
		case <-l.state.stopped:
			return false, nil
		case <-done:
			return false, nil
		case l.ch <- ev:
			sent = true
		default:
		}
	} else {
		select {
		case <-l.state.stopped:
			return false, nil
		case <-done:
			return false, nil
		case l.ch <- ev:
			sent = true
		}
	}
	if !sent {
		info = &BlockInfo{When: time.Now(), Topic: ev.Topic, ListenerID: l.id, Cap: cap(l.ch), Len: len(l.ch), Event: ev, Reason: "buffer_full"}
	}
	remove = sent && ev.Flags&FlagOnce != 0 || !sent && ev.Flags&FlagClose != 0
	if remove {
		l.closeLocked()
	}
	return
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
	var once sync.Once
	return func() { once.Do(func() { close(stop) }) }
}
