package main

import (
	"context"
	cryptoRand "crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"bitcask/internal/store"
)

type opType int

const (
	opGet opType = iota
	opPut
	opDel
)

type metrics struct {
	puts        uint64
	gets        uint64
	dels        uint64
	errors      uint64
	bytesWrite  uint64
	bytesRead   uint64
	latencyCh   chan time.Duration // sampled latencies
	sampleLimit int
	samples     []time.Duration
	mu          sync.Mutex
	start       time.Time
}

func newMetrics(sampleLimit int) *metrics {
	return &metrics{
		latencyCh:   make(chan time.Duration, 10000),
		sampleLimit: sampleLimit,
		start:       time.Now(),
	}
}

func (m *metrics) collector() {
	for d := range m.latencyCh {
		m.mu.Lock()
		if len(m.samples) < m.sampleLimit {
			m.samples = append(m.samples, d)
		}
		m.mu.Unlock()
	}
}

func (m *metrics) addLatency(d time.Duration) {
	select {
	case m.latencyCh <- d:
	default:
		// drop if channel is full; sampling is fine
	}
}

func (m *metrics) summary() (p50, p95, p99, avg time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.samples) == 0 {
		return 0, 0, 0, 0
	}
	s := append([]time.Duration(nil), m.samples...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	n := len(s)
	p50 = s[(50*n)/100]
	p95 = s[(95*n)/100]
	p99 = s[(99*n)/100]
	var sum time.Duration
	for _, v := range s {
		sum += v
	}
	avg = sum / time.Duration(n)
	return
}

func main() {
	// -------- flags ----------
	dataPath := flag.String("data", "./data/bitcask.data", "path to bitcask data file")
	concurrency := flag.Int("concurrency", 8, "number of worker goroutines")
	duration := flag.Duration("duration", 10*time.Second, "how long to run the workload (use either duration or ops)")
	ops := flag.Int("ops", 0, "total ops per worker (0 = use duration)")
	keySpace := flag.Int("keyspace", 10000, "unique keys per worker (higher => more misses)")
	valueSize := flag.Int("valueSize", 256, "bytes per value for puts")
	readPct := flag.Int("read", 70, "percentage of GET operations (0-100)")
	writePct := flag.Int("write", 25, "percentage of PUT operations (0-100)")
	delPct := flag.Int("del", 5, "percentage of DELETE operations (0-100)")
	compactEvery := flag.Duration("compactEvery", 0, "optional: run compaction on this interval (0 = disabled)")
	printEvery := flag.Duration("printEvery", time.Second, "interval for progress prints")
	sampleLimit := flag.Int("samples", 200000, "max latency samples to store for percentile calc")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write mem profile to file on exit")
	useStartupOnly := flag.Bool("startupOnly", false, "open (and optionally write hint) then exit (benchmark startup)")
	forceScan := flag.Bool("forceScan", false, "ignore/delete hint before opening (forces full scan)")
	writeHint := flag.Bool("writeHint", false, "write a fresh hint file before exit")
	printStartup := flag.Bool("printStartup", true, "print startup mode (hint vs scan) and time")

	flag.Parse()

	if *readPct+*writePct+*delPct != 100 {
		log.Fatalf("read+write+del must equal 100")
	}

	// ensure dir exists
	if err := os.MkdirAll(filepath.Dir(*dataPath), 0755); err != nil {
		log.Fatalf("mkdir data dir: %v", err)
	}

	if *forceScan {
		hp := *dataPath + ".hint"
		_ = os.Remove(hp) // ignore error if not present
	}

	// profiling
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatalf("create cpuprofile: %v", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("start cpu profile: %v", err)
		}
		defer pprof.StopCPUProfile()
	}

	// open store
	s, err := store.Open(*dataPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer s.Close()

	startupT := time.Now()

	ss, errr := store.Open(*dataPath)
	if errr != nil {
		log.Fatalf("open store: %v", errr)
	}
	defer ss.Close()

	// Report startup mode & timing
	if *printStartup {
		mode := guessStartupMode(*dataPath) // see helper at bottom
		fmt.Printf("[startup] mode=%s time=%s\n", mode, time.Since(startupT).Truncate(time.Millisecond))
	}

	// If you're only benchmarking startup, optionally write a hint and exit.
	if *useStartupOnly {
		if *writeHint {
			if err := s.WriteHint(); err != nil {
				log.Printf("write hint failed: %v", err)
			} else {
				fmt.Println("[startup] wrote hint file")
			}
		}
		return
	}

	// metrics
	m := newMetrics(*sampleLimit)
	go m.collector()

	// progress ticker
	ticker := time.NewTicker(*printEvery)
	defer ticker.Stop()

	// compaction ticker
	var compTicker *time.Ticker
	if *compactEvery > 0 {
		compTicker = time.NewTicker(*compactEvery)
		defer compTicker.Stop()
	}

	// cancellation (duration or ctrl+c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if *ops == 0 && *duration > 0 {
		time.AfterFunc(*duration, cancel)
	}

	// handle ctrl+c
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch
		cancel()
	}()

	// start workers
	var wg sync.WaitGroup
	wg.Add(*concurrency)

	start := time.Now()
	m.start = start

	// capture totals every print interval
	lastTotal := uint64(0)

	for wid := 0; wid < *concurrency; wid++ {
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(newSeed())))
			// key set for this worker
			keys := make([]string, *keySpace)
			for i := 0; i < *keySpace; i++ {
				keys[i] = fmt.Sprintf("w%02d:k%06d", id, i)
			}

			opsForThis := *ops
			for {
				if *ops == 0 {
					select {
					case <-ctx.Done():
						return
					default:
					}
				} else {
					if opsForThis <= 0 {
						return
					}
					opsForThis--
				}

				// choose op according to pct
				p := r.Intn(100)
				var op opType
				switch {
				case p < *readPct:
					op = opGet
				case p < *readPct+*writePct:
					op = opPut
				default:
					op = opDel
				}

				k := keys[r.Intn(len(keys))]
				switch op {
				case opGet:
					t0 := time.Now()
					val, err := s.Get(k)
					d := time.Since(t0)
					m.addLatency(d)
					if err != nil && !errorsIs(err, store.ErrKeyNotFound) {
						atomic.AddUint64(&m.errors, 1)
					} else if err == nil {
						atomic.AddUint64(&m.gets, 1)
						atomic.AddUint64(&m.bytesRead, uint64(len(val)))
					} else {
						// key not found counts as a get (miss); still record op
						atomic.AddUint64(&m.gets, 1)
					}
				case opPut:
					payload := randBytes(r, *valueSize)
					t0 := time.Now()
					err := s.Put(k, b2s(payload))
					d := time.Since(t0)
					m.addLatency(d)
					if err != nil {
						atomic.AddUint64(&m.errors, 1)
					} else {
						atomic.AddUint64(&m.puts, 1)
						atomic.AddUint64(&m.bytesWrite, uint64(len(payload)))
					}
				case opDel:
					t0 := time.Now()
					err := s.Delete(k)
					d := time.Since(t0)
					m.addLatency(d)
					if err != nil {
						atomic.AddUint64(&m.errors, 1)
					} else {
						atomic.AddUint64(&m.dels, 1)
					}
				}
			}
		}(wid)
	}

	// background compaction (optional)
	done := make(chan struct{})
	if compTicker != nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					close(done)
					return
				case <-compTicker.C:
					t0 := time.Now()
					if err := s.Compact(); err != nil {
						log.Printf("[compact] error: %v", err)
						continue
					}
					log.Printf("[compact] completed in %s", time.Since(t0))
				}
			}
		}()
	}

	// progress printer
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				total := atomic.LoadUint64(&m.puts) + atomic.LoadUint64(&m.gets) + atomic.LoadUint64(&m.dels)
				elapsed := time.Since(start).Seconds()
				intervalOps := total - lastTotal
				lastTotal = total
				opsPerSec := float64(intervalOps) / tickerIntervalSecs(*printEvery)
				fmt.Printf("[%.1fs] ops=%d (%.0f ops/s) puts=%d gets=%d dels=%d errs=%d written=%s read=%s\n",
					elapsed,
					total,
					opsPerSec,
					atomic.LoadUint64(&m.puts),
					atomic.LoadUint64(&m.gets),
					atomic.LoadUint64(&m.dels),
					atomic.LoadUint64(&m.errors),
					humanBytes(atomic.LoadUint64(&m.bytesWrite)),
					humanBytes(atomic.LoadUint64(&m.bytesRead)),
				)
			}
		}
	}()

	// wait for workers
	wg.Wait()
	cancel()
	if compTicker != nil {
		<-done
	}
	close(m.latencyCh) // stop collector
	time.Sleep(50 * time.Millisecond)

	// final stats
	total := atomic.LoadUint64(&m.puts) + atomic.LoadUint64(&m.gets) + atomic.LoadUint64(&m.dels)
	elapsed := time.Since(start).Seconds()
	qps := float64(total) / elapsed
	p50, p95, p99, avg := m.summary()

	// file size
	fi, _ := os.Stat(*dataPath)
	size := uint64(0)
	if fi != nil {
		size = uint64(fi.Size())
	}

	fmt.Println("--------- summary ---------")
	fmt.Printf("duration:         %s\n", time.Since(start).Truncate(time.Millisecond))
	fmt.Printf("total ops:        %d  (%.0f ops/s)\n", total, qps)
	fmt.Printf("puts/gets/dels:   %d / %d / %d\n", atomic.LoadUint64(&m.puts), atomic.LoadUint64(&m.gets), atomic.LoadUint64(&m.dels))
	fmt.Printf("errors:           %d\n", atomic.LoadUint64(&m.errors))
	fmt.Printf("latency avg:      %s\n", avg)
	fmt.Printf("latency p50/p95/p99: %s / %s / %s\n", p50, p95, p99)
	fmt.Printf("bytes written:    %s\n", humanBytes(atomic.LoadUint64(&m.bytesWrite)))
	fmt.Printf("bytes read:       %s\n", humanBytes(atomic.LoadUint64(&m.bytesRead)))
	fmt.Printf("file size:        %s\n", humanBytes(size))

	if *writeHint {
		t0 := time.Now()
		if err := s.WriteHint(); err != nil {
			log.Printf("write hint failed: %v", err)
		} else {
			fmt.Printf("[shutdown] wrote hint in %s\n", time.Since(t0).Truncate(time.Millisecond))
		}
	}

	// memory profile (optional)
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatalf("create memprofile: %v", err)
		}
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatalf("write mem profile: %v", err)
		}
		_ = f.Close()
	}
}

// -------- helpers --------

func humanBytes(b uint64) string {
	const (
		KB = 1 << 10
		MB = 1 << 20
		GB = 1 << 30
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.2f GiB", float64(b)/GB)
	case b >= MB:
		return fmt.Sprintf("%.2f MiB", float64(b)/MB)
	case b >= KB:
		return fmt.Sprintf("%.2f KiB", float64(b)/KB)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func randBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	// generate blocks of uint64 for speed
	i := 0
	for i+8 <= n {
		u := r.Uint64()
		binary.LittleEndian.PutUint64(b[i:], u)
		i += 8
	}
	for i < n {
		b[i] = byte(r.Intn(256))
		i++
	}
	return b
}

func b2s(b []byte) string {
	// zero-copy conversion is unsafe; keep it safe
	return string(b)
}

func newSeed() uint64 {
	var s uint64
	_ = binary.Read(cryptoRand.Reader, binary.LittleEndian, &s)
	if s == 0 {
		s = uint64(time.Now().UnixNano())
	}
	return s
}

func tickerIntervalSecs(d time.Duration) float64 {
	if d <= 0 {
		return 1
	}
	return d.Seconds()
}

func errorsIs(err error, target error) bool {
	// allow running even if you defined errors in store package
	return err != nil && target != nil && err.Error() == target.Error()
}

// guessStartupMode reports whether Open() likely used a hint or scanned.
// It's a heuristic: if <data>.hint exists and is at least as new as <data>, we say "hint", else "scan".
func guessStartupMode(dataPath string) string {
	hint := dataPath + ".hint"
	dfi, derr := os.Stat(dataPath)
	hfi, herr := os.Stat(hint)

	if derr != nil {
		// shouldn't happen because we just opened the store
		return "unknown"
	}
	if herr != nil {
		return "scan" // no hint
	}
	if !hfi.ModTime().Before(dfi.ModTime()) {
		return "hint"
	}
	return "scan"
}
