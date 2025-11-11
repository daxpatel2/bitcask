package store

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func openTestStore(t *testing.T, dir string) *FileStore {
	t.Helper()
	fs, err := Open(dir)
	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	return fs
}

func TestFileStorePutGetDelete(t *testing.T) {
	dir := t.TempDir()
	fs := openTestStore(t, dir)
	defer fs.Close()

	if err := fs.Put("alpha", "one"); err != nil {
		t.Fatalf("put alpha: %v", err)
	}

	got, err := fs.Get("alpha")
	if err != nil {
		t.Fatalf("get alpha: %v", err)
	}
	if string(got) != "one" {
		t.Fatalf("expected value 'one', got %q", got)
	}

	if err := fs.Delete("alpha"); err != nil {
		t.Fatalf("delete alpha: %v", err)
	}
	if _, err := fs.Get("alpha"); err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}
}

func TestFileStorePersistsAcrossOpen(t *testing.T) {
	dir := t.TempDir()
	func() {
		fs := openTestStore(t, dir)
		defer fs.Close()
		if err := fs.Put("k1", "v1"); err != nil {
			t.Fatalf("put k1: %v", err)
		}
		if err := fs.Put("k2", "v2"); err != nil {
			t.Fatalf("put k2: %v", err)
		}
	}()

	fs := openTestStore(t, dir)
	defer fs.Close()

	tests := map[string]string{"k1": "v1", "k2": "v2"}
	for k, want := range tests {
		got, err := fs.Get(k)
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("value mismatch for %s, want %q got %q", k, want, got)
		}
	}
}

func TestFileStoreWriteHint(t *testing.T) {
	dir := t.TempDir()
	fs := openTestStore(t, dir)
	if err := fs.Put("hinted", "value"); err != nil {
		t.Fatalf("put hinted: %v", err)
	}
	if err := fs.WriteHint(); err != nil {
		t.Fatalf("write hint: %v", err)
	}
	hintPath := HintPath(dir)
	info, err := os.Stat(hintPath)
	if err != nil {
		t.Fatalf("stat hint: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("hint file %s is empty", hintPath)
	}
	fs.Close()

	// Ensure reopening succeeds and data survives when hint file exists.
	fs, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen store with hint: %v", err)
	}
	defer fs.Close()
	val, err := fs.Get("hinted")
	if err != nil || string(val) != "value" {
		t.Fatalf("unexpected value after reopen: %q err=%v", val, err)
	}
}

func TestCompactOnceReclaimsObsoleteSegments(t *testing.T) {
	dir := t.TempDir()
	fs := openTestStore(t, dir)

	largeValue := strings.Repeat("x", 300_000) // ensures rotation after a few writes
	for i := 0; i < 5; i++ {
		if err := fs.Put(keyName(i), largeValue); err != nil {
			t.Fatalf("initial put %d: %v", i, err)
		}
	}

	// Force newer versions of the same keys so sealed segments contain obsolete values.
	for i := 0; i < 5; i++ {
		if err := fs.Put(keyName(i), "fresh-"+keyName(i)); err != nil {
			t.Fatalf("update put %d: %v", i, err)
		}
	}

	// We should now have at least one sealed segment.
	if len(nonActiveSegments(fs)) == 0 {
		t.Fatalf("expected sealed segments after rotation")
	}

	didWork, err := fs.CompactOnce()
	if err != nil {
		t.Fatalf("compact once: %v", err)
	}
	if !didWork {
		t.Fatalf("expected compaction work to occur")
	}

	if len(nonActiveSegments(fs)) != 0 {
		t.Fatalf("expected all sealed segments to be compacted, still have %d", len(nonActiveSegments(fs)))
	}

	// Ensure latest values are still readable.
	for i := 0; i < 5; i++ {
		want := "fresh-" + keyName(i)
		got, err := fs.Get(keyName(i))
		if err != nil {
			t.Fatalf("get %s after compaction: %v", keyName(i), err)
		}
		if string(got) != want {
			t.Fatalf("value mismatch after compaction for %s: want %q got %q", keyName(i), want, got)
		}
	}

	// Ensure no stray .compact temp files linger.
	files, err := filepath.Glob(filepath.Join(dir, "*"+compactExt))
	if err != nil {
		t.Fatalf("glob compact files: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected compact files to be cleaned up, found %v", files)
	}
}

func keyName(i int) string {
	return "key-" + strconv.Itoa(i)
}
