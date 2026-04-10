package wal

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Team-Deepiri/deepiri-sugar-glider/internal/redisstreams"
)

type Entry struct {
	Time    string                      `json:"time"`
	Reason  string                      `json:"reason"`
	Request redisstreams.PublishRequest `json:"request"`
}

type Log struct {
	path string
	mu   sync.Mutex
}

const (
	sugarGliderWALFilename = "sugar-glider.wal.jsonl"
	legacyWALFilename      = "sidecar.wal.jsonl"
)

func New(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	path, err := resolveWALPath(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve wal path: %w", err)
	}
	return &Log{path: path}, nil
}

func resolveWALPath(dir string) (string, error) {
	sugarPath := filepath.Join(dir, sugarGliderWALFilename)
	if _, err := os.Stat(sugarPath); err == nil {
		return sugarPath, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	legacyPath := filepath.Join(dir, legacyWALFilename)
	if _, err := os.Stat(legacyPath); err == nil {
		return legacyPath, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	return sugarPath, nil
}

func (l *Log) Append(reason string, req redisstreams.PublishRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	return enc.Encode(Entry{
		Time:    time.Now().UTC().Format(time.RFC3339Nano),
		Reason:  reason,
		Request: req,
	})
}

func (l *Log) Depth() (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	count := 0
	for s.Scan() {
		if s.Text() != "" {
			count++
		}
	}
	return count, s.Err()
}

func (l *Log) Path() string {
	return l.path
}

// Replay drains up to maxEntries from WAL in-order by invoking publish for each entry.
// Entries that fail replay are retained in WAL, preserving at-least-once behavior.
func (l *Log) Replay(
	ctx context.Context,
	maxEntries int64,
	publish func(context.Context, redisstreams.PublishRequest) error,
) (int, error) {
	if maxEntries == 0 {
		return 0, nil
	}
	if maxEntries < 0 {
		return 0, fmt.Errorf("maxEntries must be >= 0")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	lines := make([]string, 0)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	if len(lines) == 0 {
		return 0, nil
	}

	replayed := 0
	remaining := make([]string, 0, len(lines))

	for idx, line := range lines {
		if int64(replayed) >= maxEntries {
			remaining = append(remaining, line)
			continue
		}

		var entry Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			remaining = append(remaining, line)
			continue
		}

		if err := publish(ctx, entry.Request); err != nil {
			remaining = append(remaining, line)
			if idx+1 < len(lines) {
				remaining = append(remaining, lines[idx+1:]...)
			}
			break
		}

		replayed++
	}

	if err := l.rewrite(remaining); err != nil {
		return replayed, err
	}
	return replayed, nil
}

func (l *Log) rewrite(lines []string) error {
	tmpPath := l.path + ".tmp"
	content := strings.Join(lines, "\n")
	if content != "" {
		content += "\n"
	}

	if err := os.WriteFile(tmpPath, []byte(content), 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, l.path)
}
