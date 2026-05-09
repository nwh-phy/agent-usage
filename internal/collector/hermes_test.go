package collector

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func createHermesStateDB(t *testing.T, path string) {
	t.Helper()
	src, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open hermes state db: %v", err)
	}
	defer src.Close()

	_, err = src.Exec(`
		CREATE TABLE sessions (
			id TEXT PRIMARY KEY,
			source TEXT NOT NULL,
			model TEXT,
			parent_session_id TEXT,
			started_at REAL NOT NULL,
			ended_at REAL,
			message_count INTEGER DEFAULT 0,
			tool_call_count INTEGER DEFAULT 0,
			input_tokens INTEGER DEFAULT 0,
			output_tokens INTEGER DEFAULT 0,
			cache_read_tokens INTEGER DEFAULT 0,
			cache_write_tokens INTEGER DEFAULT 0,
			reasoning_tokens INTEGER DEFAULT 0,
			estimated_cost_usd REAL,
			actual_cost_usd REAL,
			api_call_count INTEGER DEFAULT 0
		);
		CREATE TABLE messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT NOT NULL,
			role TEXT NOT NULL,
			content TEXT,
			timestamp REAL NOT NULL,
			token_count INTEGER
		);
	`)
	if err != nil {
		t.Fatalf("create hermes schema: %v", err)
	}
}

func insertHermesSession(t *testing.T, path, sessionID string, started time.Time, input, output, cacheRead, cacheWrite, reasoning int64, calls int) {
	t.Helper()
	src, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open hermes state db: %v", err)
	}
	defer src.Close()

	_, err = src.Exec(`
		INSERT INTO sessions (
			id, source, model, started_at, input_tokens, output_tokens,
			cache_read_tokens, cache_write_tokens, reasoning_tokens,
			estimated_cost_usd, api_call_count
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		sessionID, "cli", "anthropic/claude-sonnet-4.6", unixSeconds(started),
		input, output, cacheRead, cacheWrite, reasoning, 0.123, calls,
	)
	if err != nil {
		t.Fatalf("insert hermes session: %v", err)
	}

	for i := 0; i < 2; i++ {
		_, err = src.Exec(
			`INSERT INTO messages(session_id, role, content, timestamp, token_count) VALUES(?,?,?,?,?)`,
			sessionID, "user", "hello", unixSeconds(started.Add(time.Duration(i+1)*time.Minute)), 12,
		)
		if err != nil {
			t.Fatalf("insert hermes user message: %v", err)
		}
	}
	_, err = src.Exec(
		`INSERT INTO messages(session_id, role, content, timestamp, token_count) VALUES(?,?,?,?,?)`,
		sessionID, "assistant", "hi", unixSeconds(started.Add(3*time.Minute)), 34,
	)
	if err != nil {
		t.Fatalf("insert hermes assistant message: %v", err)
	}
}

func unixSeconds(t time.Time) float64 {
	return float64(t.UnixNano()) / float64(time.Second)
}

func TestHermesCollector_ScanStateDB(t *testing.T) {
	db := tempDB(t)
	dir := t.TempDir()
	stateDB := filepath.Join(dir, "state.db")
	createHermesStateDB(t, stateDB)

	started := time.Date(2026, 5, 9, 10, 0, 0, 0, time.UTC)
	insertHermesSession(t, stateDB, "hermes-sess-1", started, 1000, 400, 200, 50, 25, 3)

	hc := NewHermesCollector(db, []string{stateDB})
	if err := hc.Scan(); err != nil {
		t.Fatalf("Scan: %v", err)
	}

	from := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	stats, err := db.GetDashboardStats(from, to, "hermes", "")
	if err != nil {
		t.Fatalf("GetDashboardStats: %v", err)
	}
	if stats.TotalTokens != 1650 {
		t.Errorf("expected 1650 tokens, got %d", stats.TotalTokens)
	}
	if stats.TotalPrompts != 2 {
		t.Errorf("expected 2 prompts, got %d", stats.TotalPrompts)
	}
	if stats.TotalCalls != 3 {
		t.Errorf("expected 3 API calls from Hermes api_call_count, got %d", stats.TotalCalls)
	}

	sessions, err := db.GetSessions(from, to, "hermes", "")
	if err != nil {
		t.Fatalf("GetSessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 Hermes session, got %d", len(sessions))
	}
	if sessions[0].Source != "hermes" {
		t.Errorf("expected source hermes, got %s", sessions[0].Source)
	}
	if sessions[0].Project != "cli" {
		t.Errorf("expected project cli from Hermes source tag, got %s", sessions[0].Project)
	}

	details, err := db.GetSessionDetail("hermes-sess-1")
	if err != nil {
		t.Fatalf("GetSessionDetail: %v", err)
	}
	if len(details) != 1 {
		t.Fatalf("expected 1 model detail row, got %d", len(details))
	}
	if details[0].Model != "anthropic/claude-sonnet-4.6" {
		t.Errorf("expected Hermes model, got %s", details[0].Model)
	}
	if details[0].Calls != 3 {
		t.Errorf("expected 3 model calls, got %d", details[0].Calls)
	}
	if details[0].CacheRead != 200 {
		t.Errorf("expected cache_read 200, got %d", details[0].CacheRead)
	}
	if details[0].CacheCreate != 50 {
		t.Errorf("expected cache_write 50, got %d", details[0].CacheCreate)
	}
}

func TestHermesCollector_RescanReplacesCumulativeSession(t *testing.T) {
	db := tempDB(t)
	dir := t.TempDir()
	stateDB := filepath.Join(dir, "state.db")
	createHermesStateDB(t, stateDB)

	started := time.Date(2026, 5, 9, 10, 0, 0, 0, time.UTC)
	insertHermesSession(t, stateDB, "hermes-sess-1", started, 100, 50, 0, 0, 0, 1)

	hc := NewHermesCollector(db, []string{stateDB})
	if err := hc.Scan(); err != nil {
		t.Fatalf("Scan 1: %v", err)
	}

	src, err := sql.Open("sqlite", stateDB)
	if err != nil {
		t.Fatalf("open hermes state db: %v", err)
	}
	_, err = src.Exec(`
		UPDATE sessions
		SET input_tokens=300, output_tokens=150, cache_read_tokens=20, api_call_count=2
		WHERE id='hermes-sess-1'`)
	src.Close()
	if err != nil {
		t.Fatalf("update hermes session: %v", err)
	}

	if err := hc.Scan(); err != nil {
		t.Fatalf("Scan 2: %v", err)
	}

	from := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	stats, err := db.GetDashboardStats(from, to, "hermes", "")
	if err != nil {
		t.Fatalf("GetDashboardStats: %v", err)
	}
	if stats.TotalTokens != 470 {
		t.Errorf("expected replacement total 470 tokens, got %d", stats.TotalTokens)
	}
	if stats.TotalCalls != 2 {
		t.Errorf("expected replacement call count 2, got %d", stats.TotalCalls)
	}
	if stats.TotalPrompts != 2 {
		t.Errorf("expected prompts to remain 2 after rescan, got %d", stats.TotalPrompts)
	}
}

func TestHermesCollector_MissingPath(t *testing.T) {
	db := tempDB(t)
	hc := NewHermesCollector(db, []string{"/nonexistent/hermes/state.db"})
	if err := hc.Scan(); err != nil {
		t.Fatalf("Scan should not error on missing path: %v", err)
	}
}

func TestHermesSQLiteReadOnlyDSNConvertsUNCPath(t *testing.T) {
	got := hermesSQLiteReadOnlyDSN(`\\wsl.localhost\Ubuntu\home\wenhui\.hermes\state.db`)
	want := `file://wsl.localhost/Ubuntu/home/wenhui/.hermes/state.db?mode=ro&_pragma=journal_mode(wal)&_pragma=busy_timeout(3000)`
	if got != want {
		t.Fatalf("unexpected DSN:\ngot  %q\nwant %q", got, want)
	}
}

func TestParseHermesWSLPath(t *testing.T) {
	distro, linuxPath, ok := parseHermesWSLPath(`wsl:Ubuntu:/home/wenhui/.hermes/state.db`)
	if !ok {
		t.Fatal("expected WSL path to be recognized")
	}
	if distro != "Ubuntu" {
		t.Fatalf("expected distro Ubuntu, got %q", distro)
	}
	if linuxPath != "/home/wenhui/.hermes/state.db" {
		t.Fatalf("unexpected linux path: %q", linuxPath)
	}

	_, _, ok = parseHermesWSLPath(filepath.Join(t.TempDir(), "state.db"))
	if ok {
		t.Fatal("plain filesystem path should not be treated as WSL")
	}
}

func TestResolveHermesDBPathBacksUpWSLLiveDB(t *testing.T) {
	oldBackup := backupHermesWSLDB
	defer func() { backupHermesWSLDB = oldBackup }()

	tempCopy := filepath.Join(t.TempDir(), "state-copy.db")
	if err := os.WriteFile(tempCopy, []byte("sqlite-copy"), 0600); err != nil {
		t.Fatalf("write temp copy: %v", err)
	}

	var gotDistro, gotLinuxPath string
	cleaned := false
	backupHermesWSLDB = func(distro, linuxPath string) (string, func(), error) {
		gotDistro = distro
		gotLinuxPath = linuxPath
		return tempCopy, func() { cleaned = true }, nil
	}

	resolved, cleanup, err := resolveHermesDBPath(`wsl:Ubuntu:/home/wenhui/.hermes/state.db`)
	if err != nil {
		t.Fatalf("resolveHermesDBPath: %v", err)
	}
	if resolved != tempCopy {
		t.Fatalf("expected temp copy path %q, got %q", tempCopy, resolved)
	}
	if gotDistro != "Ubuntu" || gotLinuxPath != "/home/wenhui/.hermes/state.db" {
		t.Fatalf("unexpected backup args: distro=%q path=%q", gotDistro, gotLinuxPath)
	}
	cleanup()
	if !cleaned {
		t.Fatal("expected cleanup function returned by WSL backup to run")
	}
}
