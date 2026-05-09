package collector

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/briqt/agent-usage/internal/storage"
)

// HermesCollector scans Hermes Agent's SQLite state database.
type HermesCollector struct {
	db    *storage.DB
	paths []string
}

// NewHermesCollector creates a HermesCollector that reads state.db files.
func NewHermesCollector(db *storage.DB, paths []string) *HermesCollector {
	return &HermesCollector{db: db, paths: paths}
}

type hermesSessionRow struct {
	id               string
	source           string
	model            string
	startedAt        float64
	lastActiveAt     float64
	inputTokens      int64
	outputTokens     int64
	cacheReadTokens  int64
	cacheWriteTokens int64
	reasoningTokens  int64
	costUSD          float64
	apiCallCount     int
}

// Scan reads all configured Hermes state databases.
func (c *HermesCollector) Scan() error {
	for _, configuredPath := range c.paths {
		dbPath, cleanup, err := resolveHermesDBPath(configuredPath)
		if err != nil {
			log.Printf("hermes: cannot prepare %s: %v", configuredPath, err)
			continue
		}
		func() {
			defer cleanup()
			if _, err := os.Stat(dbPath); err != nil {
				log.Printf("hermes: cannot read %s: %v", configuredPath, err)
				return
			}
			if err := c.processDB(dbPath); err != nil {
				log.Printf("hermes: error processing %s: %v", configuredPath, err)
			}
		}()
	}
	return nil
}

var backupHermesWSLDB = defaultBackupHermesWSLDB

func parseHermesWSLPath(path string) (string, string, bool) {
	if !strings.HasPrefix(strings.ToLower(path), "wsl:") {
		return "", "", false
	}
	rest := path[len("wsl:"):]
	parts := strings.SplitN(rest, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func resolveHermesDBPath(path string) (string, func(), error) {
	distro, linuxPath, ok := parseHermesWSLPath(path)
	if !ok {
		return path, func() {}, nil
	}
	return backupHermesWSLDB(distro, linuxPath)
}

func defaultBackupHermesWSLDB(distro, linuxPath string) (string, func(), error) {
	tmp, err := os.CreateTemp("", "agent-usage-hermes-wsl-*.db")
	if err != nil {
		return "", nil, fmt.Errorf("create temp hermes db: %w", err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return "", nil, fmt.Errorf("close temp hermes db: %w", err)
	}

	wslDst, err := windowsPathToWSLPath(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return "", nil, err
	}

	script := fmt.Sprintf(`
import pathlib
import sqlite3

src = %q
dst = %q
pathlib.Path(dst).unlink(missing_ok=True)
source = sqlite3.connect("file:" + src + "?mode=ro", uri=True)
target = sqlite3.connect(dst)
source.backup(target)
target.close()
source.close()
`, linuxPath, wslDst)

	cmd := exec.Command("wsl.exe", "-d", distro, "--", "python3", "-c", script)
	if out, err := cmd.CombinedOutput(); err != nil {
		os.Remove(tmpPath)
		return "", nil, fmt.Errorf("backup WSL hermes db: %w: %s", err, strings.TrimSpace(string(out)))
	}

	cleanup := func() {
		os.Remove(tmpPath)
		os.Remove(tmpPath + "-wal")
		os.Remove(tmpPath + "-shm")
	}
	return tmpPath, cleanup, nil
}

func windowsPathToWSLPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve windows path: %w", err)
	}
	volume := filepath.VolumeName(abs)
	if len(volume) < 2 || volume[1] != ':' {
		return "", fmt.Errorf("cannot convert non-drive path to WSL path: %s", path)
	}
	drive := strings.ToLower(string(volume[0]))
	rest := strings.TrimPrefix(abs[len(volume):], `\`)
	rest = strings.ReplaceAll(rest, `\`, `/`)
	if rest == "" {
		return "/mnt/" + drive, nil
	}
	return "/mnt/" + drive + "/" + rest, nil
}

func (c *HermesCollector) processDB(dbPath string) error {
	srcDB, err := sql.Open("sqlite", hermesSQLiteReadOnlyDSN(dbPath))
	if err != nil {
		return fmt.Errorf("open hermes db: %w", err)
	}
	defer srcDB.Close()

	hasSessions, err := sqliteHasTable(srcDB, "sessions")
	if err != nil {
		return fmt.Errorf("check hermes sessions table: %w", err)
	}
	if !hasSessions {
		return fmt.Errorf("missing sessions table")
	}

	hasMessages, err := sqliteHasTable(srcDB, "messages")
	if err != nil {
		return fmt.Errorf("check hermes messages table: %w", err)
	}
	costExpr := "0"
	hasEstimatedCost := sqliteHasColumn(srcDB, "sessions", "estimated_cost_usd")
	hasActualCost := sqliteHasColumn(srcDB, "sessions", "actual_cost_usd")
	if hasEstimatedCost {
		costExpr = "COALESCE(s.estimated_cost_usd, 0)"
	}
	if hasActualCost && hasEstimatedCost {
		costExpr = "COALESCE(s.actual_cost_usd, s.estimated_cost_usd, 0)"
	} else if hasActualCost {
		costExpr = "COALESCE(s.actual_cost_usd, 0)"
	}

	apiCallExpr := "0"
	if sqliteHasColumn(srcDB, "sessions", "api_call_count") {
		apiCallExpr = "COALESCE(s.api_call_count, 0)"
	}

	lastActiveExpr := "s.started_at"
	if sqliteHasColumn(srcDB, "sessions", "ended_at") {
		lastActiveExpr = "COALESCE(s.ended_at, s.started_at)"
	}
	if hasMessages {
		lastActiveExpr = "COALESCE((SELECT MAX(m.timestamp) FROM messages m WHERE m.session_id = s.id), " + lastActiveExpr + ")"
	}

	rows, err := srcDB.Query(fmt.Sprintf(`
		SELECT s.id, s.source, COALESCE(s.model, ''), s.started_at, %s,
			COALESCE(s.input_tokens, 0), COALESCE(s.output_tokens, 0),
			COALESCE(s.cache_read_tokens, 0), COALESCE(s.cache_write_tokens, 0),
			COALESCE(s.reasoning_tokens, 0), %s, %s
		FROM sessions s
		ORDER BY s.started_at`, lastActiveExpr, costExpr, apiCallExpr))
	if err != nil {
		return fmt.Errorf("query hermes sessions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var hs hermesSessionRow
		if err := rows.Scan(&hs.id, &hs.source, &hs.model, &hs.startedAt, &hs.lastActiveAt,
			&hs.inputTokens, &hs.outputTokens, &hs.cacheReadTokens, &hs.cacheWriteTokens,
			&hs.reasoningTokens, &hs.costUSD, &hs.apiCallCount); err != nil {
			return fmt.Errorf("scan hermes session: %w", err)
		}
		if err := c.replaceSession(srcDB, hasMessages, hs); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (c *HermesCollector) replaceSession(srcDB *sql.DB, hasMessages bool, hs hermesSessionRow) error {
	if hs.id == "" {
		return nil
	}
	if hs.inputTokens == 0 && hs.outputTokens == 0 && hs.cacheReadTokens == 0 && hs.cacheWriteTokens == 0 && hs.reasoningTokens == 0 && hs.costUSD == 0 {
		return nil
	}

	model := hs.model
	if model == "" {
		model = "unknown"
	}
	calls := hs.apiCallCount
	if calls <= 0 {
		calls = 1
	}

	events, err := collectHermesPromptEvents(srcDB, hasMessages, hs.id)
	if err != nil {
		return err
	}

	session := &storage.SessionRecord{
		Source:    "hermes",
		SessionID: hs.id,
		Project:   hs.source,
		StartTime: unixSecondsToTime(hs.startedAt),
		Prompts:   len(events),
	}
	record := &storage.UsageRecord{
		Source:                   "hermes",
		SessionID:                hs.id,
		Model:                    model,
		Calls:                    calls,
		Timestamp:                unixSecondsToTime(hs.lastActiveAt),
		Project:                  hs.source,
		InputTokens:              hs.inputTokens,
		OutputTokens:             hs.outputTokens,
		CacheReadInputTokens:     hs.cacheReadTokens,
		CacheCreationInputTokens: hs.cacheWriteTokens,
		ReasoningOutputTokens:    hs.reasoningTokens,
		CostUSD:                  hs.costUSD,
	}

	return c.db.ReplaceSessionData("hermes", hs.id, session, []*storage.UsageRecord{record}, events)
}

func collectHermesPromptEvents(srcDB *sql.DB, hasMessages bool, sessionID string) ([]*storage.PromptEvent, error) {
	if !hasMessages {
		return nil, nil
	}

	rows, err := srcDB.Query(`SELECT timestamp FROM messages WHERE session_id=? AND role='user' ORDER BY timestamp`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("query hermes prompts: %w", err)
	}
	defer rows.Close()

	var events []*storage.PromptEvent
	for rows.Next() {
		var ts float64
		if err := rows.Scan(&ts); err != nil {
			return nil, fmt.Errorf("scan hermes prompt: %w", err)
		}
		events = append(events, &storage.PromptEvent{
			Source:    "hermes",
			SessionID: sessionID,
			Timestamp: unixSecondsToTime(ts),
		})
	}
	return events, rows.Err()
}

func sqliteHasTable(db *sql.DB, name string) (bool, error) {
	var found string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&found)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func hermesSQLiteReadOnlyDSN(path string) string {
	if strings.HasPrefix(path, `\\`) {
		uriPath := strings.ReplaceAll(path, `\`, `/`)
		uriPath = strings.TrimPrefix(uriPath, `//`)
		return "file://" + uriPath + "?mode=ro&_pragma=journal_mode(wal)&_pragma=busy_timeout(3000)"
	}
	return path + "?mode=ro&_pragma=journal_mode(wal)&_pragma=busy_timeout(3000)"
}

func sqliteHasColumn(db *sql.DB, table, column string) bool {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return false
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, pk int
		var defaultValue interface{}
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return false
		}
		if name == column {
			return true
		}
	}
	return false
}

func unixSecondsToTime(v float64) time.Time {
	whole, frac := math.Modf(v)
	return time.Unix(int64(whole), int64(frac*1e9)).UTC()
}
