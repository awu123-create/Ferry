package config_test

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/awu123-create/ferry/internal/config"
)

func TestLoadServerConfig_Defaults(t *testing.T) {
	cfg, err := config.LoadServerConfig()
	if err != nil {
		t.Fatalf("LoadServerConfig returned unexpected error: %v", err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.MetricsAddr != ":2112" {
		t.Fatalf("unexpected default addrs: http=%q metrics=%q", cfg.HTTPAddr, cfg.MetricsAddr)
	}
	if cfg.DefaultMaxAttempts != 1 {
		t.Fatalf("expected Milestone 1 default max attempts = 1, got %d", cfg.DefaultMaxAttempts)
	}
	if cfg.DefaultTaskWindow != 24*time.Hour {
		t.Fatalf("expected default task window = 24h, got %v", cfg.DefaultTaskWindow)
	}
	if cfg.OutboxBatchSize != 100 || cfg.OutboxPollInterval != 500*time.Millisecond || cfg.OutboxBatchTimeout != 5*time.Second {
		t.Fatalf("unexpected outbox defaults: %+v", cfg)
	}
}

func TestLoadServerConfig_ValidationErrors(t *testing.T) {
	t.Run("invalid_max_attempts", func(t *testing.T) {
		t.Setenv("FERRY_DEFAULT_MAX_ATTEMPTS", "25")
		if _, err := config.LoadServerConfig(); err == nil {
			t.Fatal("expected error for FERRY_DEFAULT_MAX_ATTEMPTS=25")
		}
	})

	t.Run("invalid_duration", func(t *testing.T) {
		t.Setenv("FERRY_OUTBOX_POLL_INTERVAL", "not-a-duration")
		if _, err := config.LoadServerConfig(); err == nil {
			t.Fatal("expected error for invalid FERRY_OUTBOX_POLL_INTERVAL")
		}
	})

	t.Run("empty_kafka_brokers", func(t *testing.T) {
		t.Setenv("FERRY_KAFKA_BROKERS", " , ")
		if _, err := config.LoadServerConfig(); err == nil {
			t.Fatal("expected error for empty FERRY_KAFKA_BROKERS")
		}
	})
}

func TestLoadWorkerConfig_DefaultsAndOverrides(t *testing.T) {
	t.Setenv("FERRY_WORKER_CONCURRENCY", "16")
	t.Setenv("FERRY_SHUTDOWN_DRAIN_TIMEOUT", "10s")

	cfg, err := config.LoadWorkerConfig()
	if err != nil {
		t.Fatalf("LoadWorkerConfig returned unexpected error: %v", err)
	}
	if cfg.WorkerConcurrency != 16 {
		t.Fatalf("expected WorkerConcurrency=16, got %d", cfg.WorkerConcurrency)
	}
	if cfg.ShutdownDrainTimeout != 10*time.Second {
		t.Fatalf("expected ShutdownDrainTimeout=10s, got %v", cfg.ShutdownDrainTimeout)
	}
	if cfg.MaxResponseSnippetBytes != 1024 {
		t.Fatalf("expected MaxResponseSnippetBytes=1024, got %d", cfg.MaxResponseSnippetBytes)
	}
}

func TestLoadWorkerConfig_ValidationErrors(t *testing.T) {
	t.Setenv("FERRY_WORKER_CONCURRENCY", "0")
	if _, err := config.LoadWorkerConfig(); err == nil {
		t.Fatal("expected error for FERRY_WORKER_CONCURRENCY=0")
	}
}

func TestLoadReceiverConfig_OverridesAndValidation(t *testing.T) {
	t.Setenv("FERRY_RECEIVER_DEFAULT_STATUS", "400")
	t.Setenv("FERRY_RECEIVER_DEFAULT_DELAY", "50ms")

	cfg, err := config.LoadReceiverConfig()
	if err != nil {
		t.Fatalf("LoadReceiverConfig returned unexpected error: %v", err)
	}
	if cfg.DefaultStatus != 400 || cfg.DefaultDelay != 50*time.Millisecond {
		t.Fatalf("unexpected receiver config: %+v", cfg)
	}

	t.Setenv("FERRY_RECEIVER_DEFAULT_STATUS", "999")
	if _, err := config.LoadReceiverConfig(); err == nil {
		t.Fatal("expected error for invalid FERRY_RECEIVER_DEFAULT_STATUS")
	}
}

func TestEnvExample_MatchesConfigAndTestEnvKeys(t *testing.T) {
	expected := collectConfigEnvKeys(t, "config.go")
	for _, dir := range []string{
		filepath.Join("..", "..", "cmd"),
		filepath.Join("..", "..", "internal"),
		filepath.Join("..", "..", "tests"),
	} {
		for k := range collectTestEnvKeys(t, dir) {
			expected[k] = struct{}{}
		}
	}

	actual := parseEnvExampleKeys(t, filepath.Join("..", "..", ".env.example"))

	var missing, extra []string
	for k := range expected {
		if _, ok := actual[k]; !ok {
			missing = append(missing, k)
		}
	}
	for k := range actual {
		if _, ok := expected[k]; !ok {
			extra = append(extra, k)
		}
	}
	slices.Sort(missing)
	slices.Sort(extra)

	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf(".env.example drifted from code: missing=%v extra=%v", missing, extra)
	}
}

func collectConfigEnvKeys(t *testing.T, filename string) map[string]struct{} {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}

	keys := make(map[string]struct{})
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		if !isEnvLookupCall(call.Fun) {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		key, err := strconv.Unquote(lit.Value)
		if err != nil {
			t.Fatalf("%s: unquote %s: %v", filename, lit.Value, err)
		}
		if strings.HasPrefix(key, "FERRY_") {
			keys[key] = struct{}{}
		}
		return true
	})

	if len(keys) == 0 {
		t.Fatalf("expected non-empty FERRY_* keys parsed from %s", filename)
	}
	return keys
}

func isEnvLookupCall(fn ast.Expr) bool {
	switch f := fn.(type) {
	case *ast.Ident:
		return strings.HasPrefix(f.Name, "env")
	case *ast.SelectorExpr:
		pkg, ok := f.X.(*ast.Ident)
		return ok && pkg.Name == "os" && (f.Sel.Name == "Getenv" || f.Sel.Name == "LookupEnv")
	default:
		return false
	}
}

func collectTestEnvKeys(t *testing.T, root string) map[string]struct{} {
	t.Helper()

	keys := make(map[string]struct{})
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
		}
		var firstErr error
		ast.Inspect(file, func(n ast.Node) bool {
			if firstErr != nil {
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "os" || (sel.Sel.Name != "Getenv" && sel.Sel.Name != "LookupEnv") {
				return true
			}
			lit, ok := call.Args[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			key, unquoteErr := strconv.Unquote(lit.Value)
			if unquoteErr != nil {
				firstErr = unquoteErr
				return false
			}
			if strings.HasPrefix(key, "FERRY_TEST_") {
				keys[key] = struct{}{}
			}
			return true
		})
		return firstErr
	})
	if err != nil {
		t.Fatalf("walk %s for FERRY_TEST_* env keys: %v", root, err)
	}
	return keys
}

func parseEnvExampleKeys(t *testing.T, path string) map[string]struct{} {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			t.Errorf("close %s: %v", path, closeErr)
		}
	}()

	keys := make(map[string]struct{})
	scanner := bufio.NewScanner(f)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, _, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(key) == "" {
			t.Fatalf("%s:%d: expected KEY=VALUE entry, got %q", path, lineNo, line)
		}
		key = strings.TrimSpace(key)
		if _, dup := keys[key]; dup {
			t.Fatalf("%s:%d: duplicate env key %q", path, lineNo, key)
		}
		keys[key] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return keys
}
