package logger_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestStaticLogMessages_RepoHasZeroViolations(t *testing.T) {
	var violations []string
	for _, root := range []string{
		filepath.Join("..", "..", "cmd"),
		filepath.Join("..", "..", "internal"),
	} {
		violations = append(violations, checkStaticLogCallsInTree(t, root)...)
	}
	if len(violations) > 0 {
		t.Fatalf("non-static structured log messages detected:\n%s", strings.Join(violations, "\n"))
	}
}

func TestStaticLogMessages_CheckerDetectsPositiveAndNegativeCases(t *testing.T) {
	cases := []struct {
		name      string
		files     map[string]string
		wantCount int
		wantSub   string
	}{
		{
			name: "valid_literals_concat_cross_file_const_and_package_funcs",
			files: map[string]string{
				"consts.go": `package sample
const (
	baseMsg = "worker"
	fullMsg = baseMsg + " started"
)
`,
				"main.go": `package sample
import (
	"net/http"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)
func run(log *zap.Logger, w http.ResponseWriter, err error) {
	const localMsg = "step " + "completed"
	_ = err.Error()
	http.Error(w, err.Error(), http.StatusBadRequest)
	log.Info(fullMsg, zap.Error(err))
	log.Warn(localMsg)
	log.Log(zapcore.InfoLevel, "explicit " + "level")
	_ = log.Check(zapcore.DebugLevel, "checked")
}
`,
			},
			wantCount: 0,
		},
		{
			name: "sibling_file_without_zap_import_dynamic_sprintf",
			files: map[string]string{
				"server.go": `package sample
import "go.uber.org/zap"
type S struct { log *zap.Logger }
`,
				"handler.go": `package sample
import "fmt"
func (s *S) handle(id string) {
	s.log.Info(fmt.Sprintf("created %s", id))
}
`,
			},
			wantCount: 1,
			wantSub:   "Info message must be a non-empty static string",
		},
		{
			name: "cross_package_logger_new_without_zap_import",
			files: map[string]string{
				"main.go": `package main
import "github.com/awu123-create/ferry/internal/logger"
func run(dyn string) {
	log, _ := logger.New("info")
	log.Warn(dyn)
}
`,
			},
			wantCount: 1,
			wantSub:   "Warn message must be a non-empty static string",
		},
		{
			name: "local_var_shadowing_package_const_rejected",
			files: map[string]string{
				"consts.go": `package sample
const msg = "static"
`,
				"run.go": `package sample
import "fmt"
type L interface { Info(string) }
func f(l L, x int) {
	msg := fmt.Sprintf("dynamic %d", x)
	l.Info(msg)
}
`,
			},
			wantCount: 1,
			wantSub:   "Info message must be a non-empty static string",
		},
		{
			name: "local_var_shadowing_imported_pkg_ident_caught",
			files: map[string]string{
				"run.go": `package sample
import "github.com/awu123-create/ferry/internal/logger"
type L interface { Error(string) }
func f(l L, dyn string) {
	_ = logger.WithTaskID
	logger := l
	logger.Error(dyn)
}
`,
			},
			wantCount: 1,
			wantSub:   "Error message must be a non-empty static string",
		},
		{
			name: "check_and_whitespace_only_message_rejected",
			files: map[string]string{
				"run.go": `package sample
type L interface {
	Info(string)
	Check(int, string) bool
}
func f(l L, dyn string) {
	l.Info("   ")
	_ = l.Check(1, dyn)
}
`,
			},
			wantCount: 2,
			wantSub:   "Check message must be a non-empty static string",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, src := range tc.files {
				full := filepath.Join(dir, name)
				if err := os.WriteFile(full, []byte(src), 0o600); err != nil {
					t.Fatalf("write %s: %v", full, err)
				}
			}
			got := checkStaticLogCallsInTree(t, dir)
			if len(got) != tc.wantCount {
				t.Fatalf("expected %d violations, got %d:\n%s", tc.wantCount, len(got), strings.Join(got, "\n"))
			}
			if tc.wantSub != "" && !strings.Contains(strings.Join(got, "\n"), tc.wantSub) {
				t.Fatalf("expected violation containing %q, got:\n%s", tc.wantSub, strings.Join(got, "\n"))
			}
		})
	}
}

func checkStaticLogCallsInTree(t *testing.T, root string) []string {
	t.Helper()

	dirFiles := make(map[string][]string)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		dir := filepath.Dir(path)
		dirFiles[dir] = append(dirFiles[dir], path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}

	fset := token.NewFileSet()
	var violations []string

	for _, paths := range dirFiles {
		parsed := make([]*ast.File, 0, len(paths))
		pkgConsts := make(map[string]ast.Expr)

		for _, p := range paths {
			file, parseErr := parser.ParseFile(fset, p, nil, 0)
			if parseErr != nil {
				t.Fatalf("parse %s: %v", p, parseErr)
			}
			parsed = append(parsed, file)
			collectTopLevelConstExprs(file, pkgConsts)
		}

		for _, file := range parsed {
			importedPkgs := collectImportedPkgNames(file)

			ast.Inspect(file, func(n ast.Node) bool {
				if sel, isSel := n.(*ast.SelectorExpr); isSel && sel.Sel.Name == "Sugar" {
					if pkgIdent, isIdent := sel.X.(*ast.Ident); !isIdent || pkgIdent.Obj != nil || !importedPkgs[pkgIdent.Name] {
						pos := fset.Position(sel.Pos())
						violations = append(violations, pos.String()+": Sugar logger is forbidden; use structured zap.Field")
						return true
					}
				}
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				// 未被局部变量遮蔽的包级函数调用（如 zap.Error(err)、http.Error(...)）不是 Logger 方法调用。
				if pkgIdent, isIdent := sel.X.(*ast.Ident); isIdent && pkgIdent.Obj == nil && importedPkgs[pkgIdent.Name] {
					return true
				}

				var msgIdx int
				switch sel.Sel.Name {
				case "Debug", "Info", "Warn", "Error", "DPanic", "Panic", "Fatal":
					if len(call.Args) == 0 {
						return true
					}
					msgIdx = 0
				case "Log", "Check":
					if len(call.Args) < 2 {
						return true
					}
					msgIdx = 1
				default:
					return true
				}

				val, ok := evalStaticString(call.Args[msgIdx], pkgConsts, map[string]bool{})
				if !ok || strings.TrimSpace(val) == "" {
					pos := fset.Position(call.Args[msgIdx].Pos())
					violations = append(violations, pos.String()+": "+sel.Sel.Name+" message must be a non-empty static string literal or constant")
				}
				return true
			})
		}
	}

	return violations
}

func collectTopLevelConstExprs(file *ast.File, out map[string]ast.Expr) {
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		var prevValues []ast.Expr
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			values := vs.Values
			if len(values) == 0 {
				values = prevValues
			} else {
				prevValues = values
			}
			for i, name := range vs.Names {
				if i < len(values) {
					out[name.Name] = values[i]
				}
			}
		}
	}
}

func collectImportedPkgNames(file *ast.File) map[string]bool {
	out := make(map[string]bool, len(file.Imports))
	for _, imp := range file.Imports {
		pkgPath, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			continue
		}
		localName := filepath.Base(pkgPath)
		if imp.Name != nil {
			if imp.Name.Name == "_" || imp.Name.Name == "." {
				continue
			}
			localName = imp.Name.Name
		}
		out[localName] = true
	}
	return out
}

func evalStaticString(expr ast.Expr, pkgConsts map[string]ast.Expr, visiting map[string]bool) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", false
		}
		val, err := strconv.Unquote(e.Value)
		if err != nil {
			return "", false
		}
		return val, true
	case *ast.ParenExpr:
		return evalStaticString(e.X, pkgConsts, visiting)
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			return "", false
		}
		left, okL := evalStaticString(e.X, pkgConsts, visiting)
		right, okR := evalStaticString(e.Y, pkgConsts, visiting)
		if !okL || !okR {
			return "", false
		}
		return left + right, true
	case *ast.Ident:
		if e.Obj != nil {
			if e.Obj.Kind != ast.Con {
				return "", false
			}
			vs, ok := e.Obj.Decl.(*ast.ValueSpec)
			if !ok {
				return "", false
			}
			for i, name := range vs.Names {
				if name.Name == e.Name && i < len(vs.Values) {
					key := "local:" + e.Name
					if visiting[key] {
						return "", false
					}
					visiting[key] = true
					defer delete(visiting, key)
					return evalStaticString(vs.Values[i], pkgConsts, visiting)
				}
			}
			if len(vs.Values) > 0 {
				return "", false
			}
		}
		rhs, ok := pkgConsts[e.Name]
		if !ok {
			return "", false
		}
		key := "pkg:" + e.Name
		if visiting[key] {
			return "", false
		}
		visiting[key] = true
		defer delete(visiting, key)
		return evalStaticString(rhs, pkgConsts, visiting)
	default:
		return "", false
	}
}
