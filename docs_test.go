package main

import (
	"flag"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The README's configuration table is the first thing a user reads, and a flag
// added or renamed without touching it turns the documentation into a lie.
// These tests walk the real flag set and the real README, so the two cannot
// drift apart silently.

// readmeFlagRow matches a row of the configuration table:
// | `-flag` | `ENV_VAR` | `default` | Description |
var readmeFlagRow = regexp.MustCompile(`^\|\s*` + "`-([a-z-]+)`" + `[^|]*\|([^|]*)\|([^|]*)\|`)

// documentedFlag is one row of the README table, normalised.
type documentedFlag struct {
	env     string
	defwant string
}

// readREADMEFlags parses the configuration table out of the README.
func readREADMEFlags(t *testing.T) map[string]documentedFlag {
	t.Helper()
	raw, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("read README: %v", err)
	}

	inTable := false
	out := map[string]documentedFlag{}
	for _, line := range strings.Split(string(raw), "\n") {
		// The configuration table is the one with this exact header.
		if strings.HasPrefix(line, "| Flag | Env var | Default | Description |") {
			inTable = true
			continue
		}
		if inTable && !strings.HasPrefix(line, "|") {
			break
		}
		m := readmeFlagRow.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		out[m[1]] = documentedFlag{
			env:     firstBackticked(m[2]),
			defwant: firstBackticked(m[3]),
		}
	}
	if len(out) == 0 {
		t.Fatal("no configuration table found in README.md")
	}
	return out
}

// firstBackticked returns the first `code span` in a table cell, or "" when the
// cell only carries prose such as _(empty)_.
func firstBackticked(cell string) string {
	if m := regexp.MustCompile("`([^`]+)`").FindStringSubmatch(cell); m != nil {
		return m[1]
	}
	return ""
}

// envFromUsage pulls the environment variable out of a flag's usage string,
// which always ends in "(env: NAME...)".
func envFromUsage(usage string) string {
	m := regexp.MustCompile(`\(env: ([A-Z_]+)`).FindStringSubmatch(usage)
	if m == nil {
		return ""
	}
	return m[1]
}

// pristineFlagSet registers the flags with no environment set, so the defaults
// are the ones the README documents.
func pristineFlagSet(t *testing.T) *flag.FlagSet {
	t.Helper()
	original := lookupEnv
	lookupEnv = func(string) (string, bool) { return "", false }
	t.Cleanup(func() { lookupEnv = original })

	fs := flag.NewFlagSet("cloud-tasks-emulator", flag.ContinueOnError)
	registerFlags(fs)
	return fs
}

func TestREADMEDocumentsEveryFlag(t *testing.T) {
	documented := readREADMEFlags(t)

	seen := map[string]bool{}
	pristineFlagSet(t).VisitAll(func(f *flag.Flag) {
		seen[f.Name] = true
		doc, ok := documented[f.Name]
		if !ok {
			t.Errorf("flag -%s is not in the README configuration table", f.Name)
			return
		}
		if env := envFromUsage(f.Usage); env != doc.env {
			t.Errorf("flag -%s: README says env %q, the flag's usage says %q", f.Name, doc.env, env)
		}
		// An empty default is written as prose (_(none)_ / _(empty)_) rather
		// than as a code span, so only compare when the flag has one.
		if f.DefValue != "" && f.DefValue != doc.defwant {
			t.Errorf("flag -%s: README says default %q, the flag's default is %q", f.Name, doc.defwant, f.DefValue)
		}
	})

	for name := range documented {
		if !seen[name] {
			t.Errorf("README documents -%s, which the emulator does not accept", name)
		}
	}
}

// TestREADMEFlagsAppearInMigrationTable keeps the aertje flag-mapping table
// honest about the flags this emulator actually has.
func TestREADMEFlagsAppearInMigrationTable(t *testing.T) {
	raw, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("read README: %v", err)
	}
	migration := string(raw)
	if i := strings.Index(migration, "### Flag mapping"); i >= 0 {
		migration = migration[i:]
	} else {
		t.Fatal("no '### Flag mapping' section in README.md")
	}
	if end := strings.Index(migration, "\n### "); end > 0 {
		migration = migration[:end]
	}

	pristineFlagSet(t).VisitAll(func(f *flag.Flag) {
		// -host and -port share a row, so match on the bare name.
		if !strings.Contains(migration, "`-"+f.Name+"`") {
			t.Errorf("flag -%s is missing from the migration flag-mapping table", f.Name)
		}
	})
}

// markdownLink matches an inline markdown link target, dropping any anchor.
var markdownLink = regexp.MustCompile(`\[[^\]]*\]\(([^)#\s]+)(?:#[^)]*)?\)`)

// TestMarkdownLinksResolve keeps the docs navigable: a renamed or deleted file
// should fail the build rather than leave a dead link in the README.
func TestMarkdownLinksResolve(t *testing.T) {
	var docs []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "node_modules") {
			return fs.SkipDir
		}
		if !d.IsDir() && strings.HasSuffix(path, ".md") {
			docs = append(docs, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(docs) == 0 {
		t.Fatal("no markdown files found")
	}

	for _, doc := range docs {
		raw, err := os.ReadFile(doc)
		if err != nil {
			t.Fatalf("read %s: %v", doc, err)
		}
		for _, m := range markdownLink.FindAllStringSubmatch(string(raw), -1) {
			target := m[1]
			if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") || strings.HasPrefix(target, "mailto:") {
				continue
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(doc), target)); err != nil {
				t.Errorf("%s links to %s, which does not exist", doc, target)
			}
		}
	}
}
