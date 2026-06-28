package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNdjsonToJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "single object",
			input: `{"name":"alice","age":30}`,
			want:  `[{"name":"alice","age":30}]`,
		},
		{
			name:  "multiple objects",
			input: "{\"name\":\"alice\",\"age\":30}\n{\"name\":\"bob\",\"age\":25}",
			want:  "[{\"name\":\"alice\",\"age\":30},\n{\"name\":\"bob\",\"age\":25}]",
		},
		{
			name:  "empty input",
			input: "",
			want:  `[]`,
		},
		{
			name:    "invalid JSON line",
			input:   `{broken`,
			wantErr: true,
		},
		{
			name:  "blank lines skipped",
			input: "{\"a\":1}\n\n{\"b\":2}",
			want:  "[{\"a\":1},\n{\"b\":2}]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := ndjsonToJSON(strings.NewReader(tt.input), &out)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got := strings.TrimSpace(out.String())
			want := strings.TrimSpace(tt.want)
			if got != want {
				t.Errorf("ndjsonToJSON()\ngot:  %s\nwant: %s", got, want)
			}
		})
	}
}

func TestNdjsonToJSONReportsLineNumber(t *testing.T) {
	input := "{\"ok\":1}\n{bad"
	var out bytes.Buffer
	err := ndjsonToJSON(strings.NewReader(input), &out)
	if err == nil {
		t.Fatal("expected error for invalid line 2")
	}
	if !strings.Contains(err.Error(), "line 2") {
		t.Errorf("error should mention line number: %v", err)
	}
}

func TestCompactStream(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "compact pretty JSON",
			input: `{"name": "alice", "age": 30}`,
			want:  `{"name":"alice","age":30}`,
		},
		{
			name:  "compact multi-value stream",
			input: "{\"a\":1}\n{\"b\":2}",
			want:  "{\"a\":1}\n{\"b\":2}",
		},
		{
			name:    "invalid JSON",
			input:   `{broken`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			err := compactStream(strings.NewReader(tt.input), &out)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got := strings.TrimSpace(out.String())
			want := strings.TrimSpace(tt.want)
			if got != want {
				t.Errorf("compactStream()\ngot:  %s\nwant: %s", got, want)
			}
		})
	}
}

func TestCompactFile(t *testing.T) {
	t.Run("valid JSON file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.json")
		content := `{"name": "alice", "age": 30}`
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}

		got, err := compactFile(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		want := `{"name":"alice","age":30}`
		if string(got) != want {
			t.Errorf("compactFile() = %q, want %q", string(got), want)
		}
	})

	t.Run("empty file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "empty.json")
		if err := os.WriteFile(path, []byte(""), 0644); err != nil {
			t.Fatal(err)
		}

		_, err := compactFile(path)
		if err == nil {
			t.Error("expected error for empty file, got nil")
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "bad.json")
		if err := os.WriteFile(path, []byte(`{broken`), 0644); err != nil {
			t.Fatal(err)
		}

		_, err := compactFile(path)
		if err == nil {
			t.Error("expected error for invalid JSON, got nil")
		}
	})
}

func TestJsonToNdjson(t *testing.T) {
	t.Run("processes directory of JSON files", func(t *testing.T) {
		dir := t.TempDir()

		files := map[string]string{
			"a.json": `{"name": "alice", "age": 30}`,
			"b.json": `{"name": "bob", "age": 25}`,
		}
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
				t.Fatal(err)
			}
		}

		output := filepath.Join(t.TempDir(), "out.ndjson")
		err := jsonToNdjson(dir, output, false, 2, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		data, err := os.ReadFile(output)
		if err != nil {
			t.Fatal(err)
		}

		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) != 2 {
			t.Fatalf("expected 2 lines, got %d", len(lines))
		}

		for _, line := range lines {
			if !json.Valid([]byte(line)) {
				t.Errorf("invalid JSON line: %s", line)
			}
		}
	})

	t.Run("output is deterministic (sorted)", func(t *testing.T) {
		dir := t.TempDir()

		for i := 0; i < 10; i++ {
			name := filepath.Join(dir, string(rune('a'+i))+".json")
			content := fmt.Sprintf(`{"id": %d}`, i)
			if err := os.WriteFile(name, []byte(content), 0644); err != nil {
				t.Fatal(err)
			}
		}

		output := filepath.Join(t.TempDir(), "out.ndjson")

		// Run twice, expect identical output.
		err := jsonToNdjson(dir, output, false, 4, false)
		if err != nil {
			t.Fatal(err)
		}
		first, _ := os.ReadFile(output)

		err = jsonToNdjson(dir, output, false, 4, false)
		if err != nil {
			t.Fatal(err)
		}
		second, _ := os.ReadFile(output)

		if string(first) != string(second) {
			t.Errorf("output not deterministic:\nfirst:  %s\nsecond: %s", first, second)
		}
	})

	t.Run("missing flags returns error", func(t *testing.T) {
		err := jsonToNdjson("", "", false, 1, false)
		if err == nil {
			t.Error("expected error for missing flags")
		}
	})

	t.Run("empty directory returns error", func(t *testing.T) {
		dir := t.TempDir()
		output := filepath.Join(t.TempDir(), "out.ndjson")
		err := jsonToNdjson(dir, output, false, 1, false)
		if err == nil {
			t.Error("expected error for empty directory")
		}
	})

	t.Run("delete after success", func(t *testing.T) {
		dir := t.TempDir()

		for i := 0; i < 3; i++ {
			name := filepath.Join(dir, string(rune('a'+i))+".json")
			if err := os.WriteFile(name, []byte(`{"x":1}`), 0644); err != nil {
				t.Fatal(err)
			}
		}

		output := filepath.Join(t.TempDir(), "out.ndjson")
		err := jsonToNdjson(dir, output, true, 2, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Source files should be deleted.
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Errorf("expected empty directory after -delete, got %d entries", len(entries))
		}

		// Output should still exist and be valid.
		data, err := os.ReadFile(output)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) != 3 {
			t.Errorf("expected 3 output lines, got %d", len(lines))
		}
	})
}

func TestNdjsonToFiles(t *testing.T) {
	t.Run("splits NDJSON into individual files", func(t *testing.T) {
		input := "{\"name\":\"alice\",\"age\":30}\n{\"name\":\"bob\",\"age\":25}\n"
		outDir := filepath.Join(t.TempDir(), "split")

		err := ndjsonToFiles(strings.NewReader(input), outDir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Should create 0001.json and 0002.json.
		f1 := filepath.Join(outDir, "0001.json")
		f2 := filepath.Join(outDir, "0002.json")

		data1, err := os.ReadFile(f1)
		if err != nil {
			t.Fatalf("missing file %s: %v", f1, err)
		}
		data2, err := os.ReadFile(f2)
		if err != nil {
			t.Fatalf("missing file %s: %v", f2, err)
		}

		if !json.Valid(data1) {
			t.Errorf("invalid JSON in 0001.json: %s", data1)
		}
		if !json.Valid(data2) {
			t.Errorf("invalid JSON in 0002.json: %s", data2)
		}
	})

	t.Run("compacts output files", func(t *testing.T) {
		input := "{ \"name\" : \"alice\" , \"age\" : 30 }\n"
		outDir := filepath.Join(t.TempDir(), "split")

		err := ndjsonToFiles(strings.NewReader(input), outDir)
		if err != nil {
			t.Fatal(err)
		}

		data, _ := os.ReadFile(filepath.Join(outDir, "0001.json"))
		// Should be compact (no spaces around colons).
		want := `{"name":"alice","age":30}`
		if strings.TrimSpace(string(data)) != want {
			t.Errorf("expected compact JSON %q, got %q", want, strings.TrimSpace(string(data)))
		}
	})

	t.Run("skips blank lines", func(t *testing.T) {
		input := "{\"a\":1}\n\n{\"b\":2}\n"
		outDir := filepath.Join(t.TempDir(), "split")

		err := ndjsonToFiles(strings.NewReader(input), outDir)
		if err != nil {
			t.Fatal(err)
		}

		entries, _ := os.ReadDir(outDir)
		if len(entries) != 2 {
			t.Errorf("expected 2 files, got %d", len(entries))
		}
	})

	t.Run("rejects invalid JSON", func(t *testing.T) {
		input := "{broken\n"
		outDir := filepath.Join(t.TempDir(), "split")

		err := ndjsonToFiles(strings.NewReader(input), outDir)
		if err == nil {
			t.Error("expected error for invalid JSON")
		}
	})

	t.Run("requires output directory", func(t *testing.T) {
		err := ndjsonToFiles(strings.NewReader("{}"), "")
		if err == nil {
			t.Error("expected error for missing output dir")
		}
	})

	t.Run("empty input creates directory but no files", func(t *testing.T) {
		outDir := filepath.Join(t.TempDir(), "split")
		err := ndjsonToFiles(strings.NewReader(""), outDir)
		if err != nil {
			t.Fatal(err)
		}

		entries, _ := os.ReadDir(outDir)
		if len(entries) != 0 {
			t.Errorf("expected 0 files for empty input, got %d", len(entries))
		}
	})
}

// Benchmarks

func BenchmarkNdjsonToJSON(b *testing.B) {
	line := `{"id":1,"name":"alice","email":"alice@example.com","active":true,"score":98.6}` + "\n"
	var input strings.Builder
	for i := 0; i < 10000; i++ {
		input.WriteString(line)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out bytes.Buffer
		ndjsonToJSON(strings.NewReader(input.String()), &out)
	}
}

func BenchmarkNdjsonToFiles(b *testing.B) {
	line := `{"id":1,"name":"alice","email":"alice@example.com","active":true,"score":98.6}` + "\n"
	var input strings.Builder
	for i := 0; i < 1000; i++ {
		input.WriteString(line)
	}
	inputStr := input.String()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		outDir := filepath.Join(b.TempDir(), "split")
		b.StartTimer()
		ndjsonToFiles(strings.NewReader(inputStr), outDir)
	}
}

func BenchmarkCompactStream(b *testing.B) {
	obj := `{"id":1,"name":"alice","email":"alice@example.com","active":true,"score":98.6}` + "\n"
	var input strings.Builder
	for i := 0; i < 10000; i++ {
		input.WriteString(obj)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out bytes.Buffer
		compactStream(strings.NewReader(input.String()), &out)
	}
}

func BenchmarkCompactFile(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.json")
	obj := map[string]interface{}{
		"id":     1,
		"name":   "alice",
		"email":  "alice@example.com",
		"active": true,
		"score":  98.6,
	}
	data, _ := json.Marshal(obj)
	os.WriteFile(path, data, 0644)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		compactFile(path)
	}
}