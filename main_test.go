package main

import (
	"bytes"
	"encoding/json"
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
			name: "multiple objects",
			input: `{"name":"alice","age":30}
{"name":"bob","age":25}`,
			want: `[{"name":"alice","age":30},
{"name":"bob","age":25}]`,
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
			name: "blank lines skipped",
			input: `{"a":1}

{"b":2}`,
			want: `[{"a":1},
{"b":2}]`,
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
			name: "compact multi-value stream",
			input: `{"a":1}
{"b":2}`,
			want: `{"a":1}
{"b":2}`,
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

	t.Run("missing flags returns error", func(t *testing.T) {
		err := jsonToNdjson("", "", false, 1, false)
		if err == nil {
			t.Error("expected error for missing flags")
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