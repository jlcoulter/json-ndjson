package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	startTime := time.Now()

	dir := flag.String("input", "", "Input directory (json2ndjson/ndjson2files mode)")
	output := flag.String("output", "", "Output path (json2ndjson: NDJSON file; ndjson2files: output directory)")
	deleteAfter := flag.Bool("delete", false, "Delete source files after successful processing")
	workers := flag.Int("workers", 10, "Concurrent workers (json2ndjson mode)")
	debug := flag.Bool("d", false, "Enable debug logging")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <mode> [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Modes:\n")
		fmt.Fprintf(os.Stderr, "  json2ndjson    Directory of .json files → single NDJSON file\n")
		fmt.Fprintf(os.Stderr, "  ndjson2json    NDJSON stream (stdin) → JSON array (stdout)\n")
		fmt.Fprintf(os.Stderr, "  ndjson2files   NDJSON stream (stdin) → directory of .json files\n")
		fmt.Fprintf(os.Stderr, "  compact        JSON stream (stdin) → compacted JSON (stdout)\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s json2ndjson -input ./data -output out.ndjson\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s json2ndjson -input ./data -output out.ndjson -delete\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  cat data.ndjson | %s ndjson2json > data.json\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  cat data.ndjson | %s ndjson2files -output ./split\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  cat data.json | %s compact > compacted.json\n", os.Args[0])
	}

	flag.Parse()

	mode := "json2ndjson"
	if args := flag.Args(); len(args) > 0 {
		mode = args[0]
	}

	var err error
	switch mode {
	case "json2ndjson":
		err = jsonToNdjson(*dir, *output, *deleteAfter, *workers, *debug)
	case "ndjson2json":
		err = ndjsonToJSON(os.Stdin, os.Stdout)
	case "ndjson2files":
		err = ndjsonToFiles(os.Stdin, *output)
	case "compact":
		err = compactStream(os.Stdin, os.Stdout)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", mode)
		flag.Usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "completed in %s\n", time.Since(startTime).Round(time.Millisecond))
}

// --- json2ndjson ---

// jsonToNdjson reads all .json files from a directory, validates and compacts
// each one, and writes them as NDJSON (one JSON object per line) to output.
// Files are processed concurrently but written in sorted filename order so
// output is deterministic. Memory scales with the largest single file.
func jsonToNdjson(dir, output string, deleteAfter bool, workers int, debug bool) error {
	if dir == "" || output == "" {
		return fmt.Errorf("-input and -output are required for json2ndjson mode")
	}

	// Collect and sort file paths for deterministic output order.
	var paths []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".json" {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk directory: %w", err)
	}
	sort.Strings(paths)

	if len(paths) == 0 {
		return fmt.Errorf("no .json files found in %s", dir)
	}

	out, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer out.Close()

	writer := bufio.NewWriter(out)
	defer writer.Flush()

	type result struct {
		line []byte
		path string
	}

	jobs := make(chan string, workers*2)
	results := make(chan result, workers*2)

	// Workers compact files concurrently.
	var wg sync.WaitGroup
	var failed atomic.Uint64
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				line, err := compactFile(path)
				if err != nil {
					failed.Add(1)
					fmt.Fprintf(os.Stderr, "error processing %s: %v\n", path, err)
					continue
				}
				results <- result{line: line, path: path}
			}
		}()
	}

	// Feed sorted paths to workers.
	go func() {
		for _, p := range paths {
			jobs <- p
		}
		close(jobs)
	}()

	// Close results channel when all workers are done.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results, preserving sorted order.
	// Map from original path to compacted line.
	ordered := make([][]byte, 0, len(paths))
	collected := make(map[string][]byte, len(paths))
	for r := range results {
		collected[r.path] = r.line
		if debug {
			fmt.Fprintf(os.Stderr, "processed: %s\n", filepath.Base(r.path))
		}
	}

	// Write in sorted order.
	for _, p := range paths {
		if line, ok := collected[p]; ok {
			ordered = append(ordered, line)
		}
	}

	for i, line := range ordered {
		writer.Write(line)
		if i < len(ordered)-1 {
			writer.WriteByte('\n')
		}
	}
	if len(ordered) > 0 {
		writer.WriteByte('\n')
	}

	// Only delete source files after successful write.
	if deleteAfter {
		// Flush before deleting so data is on disk.
		if err := writer.Flush(); err != nil {
			return fmt.Errorf("flush output: %w", err)
		}
		if err := out.Sync(); err != nil {
			return fmt.Errorf("sync output: %w", err)
		}
		var deleteErrors int
		for _, p := range paths {
			if _, ok := collected[p]; !ok {
				continue // skip files that failed to compact
			}
			if err := os.Remove(p); err != nil {
				fmt.Fprintf(os.Stderr, "warn: could not delete %s: %v\n", p, err)
				deleteErrors++
			}
		}
		if deleteErrors > 0 {
			fmt.Fprintf(os.Stderr, "warning: %d files could not be deleted\n", deleteErrors)
		}
	}

	processed := uint64(len(ordered))
	fmt.Fprintf(os.Stderr, "processed: %d, failed: %d\n", processed, failed.Load())
	return nil
}

// compactFile reads a single JSON file and returns its compacted form.
func compactFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("empty file: %s", filepath.Base(path))
	}

	var buf bytes.Buffer
	if err := json.Compact(&buf, data); err != nil {
		return nil, fmt.Errorf("invalid JSON in %s: %w", filepath.Base(path), err)
	}
	return buf.Bytes(), nil
}

// --- ndjson2json ---

// ndjsonToJSON reads NDJSON from r and writes a JSON array to w.
// Each line must be a valid JSON object. Streams line by line — memory scales
// with the longest line, not the total input size.
func ndjsonToJSON(r io.Reader, w io.Writer) error {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	writer := bufio.NewWriter(w)
	defer writer.Flush()

	writer.WriteByte('[')

	first := true
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		if !json.Valid(line) {
			return fmt.Errorf("invalid JSON on line %d: %s", lineNum, truncate(line, 80))
		}

		if !first {
			writer.WriteString(",\n")
		}
		first = false

		writer.Write(line)
	}

	writer.WriteByte(']')
	writer.WriteByte('\n')

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading input: %w", err)
	}
	return nil
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "..."
	}
	return string(b)
}

// --- ndjson2files ---

// ndjsonToFiles reads NDJSON from r and writes each line as a separate .json
// file in the output directory. Each line must be a valid JSON object.
// Files are named 0001.json, 0002.json, etc.
func ndjsonToFiles(r io.Reader, outputDir string) error {
	if outputDir == "" {
		return fmt.Errorf("-output directory is required for ndjson2files mode")
	}

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	lineNum := 0
	written := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		if !json.Valid(line) {
			return fmt.Errorf("invalid JSON on line %d: %s", lineNum, truncate(line, 80))
		}

		// Compact to normalize formatting.
		var compacted bytes.Buffer
		if err := json.Compact(&compacted, line); err != nil {
			return fmt.Errorf("compact line %d: %w", lineNum, err)
		}

		written++
		filename := filepath.Join(outputDir, fmt.Sprintf("%04d.json", written))
		if err := os.WriteFile(filename, compacted.Bytes(), 0644); err != nil {
			return fmt.Errorf("write %s: %w", filename, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading input: %w", err)
	}

	fmt.Fprintf(os.Stderr, "wrote %d files to %s\n", written, outputDir)
	return nil
}

// --- compact ---

// compactStream reads top-level JSON values from r and writes each compacted
// value to w. Memory scales with the largest single top-level value — this
// streams between values but each value is buffered entirely.
func compactStream(r io.Reader, w io.Writer) error {
	dec := json.NewDecoder(r)

	first := true
	for dec.More() {
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return fmt.Errorf("decode: %w", err)
		}

		if !first {
			fmt.Fprintln(w)
		}
		first = false

		// json.RawMessage preserves the value; Compact normalizes whitespace.
		var compacted bytes.Buffer
		if err := json.Compact(&compacted, v); err != nil {
			return fmt.Errorf("compact: %w", err)
		}
		fmt.Fprint(w, compacted.String())
	}
	return nil
}