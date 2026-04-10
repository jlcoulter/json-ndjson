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
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	startTime := time.Now()

	dir := flag.String("input", "", "Directory of .json files to convert (json2ndjson mode)")
	output := flag.String("output", "", "Output file path (json2ndjson mode)")
	deleteAfter := flag.Bool("delete", false, "Delete source files after processing")
	workers := flag.Int("workers", 10, "Concurrent workers (json2ndjson mode)")
	debug := flag.Bool("d", false, "Enable debug logging")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [mode] [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Modes (first positional argument):\n")
		fmt.Fprintf(os.Stderr, "  json2ndjson   Convert directory of .json files to NDJSON (default)\n")
		fmt.Fprintf(os.Stderr, "  ndjson2json   Convert NDJSON stream to JSON array\n")
		fmt.Fprintf(os.Stderr, "  compact       Compact JSON stream (stdin → stdout)\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s json2ndjson -input ./data -output out.ndjson\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  cat data.ndjson | %s ndjson2json > data.json\n", os.Args[0])
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

// jsonToNdjson reads all .json files from a directory, validates and compacts
// each one, and writes them as NDJSON (one JSON object per line).
// Each file is read and compacted independently — memory usage is proportional
// to the largest single input file, not the total dataset size.
func jsonToNdjson(dir, output string, deleteAfter bool, workers int, debug bool) error {
	if dir == "" || output == "" {
		return fmt.Errorf("-input and -output are required for json2ndjson mode")
	}

	out, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer out.Close()

	writer := bufio.NewWriter(out)
	defer writer.Flush()

	var (
		writeMu   sync.Mutex
		wg         sync.WaitGroup
		processed atomic.Uint64
		failed     atomic.Uint64
	)

	jobs := make(chan string, workers*2)

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

				writeMu.Lock()
				writer.Write(line)
				writer.Write([]byte{'\n'})
				writeMu.Unlock()

				processed.Add(1)

				if debug {
					fmt.Fprintf(os.Stderr, "processed: %s\n", filepath.Base(path))
				}

				if deleteAfter {
					if err := os.Remove(path); err != nil {
						fmt.Fprintf(os.Stderr, "warn: could not delete %s: %v\n", path, err)
					}
				}
			}
		}()
	}

	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		jobs <- path
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk directory: %w", err)
	}

	close(jobs)
	wg.Wait()

	fmt.Fprintf(os.Stderr, "processed: %d, failed: %d\n", processed.Load(), failed.Load())
	return nil
}

// compactFile reads a single JSON file and returns its compacted form.
// Memory usage is proportional to the file size — each file is processed independently.
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

	trimmed := data
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("empty file: %s", filepath.Base(path))
	}

	var buf bytes.Buffer
	if err := json.Compact(&buf, trimmed); err != nil {
		return nil, fmt.Errorf("invalid JSON in %s: %w", filepath.Base(path), err)
	}
	return buf.Bytes(), nil
}

// ndjsonToJSON reads NDJSON from r and writes a JSON array to w.
// Streams line by line — memory usage is O(1) regardless of input size.
func ndjsonToJSON(r io.Reader, w io.Writer) error {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	writer := bufio.NewWriter(w)
	defer writer.Flush()

	writer.WriteByte('[')

	first := true
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Validate each line is valid JSON
		if !json.Valid(line) {
			return fmt.Errorf("invalid JSON line: %s", string(line[:min(len(line), 80)]))
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

// compactStream reads JSON from r and writes compacted JSON to w.
// Uses streaming json.Decoder — O(1) memory for any input size.
func compactStream(r io.Reader, w io.Writer) error {
	dec := json.NewDecoder(r)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "")

	for dec.More() {
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		if err := enc.Encode(v); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
	}
	return nil
}