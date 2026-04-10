package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

func main() {
	inputDir := flag.String("input", "", "Path to input directory containing JSON files")
	outputFile := flag.String("output", "", "Path to output NDJSON file")
	deleteAfter := flag.Bool("delete", false, "Delete JSON files after processing")
	workers := flag.Int("workers", 10, "Number of concurrent workers (e.g. 10000)")

	flag.Parse()

	if *inputDir == "" || *outputFile == "" {
		fmt.Println(
			"Usage: go run main.go -input <input_dir> -output <output_file> [-delete] [-workers N]",
		)
		os.Exit(1)
	}

	out, err := os.OpenFile(*outputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()

	writer := bufio.NewWriter(out)
	defer writer.Flush()

	var writeMutex sync.Mutex

	jobs := make(chan string, *workers*2)
	var wg sync.WaitGroup

	// Workers
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				line, err := processFile(path)
				if err != nil {
					fmt.Printf("Error processing %s: %v\n", path, err)
					continue
				}

				writeMutex.Lock()
				_, werr := writer.Write(append(line, '\n'))
				writeMutex.Unlock()

				if werr != nil {
					fmt.Printf("Write error for %s: %v\n", path, werr)
					continue
				}

				if *deleteAfter {
					if err := os.Remove(path); err != nil {
						fmt.Printf("Delete error for %s: %v\n", path, err)
					}
				}
			}
		}()
	}

	// Walk directory add to channel
	err = filepath.Walk(*inputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if filepath.Ext(path) != ".json" {
			return nil
		}

		jobs <- path
		return nil
	})

	if err != nil {
		fmt.Printf("Error walking directory: %v\n", err)
		os.Exit(1)
	}

	close(jobs)
	wg.Wait()

	fmt.Println("NDJSON file created successfully")
}

func processFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var js any
	if err := json.Unmarshal(data, &js); err != nil {
		return nil, fmt.Errorf("invalid JSON in %s: %w", path, err)
	}

	line, err := json.Marshal(js)
	if err != nil {
		return nil, err
	}

	return line, nil
}
