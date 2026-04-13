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

    inputDir := flag.String("input", "", "Path to input directory containing JSON files")
    outputFile := flag.String("output", "", "Path to output NDJSON file")
    deleteAfter := flag.Bool("delete", false, "Delete JSON files after processing")
    workers := flag.Int("workers", 10, "Number of concurrent workers")
    debug := flag.Bool("d", false, "Enable debug logging (ADDED resource logs)")

    flag.Parse()

    if *inputDir == "" || *outputFile == "" {
        fmt.Println("Usage: go run main.go -input <input_dir> -output <output_file> [-delete] [-workers N] [-d]")
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

    var (
        writeMutex sync.Mutex
        wg         sync.WaitGroup
    )

    var processed uint64
    var failed uint64

    jobs := make(chan string, *workers*2)

    // Workers
    for i := 0; i < *workers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for path := range jobs {
                line, err := processFile(path)
                if err != nil {
                    atomic.AddUint64(&failed, 1)
                    fmt.Printf("ERROR processing %s: %v\n", path, err)
                    continue
                }

                writeMutex.Lock()
                _, werr := writer.Write(line)
                if werr == nil {
                    _, werr = writer.Write([]byte{'\n'})
                }
                writeMutex.Unlock()

                if werr != nil {
                    atomic.AddUint64(&failed, 1)
                    fmt.Printf("ERROR writing %s: %v\n", path, werr)
                    continue
                }

                atomic.AddUint64(&processed, 1)

                if *debug {
                    fmt.Printf("ADDED resource: %s\n", filepath.Base(path))
                }

                if *deleteAfter {
                    if err := os.Remove(path); err != nil {
                        fmt.Printf("WARN delete failed %s: %v\n", path, err)
                    }
                }
            }
        }()
    }

    // Walk input directory
    err = filepath.Walk(*inputDir, func(path string, info os.FileInfo, err error) error {
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
        fmt.Printf("Error walking directory: %v\n", err)
        os.Exit(1)
    }

    close(jobs)
    wg.Wait()

    elapsed := time.Since(startTime)

    fmt.Println("--------------------------------------------------")
    fmt.Println("NDJSON creation complete")
    fmt.Printf("Resources processed: %d\n", processed)
    fmt.Printf("Resources failed   : %d\n", failed)
    fmt.Printf("Time taken         : %s\n", elapsed.Round(time.Millisecond))
    fmt.Println("--------------------------------------------------")
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

    trimmed := bytes.TrimSpace(data)
    if len(trimmed) == 0 {
        return nil, fmt.Errorf("empty file")
    }

    var compacted bytes.Buffer
    if err := json.Compact(&compacted, trimmed); err != nil {
        return nil, fmt.Errorf("invalid JSON: %w", err)
    }
    return compacted.Bytes(), nil
}
