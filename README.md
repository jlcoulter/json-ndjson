# JSON to NDJSON Converter

## Overview

This tool scans a directory of `.json` files, validates and converts each file
into a single-line JSON record, and appends the results into a newline-delimited
JSON (NDJSON) file.

Optionally, the source JSON files can be deleted after successful processing.

---

## Usage

```bash
./json-to-ndjson -input <input_dir> -output <output_file> [-delete] [-workers N]
```

Basic

```bash
./json-to-ndjson -input ./data -output ./output.ndjson
```

High concurrency

```bash
./json-to-ndjson -input ./data -output ./output.ndjson -workers 10000
```

With delete

```bash
./json-to-ndjson -input ./data -output ./output.ndjson -delete
```

---

## Features

- Reads all `.json` files from a directory (recursive)
- Concurrent processing with configurable worker count
- Validates JSON before writing
- Appends to existing NDJSON file (does not overwrite)
- Outputs standard NDJSON format (one JSON object per line)
- Optional cleanup of processed files
- Simple CLI interface

---

## Requirements

- Go 1.18 or newer

---

## Build

```bash
go build -o json-to-ndjson
```
