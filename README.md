# json-ndjson

Bidirectional JSON ↔ NDJSON converter with streaming processing.

Convert between JSON files and newline-delimited JSON (NDJSON) without loading the entire dataset into memory. Each mode processes data incrementally — memory usage stays flat regardless of input size.

## Modes

### json2ndjson — Directory of JSON files → NDJSON

```bash
json-to-ndjson json2ndjson -input ./data -output out.ndjson
json-to-ndjson json2ndjson -input ./data -output out.ndjson -delete   # remove source files
json-to-ndjson json2ndjson -input ./data -output out.ndjson -workers 20
```

Reads all `.json` files from a directory, validates and compacts each one, and writes them as NDJSON (one JSON object per line). Each file is processed independently — memory scales with the largest single file, not the total dataset.

### ndjson2json — NDJSON stream → JSON array

```bash
cat data.ndjson | json-to-ndjson ndjson2json > data.json
```

Reads NDJSON from stdin, validates each line, and writes a single JSON array to stdout. Streams line by line — O(1) memory regardless of input size.

### compact — Compact JSON stream

```bash
cat pretty.json | json-to-ndjson compact > compact.json
```

Reads JSON values from stdin and writes compacted JSON to stdout. Uses `json.Decoder` for streaming — handles arbitrarily large inputs without buffering.

## Install

```bash
go build -o json-to-ndjson .
```

Or pull the Docker image:

```bash
docker pull ghcr.io/jlcoulter/json-ndjson:latest
```

## Benchmarks

On an AMD Ryzen 7 9800X3D, processing 10,000 NDJSON lines:

```
BenchmarkNdjsonToJSON-16    	   577	   2034380 ns/op	 2164368 B/op	      15 allocs/op
BenchmarkCompactStream-16   	   176	   6835381 ns/op	 4427097 B/op	   30029 allocs/op
BenchmarkCompactFile-16     	653533	      1852 ns/op	     760 B/op	       5 allocs/op
```

## Test

```bash
go test -v -race ./...
go test -bench=. -benchmem ./...
```

## Docker

Multi-arch images for `linux/amd64` and `linux/arm64`:

```bash
docker build -t json-ndjson .
docker run --rm -v ./data:/data json-ndjson json2ndjson -input /data -output /data/out.ndjson
```

## License

MIT