FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /json-to-ndjson .

FROM alpine:3.21

RUN apk add --no-cache ca-certificates
COPY --from=builder /json-to-ndjson /usr/local/bin/json-to-ndjson

ENTRYPOINT ["json-to-ndjson"]