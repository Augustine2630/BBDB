FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /bbdb ./cmd/bbdb/

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bbdb /bbdb
ENTRYPOINT ["/bbdb", "start"]
