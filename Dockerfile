FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/dsh-manager ./cmd/dsh-manager

FROM alpine:3.22
RUN adduser -D -u 10001 dsh
USER dsh
WORKDIR /data
COPY --from=build /out/dsh-manager /usr/local/bin/dsh-manager
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/dsh-manager"]
