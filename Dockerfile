FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/dsh-manager ./cmd/dsh-manager

FROM alpine:3.22
RUN adduser -D -u 10001 dsh \
    && mkdir -p /data \
    && chown -R dsh:dsh /data
USER dsh
WORKDIR /data
COPY VERSION /usr/local/share/dsh-manager/VERSION
COPY --from=build /out/dsh-manager /usr/local/bin/dsh-manager
EXPOSE 10090 10091
ENTRYPOINT ["/usr/local/bin/dsh-manager"]
