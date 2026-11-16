ARG GO_IMAGE=golang:1.27-alpine3.24
ARG RUNTIME_IMAGE=alpine:3.24
FROM ${GO_IMAGE} AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/miniredis ./cmd/miniredis \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/aofrepair ./cmd/aofrepair

FROM ${RUNTIME_IMAGE}
RUN adduser -D -u 10001 miniredis && mkdir /data && chown miniredis /data
COPY --from=build /out/ /usr/local/bin/
USER miniredis
ENV MINI_REDIS_ADDR=0.0.0.0:8080 MINI_REDIS_AOF_PATH=/data/appendonly.aof
VOLUME ["/data"]
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=2s CMD wget -qO- http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["miniredis"]
