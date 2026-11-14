# syntax=docker/dockerfile:1.7
#
# Build multi-stage para o sistema MC714.
#
#   - builder : golang:1.27-alpine (compatível com o go.mod local), compila
#               /out/node e /out/client; cria /data/wal com dono 65532 para
#               que o distroless (nonroot) possa escrever no volume WAL.
#   - app-node   : distroless/static-debian12:nonroot, roda /node
#   - app-client : distroless/static-debian12:nonroot, roda /client

FROM golang:1.27-alpine AS builder

WORKDIR /src

# Cria /data/wal com dono 65532 (nonroot) NO BUILDER — distroless não tem shell
# para fazer isso depois. Ao montar um volume nomeado em /data/wal, o Docker
# copia este diretório (com ownership) para o volume na primeira criação.
RUN mkdir -p /data/wal && chown -R 65532:65532 /data

COPY go.mod ./

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ENV CGO_ENABLED=0 GOOS=linux
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -trimpath -ldflags="-s -w" -o /out/node ./cmd/node \
 && go build -trimpath -ldflags="-s -w" -o /out/client ./cmd/client

# ---- app-node ----
FROM gcr.io/distroless/static-debian12:nonroot AS app-node

# Copia /data com ownership 65532 para o estágio final. Isso faz o volume
# nomeado herdar a permissão correta na primeira montagem.
COPY --from=builder --chown=65532:65532 /data /data

COPY --from=builder /out/node /node
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
USER nonroot:nonroot
EXPOSE 8000 8080
ENTRYPOINT ["/node"]

# ---- app-client ----
FROM gcr.io/distroless/static-debian12:nonroot AS app-client
COPY --from=builder /out/client /client
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
USER nonroot:nonroot
ENTRYPOINT ["/client"]
