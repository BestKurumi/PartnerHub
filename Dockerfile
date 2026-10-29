FROM scratch AS runtime-base
WORKDIR /app
COPY web/ /app/web/
COPY deploy/mysql/ /app/deploy/mysql/
USER 65532:65532
ENTRYPOINT ["/app/hub"]

FROM golang:1.27.2-alpine AS build
WORKDIR /src
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/hub ./cmd/hub && \
    CGO_ENABLED=0 go build -trimpath -o /out/mock ./cmd/mock && \
    CGO_ENABLED=0 go build -trimpath -o /out/migrate ./cmd/migrate && \
    CGO_ENABLED=0 go build -trimpath -o /out/healthprobe ./cmd/healthprobe


FROM runtime-base AS runtime
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/ /app/
