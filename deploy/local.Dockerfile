# Optional fallback only; first build Linux binaries with make build-local-image.
FROM scratch
WORKDIR /app
COPY web/ /app/web/
COPY deploy/mysql/ /app/deploy/mysql/
COPY .local/bin/hub .local/bin/mock .local/bin/migrate .local/bin/healthprobe /app/
USER 65532:65532
ENTRYPOINT ["/app/hub"]
