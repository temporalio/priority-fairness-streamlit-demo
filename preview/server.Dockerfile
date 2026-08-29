FROM golang:1.26.4-bookworm@sha256:b305420a68d0f229d91eb3b3ed9e519fcf2cf5461da4bef997bf927e8c0bfd2b AS builder

ARG API_COMMIT=8615aa77180cfbe1ed1413c0f67579100d1c739c
ARG API_GO_COMMIT=e54fd69950e119eaf0df6d31dfa795467e66f910
ARG SERVER_COMMIT=0d77be1f0b2531d1bc14228a45e18636e9d3c100

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN go install github.com/bufbuild/buf/cmd/buf@v1.47.2

WORKDIR /src
RUN git init api \
    && git -C api remote add origin https://github.com/dnr/api.git \
    && git -C api fetch --depth=1 origin "${API_COMMIT}" \
    && git -C api checkout --detach FETCH_HEAD

COPY preview/buf.gen.yaml /src/api/buf.gen.preview.yaml
RUN cd /src/api \
    && buf generate \
        --template buf.gen.preview.yaml \
        --path temporal/api/taskqueue/v1/message.proto \
        --path temporal/api/workflowservice/v1/request_response.proto

RUN git init api-go \
    && git -C api-go remote add origin https://github.com/temporalio/api-go.git \
    && git -C api-go fetch --depth=1 origin "${API_GO_COMMIT}" \
    && git -C api-go checkout --detach FETCH_HEAD \
    && cp api/.generated/temporal/api/taskqueue/v1/message.pb.go api-go/taskqueue/v1/message.pb.go \
    && cp api/.generated/temporal/api/workflowservice/v1/request_response.pb.go api-go/workflowservice/v1/request_response.pb.go

RUN git init temporal \
    && git -C temporal remote add origin https://github.com/dnr/temporal.git \
    && git -C temporal fetch --depth=1 origin "${SERVER_COMMIT}" \
    && git -C temporal checkout --detach FETCH_HEAD \
    && cd temporal \
    && go mod edit -replace=go.temporal.io/api=/src/api-go

WORKDIR /src/temporal
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -trimpath -o /out/temporal-server ./cmd/server

FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171

LABEL org.opencontainers.image.source="https://github.com/temporalio/priority-fairness-streamlit-demo" \
      org.opencontainers.image.description="Experimental Temporal task queue concurrency preview server" \
      org.opencontainers.image.revision="0d77be1f0b2531d1bc14228a45e18636e9d3c100"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/temporal
COPY --from=builder /out/temporal-server ./temporal-server
COPY preview/server.yaml ./config/server.yaml
COPY preview/dynamicconfig.yaml ./config/dynamicconfig.yaml

EXPOSE 7233 7243

ENTRYPOINT ["/opt/temporal/temporal-server"]
CMD ["--config-file", "/opt/temporal/config/server.yaml", "--allow-no-auth", "start"]
