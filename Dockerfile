# syntax=docker/dockerfile:1
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake g++ make \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY CMakeLists.txt ./
COPY include ./include
COPY src ./src
COPY tests ./tests
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --target kv_node router -j"$(nproc)"

FROM debian:bookworm-slim
RUN useradd --system --create-home --home-dir /app kv
WORKDIR /app
COPY --from=build /src/build/kv_node /src/build/router /usr/local/bin/
COPY docker/ /app/docker/
RUN mkdir -p /app/data && chown -R kv:kv /app
USER kv
# Pass the binary and config as the command, e.g.:
#   docker run kv-store kv_node /app/docker/shard1-leader.conf
ENTRYPOINT []
CMD ["kv_node", "/app/docker/shard1-leader.conf"]
