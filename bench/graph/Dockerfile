FROM clojure:temurin-21-tools-deps-bookworm-slim

WORKDIR /repo
COPY Cargo.toml .
COPY triplox-jvm/ triplox-jvm/
RUN cd triplox-jvm && ./gradlew publishToMavenLocal -x test

WORKDIR /app
COPY bench/graph/deps.edn .
RUN clojure -P
COPY bench/graph/ .
CMD ["clojure", "-M", "-m", "graph.main"]
