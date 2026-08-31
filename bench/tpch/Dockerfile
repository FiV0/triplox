FROM clojure:temurin-21-tools-deps-bookworm-slim
WORKDIR /app
COPY deps.edn .
RUN clojure -P
COPY . .
CMD ["clojure", "-M", "-m", "tpch.main"]
