# Quickstart

Two commands from a clone (copy `deploy/compose/.env.example` to `.env` first):

```text
git clone https://github.com/SunilMaurya-18/lakewire.git
docker compose -f deploy/compose/compose.yaml --env-file deploy/compose/.env up -d
```

Brokers listen on `localhost:9092` (zone a), `9192` (b), `9292` (c). Admin is `:8080`.

Produce and consume with kcat:

```text
echo "hello" | kcat -P -b localhost:9092 -t orders
kcat -C -b localhost:9092 -t orders -e
```

Create the topic first if auto-create is off (it is):

```text
bin/lakewire.cmd topics create orders -p 1 --retention 7d --bootstrap localhost:9092
```

## `lakewire dev` (no Docker)

```text
.\mvnw.cmd -pl lakewire-cli -am package -DskipTests
bin\lakewire.cmd dev
```

Ready prints `bootstrap=localhost:9092`. First start unpacks Zonky Postgres (often >10 s); later starts are faster (A57).
