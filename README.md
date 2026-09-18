# Food Ordering API

Go server for a food-ordering challenge. Spec: `openapi.yaml`.

**Problem.** Build a robust Go API that matches the OpenAPI 3.1 spec as closely
as possible. A client must be able to list the menu, fetch one product, and
place an order (optional promo). `POST /order` requires header `api_key: apitest`.
Promo codes live in three gzip dumps (`data/couponbase1.gz`, `couponbase2.gz`, `couponbase3.gz`). A code is valid only if it is 8–10 characters and appears in **at least two** of those files. Examples: `HAPPYHRS` and `FIFTYOFF` are valid `SUPER100` is not. Handle edge cases the demo API skips.

The interesting part of this implementation is not the three HTTP routes. It is
how coupons are converted offline into persistent Binary Fuse indexes and then
loaded by every API replica without rereading the dumps.

## What the API does

| Method | Path | Auth | Result |
| ---  | --- | --- | --- |
| GET  | `/product` | none | full menu |
| GET  | `/product/{productId}` | none | one item, or 400 / 404 |
| POST | `/order` | header `api_key: apitest` | create order, optional coupon |

Missing key → 401. Wrong key → 403. Bad JSON → 400. Empty cart / unknown
product → 422.

## Coupon rules

A code is valid only when all of these are true:

1. Length is 8, 9 or 10 characters.
2. It appears in **at least two different** gzip files.
3. Repeats inside the **same** file count as one hit.

Examples used by the tests:

| Code | Files | Result |
| --- | --- | --- |
| `HAPPYHRS` | 1 and 2 | valid, 10% off |
| `FIFTYOFF` | 2 and 3 | valid, 10% off |
| `SUPER100` | 1 only | invalid, order still placed, `$0` discount |

`couponCode` is optional in the spec, so an invalid code does not fail the
order. It is treated as “no promo”. If product later wants a hard 422, that
change lives in `promo.FileEngine`, not in the HTTP handler.

The spec never defines the discount amount. This build uses a flat 10% so
orders have a deterministic `discounts` / `total`. Per-code amounts
(`FIFTYOFF` → 50%) can be plugged in through `promo.DiscountFunc`.

The real coupon dumps are hundreds of MB each (not committed). Download them
into `data/` before running against production-sized files:

```bash
curl -fL -o data/couponbase1.gz \
  https://orderfoodonline-files.s3.ap-southeast-2.amazonaws.com/couponbase1.gz
curl -fL -o data/couponbase2.gz \
  https://orderfoodonline-files.s3.ap-southeast-2.amazonaws.com/couponbase2.gz
curl -fL -o data/couponbase3.gz \
  https://orderfoodonline-files.s3.ap-southeast-2.amazonaws.com/couponbase3.gz
```

## Coupon index architecture

The original dumps are described as large gzip text files. Scanning them on
every `POST /order`, or rebuilding a large map in every API process, would not
survive traffic.

Offline build (`cmd/coupon-index-builder`):

1. Stream each gzip file line by line. Do not hold decompressed text.
2. Keep only alphanumeric tokens of length 8–10. That covers both
   “one code per line” samples and noisy “random text” dumps.
3. Hash, externally sort, and deduplicate in bounded chunks. A repeated code in
   one file therefore remains one key.
4. Build one `BinaryFuse16` per dataset, sequentially, and serialize it.
5. Write counts, byte sizes, SHA-256 checksums, hash/fingerprint versions, and
   all three filenames to one manifest.
6. Atomically rename the completed temporary directory to its version name.

Request path:

1. `promo.Validator` checks coupon format.
2. It asks each of three `Membership` implementations whether the code could
   be present.
3. At least two possible matches are required.
4. `promo.FileEngine` independently calculates the current flat 10% discount.

The API loads exactly one manifest/version and verifies every filter's size,
SHA-256, and internal shape before serving. Missing, mixed, unsupported, or
corrupt indexes fail startup. Loaded filters are immutable, so concurrent
lookups need no mutex.

### Why BinaryFuse16

Binary Fuse is a compact, fast read-only membership structure suitable for a
large static base. This implementation uses the maintained
`github.com/FastFilter/xorfilter` package instead of reimplementing the
algorithm.

`BinaryFuse8` uses about 9 bits/key but has an approximately 0.39% false-positive
rate. `BinaryFuse16` uses about 18 bits/key and lowers that to approximately
0.0015%. Discounts have financial impact, so this implementation chooses
BinaryFuse16. A positive lookup still means **possibly present**, never proof.

For a code absent from all three datasets, two accidental matches are required,
but the nominal rates should not simply be multiplied because filter outcomes
may be correlated. More importantly, a code genuinely present in one dataset
needs only one false match in either remaining filter to pass 2-of-3; the union
bound is approximately 0.003%. Production systems requiring exact acceptance
should configure an authoritative `ExactMembership` confirmer
(DB/Redis/campaign service). The layered lookup first applies exact delta
overrides, rejects a definite base miss, then confirms a possible base match
when a confirmer is configured.

### Base + delta

`LayeredMembership` supports a small exact `DeltaStore` in front of each
immutable base. Delta entries can add or revoke coupons immediately. Periodic
offline compaction folds the delta into a new base version. `MapDelta` is
provided for local use; Redis or a database can implement the same small
interface without changing coupon policy, order service, handlers, or discount
calculation.

### Memory and construction

The real data is not included, so 100 million unique coupons per dataset is the
planning example:

- BinaryFuse16 final size: about 18 bits/key, approximately 225 MB
  (215 MiB) per dataset, or approximately 644 MiB for three loaded filters.
- BinaryFuse8 would be approximately half that size, with the higher
  false-positive rate described above.
- A Go `map[string]int` has runtime-, load-factor-, and key-allocation-dependent
  overhead. At roughly 40–70 bytes per union entry, 100 million entries can
  consume about 4–7 GB; a mostly disjoint 300-million-key union can be much
  larger. Measure against the actual Go release and data distribution.
- Construction is not as small as the final filter. For 100 million keys,
  xorfilter needs the 800 MB hash slice plus approximately 2.6 GB of builder
  arrays and fingerprints—roughly 3.4 GB decimal before Go runtime overhead.
  External-sort chunks default to one million hashes (about 8 MB), but the final
  deduplicated hash slice must still fit in RAM. Datasets are deliberately built
  one at a time, keeping peak construction near one dataset rather than three.

Construction belongs in a memory-sized offline job. API replicas only pay final
filter memory and load time.

Do not index every sliding 8–10 gram of a multi-GB random blob. That grows
with file size and will not fit in RAM. Tokenizing on non-alphanumeric
boundaries is the practical tradeoff for this problem.

## Layout

```
internal/handler      HTTP only. Status codes, JSON, no coupon math.
internal/service      Order / product rules. Talks to interfaces.
internal/promo        Coupon policy, layered membership, persistence, discount.
internal/repository   In-memory stores. Same interfaces can wrap SQL later.
internal/middleware   API key, request id, logs, panic recovery.
internal/domain       Request/response models.
cmd/server            Process entrypoint.
cmd/coupon-index-builder  Offline versioned index generator.
data/indexes/<version>   Generated manifest + three immutable filters.
```

In-memory products and orders are enough for the challenge. For a real
catalog / order history, replace the repository implementations. Handlers
do not change.

## Run with Go

Needs Go 1.22+. Download the source dumps as shown above, then build an
immutable version from the repository root:

```bash
go run ./cmd/coupon-index-builder \
  --input ./data \
  --output ./data/indexes \
  --version v1

go run ./cmd/server
```

The server listens on `:8080` and defaults to `./data/indexes/v1`. Select one
complete version with `COUPON_INDEX_DIR`; for example:

```bash
COUPON_INDEX_DIR=./data/indexes/v2 go run ./cmd/server
```

Build `v2` beside `v1`, distribute that complete directory through object or
shared storage, then update each replica's configuration and restart/roll it.
Because the version directory is immutable and one manifest names all datasets,
an instance cannot load a v1/v2 mixture. No distributed deployment mechanism
is included.

Imports use the module name in `go.mod` (`food-order-api/...`), not Uber paths.
Anyone who clones this repo can `go run` without extra GOPATH setup.

If you publish to GitHub and want `go install github.com/<you>/food-order-api/...`
to work, change the first line of `go.mod` to that path and rename the
`food-order-api/...` imports to match. Local `go run ./cmd/server` does not
need that.

## Run with Docker

All commands below run from the repository root (the folder with `Dockerfile`):

### Option A: plain Docker (works everywhere)

```bash
docker build -t food-order-api .
docker run --rm -p 8080:8080 --name food-order-api food-order-api
```

Stop with Ctrl+C, or from another shell:

```bash
docker stop food-order-api
```

### Option B: Compose

Compose ships in two forms. Use whichever your machine has:

```bash
# Compose v2 (plugin, newer Docker Desktop)
docker compose up --build

# Compose v1 (standalone binary, note the hyphen)
docker-compose up --build
```

If `docker compose up --build` fails with `unknown flag: --build`, your Docker
CLI has no Compose plugin. Use the hyphenated `docker-compose` command, or
Option A.

Check which one you have:

```bash
docker compose version || docker-compose version
```

Stop with Ctrl+C, then:

```bash
docker compose down or docker-compose down
```

### Verify it works

```bash
curl -s localhost:8080/product

curl -s localhost:8080/product/10

# valid coupon: 10% off -> discounts 2.66, total 23.94
curl -s -X POST localhost:8080/order \
  -H 'Content-Type: application/json' \
  -H 'api_key: apitest' \
  -d '{"couponCode":"HAPPYHRS","items":[{"productId":"10","quantity":2}]}'

# invalid coupon: order still placed, discounts 0, total 26.60
curl -s -X POST localhost:8080/order \
  -H 'Content-Type: application/json' \
  -H 'api_key: apitest' \
  -d '{"couponCode":"SUPER100","items":[{"productId":"10","quantity":2}]}'

# missing api_key -> 401
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/order \
  -d '{"items":[{"productId":"10","quantity":1}]}'
```

Build `data/indexes/v1` before building the image. The image sets
`COUPON_INDEX_DIR=/app/data/indexes/v1` and copies `data/` in.

## Tests and benchmarks

```bash
go test ./...
go test -race ./...
go test ./internal/promo -run '^$' -bench . -benchmem
```

The benchmark uses 100,000 synthetic, fixed-length coupon codes; it is not a
claim about the unavailable production dumps. Run it on deployment-class
hardware for capacity planning.

Measured on an Apple M1 Pro with Go 1.24.5 (`-count=3` ranges):

- exact map membership: 11.76–12.02 ns/op, 0 allocations
- BinaryFuse16 membership: 19.66–19.76 ns/op, 0 allocations
- 2-of-3 validation: 52.95–53.03 ns/op, 0 allocations
- 100k exact map construction: 4.80–5.06 ms and about 3.50 MB allocated,
  excluding the prebuilt string storage
- 100k pre-hashed BinaryFuse16 construction: 3.94–4.19 ms and about 3.50 MB
  allocated (temporary construction allocation, not retained filter size)
- loading three persisted 100k-key indexes with integrity verification:
  1.16–1.41 ms and about 818 KB allocated

The exact map benchmark excludes string generation/backing memory because its
synthetic strings are prepared before the timer. It is useful for lookup
latency, not a complete map RAM measurement. End-to-end gzip parsing and
external-sort time depends strongly on source compressibility and storage I/O.

## Remaining production limitations

- The server currently runs in probabilistic-only mode. Exact confirmer and
  delta interfaces are implemented, but no Redis/DB/campaign adapter or config
  wiring is included because this repository has no authoritative source.
- Filters are loaded into Go heap memory. The selected library supports
  serialization but not a supported memory-mapped representation.
- Versions are selected at process start; there is no hot reload, distributed
  rollout, or object-storage client. Deploying a version is an operational
  concern outside this local API.
- xxHash64 collisions are possible and are separate from Binary Fuse false
  positives. An exact confirmer is needed where financial policy requires
  certainty.
- The library uses 32-bit internal sizing, so one dataset is limited to at most
  `math.MaxUint32` unique hashes. Larger logical datasets need sharding.
- `MapDelta` is immutable by convention and process-local. A production delta
  needs persistent synchronization, auditing, expiry, and campaign rules.

## Assumptions worth calling out

- Menu is seeded in `main.go`. The spec examples a waffle, that is the catalog.
- Valid coupon → 10% off the subtotal, rounded to cents.
- Invalid coupon → 200 + no discount, not 422.
- Product path param is an int64 at the HTTP boundary. product `id` is a string
  in JSON, matching the spec's mixed types.
