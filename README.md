# tranfi

Streaming ETL language in C11. Pipe DSL, push/pull API, columnar batches, 40 built-in operators. Bindings for Python, Node.js, R, and WASM.

```bash
printf "name,age\nAlice,30\nBob,25\nCharlie,35\n" \
  | tranfi 'csv | filter "col(age) > 25" | sort -age | csv'
```

## Pipe DSL

Pipelines are `source | transform... | sink`. Codecs (`csv`, `jsonl`) auto-resolve by position.

### Codecs

| Codec | Options |
|-------|---------|
| `csv` | `delimiter=;` `header=false` |
| `jsonl` | |
| `text` | `batch_size=1024` |

Cross-codec: `csv | ... | jsonl`. The `text` codec splits on newlines into a single `_line` column — no field parsing, no type detection.

### Transforms

| Transform | Syntax | Description |
|-----------|--------|-------------|
| `filter` | `filter "col(age) > 25"` | Keep rows matching expression |
| `select` | `select name,age` | Keep/reorder columns |
| `rename` | `rename name=full_name` | Rename columns |
| `head` | `head 10` | First N rows |
| `tail` | `tail 10` | Last N rows |
| `skip` | `skip 5` | Skip first N rows |
| `derive` | `derive total=col(price)*col(qty)` | Computed columns |
| `sort` | `sort age` / `sort -age` | Sort (prefix `-` for desc) |
| `unique` | `unique name,city` | Deduplicate |
| `stats` | `stats` / `stats count,sum` | Column statistics |
| `validate` | `validate "col(age) > 0"` | Add `_valid` column, keep all rows |
| `trim` | `trim name,city` | Strip whitespace |
| `fill-null` | `fill-null age=0 city=unknown` | Replace nulls |
| `fill-down` | `fill-down city` | Forward-fill nulls |
| `cast` | `cast age=int score=float` | Type conversion |
| `clip` | `clip score 0 100` | Clamp to [min, max] |
| `replace` | `replace name Alice Alicia` | String find/replace (or `--regex`) |
| `hash` | `hash name,city` | DJB2 hash (`_hash` column) |
| `bin` | `bin age 18,30,50` | Discretize into bins |
| `step` | `step price running-sum cumsum` | Running agg (cumsum, delta, lag, ratio) |
| `window` | `window price 3 avg price_ma3` | Sliding window (avg, sum, min, max) |
| `explode` | `explode tags ,` | Split string into rows |
| `split` | `split name " " first,last` | Split column into columns |
| `unpivot` | `unpivot jan,feb,mar` | Wide to long |
| `top` | `top 10 score` | Top N by column |
| `sample` | `sample 100` | Reservoir sampling |
| `group-agg` | `group-agg city sum:price:total` | Group by + aggregate |
| `frequency` | `frequency city` | Value counts |
| `grep` | `grep error` / `grep -r "^err"` | Substring or regex filter (`-v` invert, `-r` regex) |
| `datetime` | `datetime date year,month,day` | Extract date parts |
| `flatten` | `flatten` | Flatten nested columns |

Aliases: `reorder` (select), `dedup` (unique).

### Expressions

Used in `filter`, `derive`, `validate`:
- Columns: `col(name)` or `col('name')`
- Arithmetic: `+` `-` `*` `/`
- Comparisons: `>` `>=` `<` `<=` `==` `!=`
- Logic: `and` `or` `not`
- Literals, parentheses
- String: `upper()` `lower()` `len()` `trim()` `concat()` `slice()` `pad_left()` `pad_right()`
- Predicates: `starts_with()` `ends_with()` `contains()`
- Conditional: `if(cond, then, else)` `coalesce(a, b, ...)`
- Math: `abs()` `round()` `floor()` `ceil()` `min()` `max()`

### Examples

```bash
# Computed columns
tranfi 'csv | derive total=col(price)*col(qty) | csv'

# Combined pipeline
tranfi 'csv | filter "col(age) > 25" | sort -score | head 10 | csv'

# Running sum + moving average
tranfi 'csv | step price running-sum cumsum | window price 3 avg ma3 | csv'

# Group by + aggregate
tranfi 'csv | group-agg city sum:price:total avg:price:avg_price | csv'

# Value counts
tranfi 'csv | frequency city | csv'

# Extract date parts
tranfi 'csv | datetime date year,month,day | csv'

# String functions in derive
tranfi 'csv | derive upper_name=upper(col(name)) initials=slice(col(name),0,1) | csv'

# Conditional expressions
tranfi 'csv | derive label=if(col(age)>25,"senior","junior") | csv'

# Regex grep and replace
tranfi 'text | grep -r "^error:.*timeout" | text' < server.log
tranfi 'csv | replace --regex name "A.*e" X | csv'

# Cross-codec
tranfi 'csv | filter "col(age) > 25" | jsonl'

# Text mode — line-oriented, no CSV parsing
tranfi 'text | grep error | text' < server.log
tranfi 'text | grep -v debug | head 100 | text' < app.log
```

## Benchmarks

1M rows, single-threaded, best of 3. Times in ms (lower is better).

### Text pipelines (1M lines, 35.7 MB)

| Task | CLI | | Python | | Node.js |
|------|----:|---------|-------:|--------|--------:|
| | **tranfi** | **coreutils** | **tranfi** | **pure python** | **tranfi** |
| passthrough | 149 | 43 | 164 | 117 | 204 |
| head 1000 | 110 | 4 | 88 | 79 | 98 |
| tail 1000 | 148 | 3 | — | — | — |
| grep | 126 | 2 | 124 | 118 | 135 |
| grep -v | 148 | 3 | 154 | — | 173 |
| count lines | 92 | 8 | — | — | — |
| sort | 865 | 487 | — | — | — |
| unique | 158 | 489 | — | — | — |

Coreutils read directly from files (seek, mmap); tranfi streams through stdin. The `text` codec skips CSV parsing entirely — just `memchr` for newlines.

### CSV pipelines (1M rows, 17.9 MB)

| Task | CLI | Python | | | | Node.js |
|------|----:|-------:|-------:|-------:|-------:|--------:|
| | **tranfi** | **tranfi** | **pandas** | **polars** | **duckdb** | **tranfi** |
| passthrough | 351 | 415 | 647 | 60 | 115 | 214 |
| filter 50% | 358 | 358 | 414 | 35 | 107 | 191 |
| select 2 cols | 283 | 309 | 420 | 34 | 91 | 150 |
| head 1000 | 143 | 134 | 147 | 17 | 19 | 64 |
| sort | 1108 | 1233 | 698 | 67 | 150 | 575 |
| unique | 197 | 186 | 179 | 26 | 87 | 95 |
| stats | 573 | 623 | 162 | 29 | 81 | 254 |
| group-agg | 194 | 202 | 195 | 31 | 114 | 132 |
| frequency | 238 | 177 | 203 | 31 | 114 | 98 |

Polars and DuckDB use multi-threaded parallel execution + SIMD. Tranfi is single-threaded streaming with bounded memory. The JS (Browser) column uses the same C core compiled to WASM.

Run benchmarks: `./build/bench 1000000`

## Architecture

```
L3 (DSL / JSON / Python / JS)  →  L2 (IR: validated, schema-inferred)  →  L1 (native C pipeline)
```

- **L3**: Pipe DSL, JSON plans, Python/JS/R builder APIs
- **L2**: Op registry, validation, schema inference
- **L1**: Streaming C11 runtime with columnar batches, side channels

Data model: columnar batches with typed columns (`bool`, `int64`, `float64`, `string`) and per-cell null bitmaps.

Every pipeline produces `main` output plus `errors`, `stats`, and `samples` side channels.

## Build

```bash
make            # build + test
make wasm       # WASM (single-file, embedded, ~413 KB)
```

Or manually:

```bash
cd build && cmake .. -DBUILD_TESTING=ON && make && ./test_core
```

## Bindings

### Python

```python
import tranfi as tf

result = tf.pipeline([
    tf.codec.csv(),
    tf.ops.filter(tf.expr("col('age') > 25")),
    tf.ops.frequency(['city']),
    tf.codec.csv_encode(),
]).run(input_file='data.csv')

print(result.output_text)
```

See [py/](py/) for the full API.

### Node.js / WASM

```js
import { pipeline, codec, ops, expr } from 'tranfi'

const result = await pipeline([
  codec.csv(),
  ops.filter(expr("col('age') > 25")),
  ops.frequency(['city']),
  codec.csvEncode(),
]).run({ inputFile: 'data.csv' })

console.log(result.outputText)
```

See [js/](js/) for the full API. Uses N-API natively, falls back to WASM in browsers.

### R

See [r/](r/).

## CLI

```bash
tranfi 'csv | filter "col(age) > 25" | csv' < input.csv > output.csv
tranfi -i input.csv -o output.csv 'csv | sort -age | csv'
tranfi -p 'csv | stats | csv' < big.csv       # progress on stderr
tranfi -j 'csv | head 10 | csv'               # output IR as JSON
```

## Testing

```bash
make test                # all tests (C + Python + Node.js)
```

Or individually:

```bash
./build/test_core        # 104 C core tests
python -m pytest test/   # 28 Python binding tests
node test/test_node.js   # 28 Node.js binding tests
```

## Project structure

```
src/                 C11 core (40 operators, 3 codecs, DSL parser, IR, compiler)
js/                  Node.js N-API + WASM bindings
py/                  Python ctypes bindings
r/                   R bindings
browser/             Browser playground
bench/               Benchmarks
test/                Tests (C, Python, Node.js)
Makefile             Build orchestration
```

## License

MIT
