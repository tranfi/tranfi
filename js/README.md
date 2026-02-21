# tranfi (Node.js / WASM)

Streaming ETL in JavaScript, powered by a native C11 core via N-API (Node.js) or WASM (browsers). Process CSV, JSONL, and text data with composable pipelines that run in constant memory, no matter how large the input.

```js
import { pipeline, codec, ops, expr } from 'tranfi'

const result = await pipeline([
  codec.csv(),
  ops.filter(expr("col('age') > 25")),
  ops.sort(['-age']),
  ops.derive({ label: expr("if(col('age')>30, 'senior', 'junior')") }),
  ops.select(['name', 'age', 'label']),
  codec.csvEncode(),
]).run({ input: 'name,age\nAlice,30\nBob,25\nCharlie,35\nDiana,28\n' })

console.log(result.outputText)
// name,age,label
// Charlie,35,senior
// Alice,30,junior
// Diana,28,junior
```

Or use the pipe DSL for one-liners:

```js
const result = await pipeline('csv | filter "col(age) > 25" | sort -age | csv')
  .run({ inputFile: 'data.csv' })
```

## Install

```bash
npm install tranfi
```

Uses N-API natively in Node.js, falls back to WASM in browsers automatically.

## CLI

Installing the package also provides the `tranfi` command:

```bash
# Via npx (no install)
echo 'name,age\nAlice,30\nBob,25' | npx tranfi 'csv | filter "age > 25" | csv'

# Or install globally
npm i -g tranfi
tranfi 'csv | filter "age > 25" | sort -age | csv' < data.csv
tranfi profile < data.csv
tranfi -R  # list recipes
```

Run `tranfi -h` for all options.

## Quick start

### Two APIs

**Builder API** -- composable, type-safe, IDE-friendly:

```js
const p = pipeline([
  codec.csv(),
  ops.filter(expr("col('score') >= 80")),
  ops.derive({ grade: expr("if(col('score')>=90, 'A', 'B')") }),
  ops.sort(['-score']),
  ops.head(10),
  codec.csvEncode(),
])
const result = await p.run({ inputFile: 'students.csv' })
```

**DSL strings** -- compact, suitable for CLI-like use:

```js
const p = pipeline('csv | filter "col(score) >= 80" | sort -score | head 10 | csv')
const result = await p.run({ inputFile: 'students.csv' })
```

Both produce identical pipelines under the hood.

### Running pipelines

```js
// From string or Buffer
const result = await p.run({ input: 'name,age\nAlice,30\n' })

// From file (streamed in 64 KB chunks)
const result = await p.run({ inputFile: 'data.csv' })

// Access results
result.output         // Buffer
result.outputText     // string (UTF-8 decoded)
result.errors         // Buffer (error channel)
result.stats          // Buffer (pipeline stats)
result.statsText      // string
result.samples        // Buffer (sample channel)
```

## Codecs

Codecs convert between raw bytes and columnar batches. Every pipeline starts with a decoder and ends with an encoder.

| Method | Description |
|--------|-------------|
| `codec.csv({ delimiter, header, batchSize, repair })` | CSV decoder. `repair: true` pads short / truncates long rows |
| `codec.csvEncode({ delimiter })` | CSV encoder |
| `codec.jsonl({ batchSize })` | JSON Lines decoder |
| `codec.jsonlEncode()` | JSON Lines encoder |
| `codec.text({ batchSize })` | Line-oriented text decoder (single `_line` column) |
| `codec.textEncode()` | Text encoder |
| `codec.tableEncode({ maxWidth, maxRows })` | Pretty-print Markdown table |

Cross-codec pipelines work naturally:

```js
// CSV in, JSONL out
pipeline([codec.csv(), ops.head(5), codec.jsonlEncode()])

// JSONL in, CSV out
pipeline([codec.jsonl(), ops.sort(['name']), codec.csvEncode()])
```

## Operators

### Row filtering

| Method | Description |
|--------|-------------|
| `ops.filter(expr)` | Keep rows matching expression |
| `ops.head(n)` | First N rows |
| `ops.tail(n)` | Last N rows |
| `ops.skip(n)` | Skip first N rows |
| `ops.top(n, column, desc?)` | Top N by column value |
| `ops.sample(n)` | Reservoir sampling (uniform random) |
| `ops.grep(pattern, { invert, column, regex })` | Substring/regex filter |
| `ops.validate(expr)` | Add `_valid` boolean column, keep all rows |

### Column operations

| Method | Description |
|--------|-------------|
| `ops.select(columns)` | Keep and reorder columns |
| `ops.rename(mapping)` | Rename columns: `rename({ name: 'full_name' })` |
| `ops.derive(columns)` | Computed columns: `derive({ total: expr("col('a')*col('b')") })` |
| `ops.cast(mapping)` | Type conversion: `cast({ age: 'int', score: 'float' })` |
| `ops.trim(columns?)` | Strip whitespace |
| `ops.fillNull(mapping)` | Replace nulls: `fillNull({ age: '0' })` |
| `ops.fillDown(columns?)` | Forward-fill nulls |
| `ops.clip(column, { min, max })` | Clamp numeric values |
| `ops.replace(column, pattern, replacement, { regex })` | String find/replace |
| `ops.hash(columns?)` | Add `_hash` column (DJB2) |
| `ops.bin(column, boundaries)` | Discretize into bins |

### Sorting and deduplication

| Method | Description |
|--------|-------------|
| `ops.sort(columns)` | Sort rows. Prefix `-` for descending: `sort(['-age', 'name'])` |
| `ops.unique(columns?)` | Deduplicate on specified columns |

### Aggregation

| Method | Description |
|--------|-------------|
| `ops.stats(statsList?)` | Column statistics. Stats: `count`, `min`, `max`, `sum`, `avg`, `stddev`, `variance`, `median`, `p25`, `p75`, `p90`, `p99`, `distinct`, `hist`, `sample` |
| `ops.frequency(columns?)` | Value counts (descending) |
| `ops.groupAgg(groupBy, aggs)` | Group by + aggregate |

```js
// Group aggregation
ops.groupAgg(['city'], [
  { column: 'price', func: 'sum', result: 'total' },
  { column: 'price', func: 'avg', result: 'avg_price' },
])
```

### Sequential / window

| Method | Description |
|--------|-------------|
| `ops.step(column, func, result?)` | Running aggregation: `running-sum`, `running-avg`, `running-min`, `running-max`, `lag` |
| `ops.window(column, size, func, result?)` | Sliding window: `avg`, `sum`, `min`, `max` |
| `ops.lead(column, { offset, result })` | Lookahead N rows |

### Reshape

| Method | Description |
|--------|-------------|
| `ops.explode(column, delimiter?)` | Split delimited string into rows |
| `ops.split(column, names, delimiter?)` | Split column into multiple columns |
| `ops.unpivot(columns)` | Wide to long (melt) |
| `ops.stack(file, { tag, tagValue })` | Vertically concatenate another CSV file |

### Date/time

| Method | Description |
|--------|-------------|
| `ops.datetime(column, extract?)` | Extract parts: `year`, `month`, `day`, `hour`, `minute`, `second`, `weekday` |
| `ops.dateTrunc(column, trunc, { result })` | Truncate to: `year`, `month`, `day`, `hour`, `minute`, `second` |

### Other

| Method | Description |
|--------|-------------|
| `ops.flatten()` | Flatten nested columns |
| `ops.reorder(columns)` | Alias for `select` |
| `ops.dedup(columns?)` | Alias for `unique` |

## Expressions

Used in `filter`, `derive`, and `validate`. Reference columns with `col('name')`.

```js
ops.filter(expr("col('age') > 25 and contains(col('name'), 'A')"))
ops.derive({
  full:  expr("concat(col('first'), ' ', col('last'))"),
  grade: expr("if(col('score')>=90, 'A', if(col('score')>=80, 'B', 'C'))"),
})
```

### Available functions

| Category | Functions |
|----------|-----------|
| Arithmetic | `+` `-` `*` `/` |
| Comparison | `>` `>=` `<` `<=` `==` `!=` |
| Logic | `and` `or` `not` |
| String | `upper(s)` `lower(s)` `initcap(s)` `len(s)` `trim(s)` `left(s,n)` `right(s,n)` `concat(a,b,...)` `replace(s,old,new)` `slice(s,start,len)` `pad_left(s,w)` `pad_right(s,w)` |
| Predicates | `starts_with(s,prefix)` `ends_with(s,suffix)` `contains(s,sub)` |
| Conditional | `if(cond,then,else)` `coalesce(a,b,...)` `nullif(a,b)` |
| Math | `abs(x)` `round(x)` `floor(x)` `ceil(x)` `sign(x)` `pow(x,y)` `sqrt(x)` `log(x)` `exp(x)` `mod(a,b)` `greatest(a,b,...)` `least(a,b,...)` |

Aliases: `substr`=`slice`, `length`=`len`, `lpad`=`pad_left`, `rpad`=`pad_right`, `min`=`least`, `max`=`greatest`.

## Recipes

Built-in named pipelines for common tasks. Use by name:

```js
const result = await pipeline('preview').run({ inputFile: 'data.csv' })
const result = await pipeline('freq').run({ inputFile: 'data.csv' })
```

| Recipe | Pipeline | Description |
|--------|----------|-------------|
| `profile` | `csv \| stats \| csv` | Full data profiling |
| `preview` | `csv \| head 10 \| csv` | First 10 rows |
| `schema` | `csv \| head 0 \| csv` | Column names only |
| `summary` | `csv \| stats count,min,max,avg,stddev \| csv` | Summary statistics |
| `count` | `csv \| stats count \| csv` | Row count |
| `cardinality` | `csv \| stats count,distinct \| csv` | Unique value counts |
| `distro` | `csv \| stats min,p25,median,p75,max \| csv` | Five-number summary |
| `freq` | `csv \| frequency \| csv` | Value frequency |
| `dedup` | `csv \| dedup \| csv` | Remove duplicates |
| `clean` | `csv \| trim \| csv` | Trim whitespace |
| `sample` | `csv \| sample 100 \| csv` | Random 100 rows |
| `head` | `csv \| head 20 \| csv` | First 20 rows |
| `tail` | `csv \| tail 20 \| csv` | Last 20 rows |
| `csv2json` | `csv \| jsonl` | CSV to JSONL |
| `json2csv` | `jsonl \| csv` | JSONL to CSV |
| `tsv2csv` | `csv delimiter="\t" \| csv` | TSV to CSV |
| `csv2tsv` | `csv \| csv delimiter="\t"` | CSV to TSV |
| `look` | `csv \| table` | Pretty-print table |
| `histogram` | `csv \| stats hist \| csv` | Distribution histograms |
| `hash` | `csv \| hash \| csv` | Row hash for change detection |
| `samples` | `csv \| stats sample \| csv` | Sample values per column |

List all recipes programmatically:

```js
import { recipes } from 'tranfi'

for (const r of await recipes()) {
  console.log(`${r.name.padEnd(15)} ${r.description}`)
}
```

## DuckDB engine

Run pipelines on DuckDB instead of the native C streaming core. The DSL is transpiled to SQL in C, then executed by DuckDB.

```bash
npm install duckdb
```

```js
import { pipeline, compileToSql } from 'tranfi'

// Run a pipeline via DuckDB
const result = await pipeline('csv | filter "age > 25" | sort -age | csv', { engine: 'duckdb' })
  .run({ inputFile: 'data.csv' })

// Or with string/Buffer input
const result2 = await pipeline('csv | head 10 | csv', { engine: 'duckdb' })
  .run({ input: csvString })
```

### SQL transpilation

Generate SQL directly from DSL strings:

```js
const sql = await compileToSql('csv | filter "col(age) > 25" | sort -age | head 10 | csv')
console.log(sql)
// WITH
//   step_1 AS (SELECT * FROM input_data WHERE ("age" > 25)),
//   step_2 AS (SELECT * FROM step_1 ORDER BY "age" DESC LIMIT 10)
// SELECT * FROM step_2
```

### Browser (WASM + DuckDB-WASM)

In the browser, use `@duckdb/duckdb-wasm` with the tranfi WASM module:

```js
import createTranfi from 'tranfi/wasm'
import * as duckdb from '@duckdb/duckdb-wasm'

const tf = await createTranfi()

// SQL generation (synchronous, no DuckDB needed)
const sql = tf.compileToSql('csv | filter "age > 25" | csv')

// Full execution with DuckDB-WASM
const db = new duckdb.AsyncDuckDB(...)
await db.instantiate(...)

const result = await tf.runDuckDB(db, 'csv | filter "age > 25" | csv', csvData)
console.log(result.outputText)  // CSV output
console.log(result.rows)        // Array of row objects
```

`runDuckDB` accepts string, `Uint8Array`, or `File` objects as data input.

## Advanced

### DSL compilation

```js
import { compileDsl, saveRecipe, loadRecipe } from 'tranfi'

// Compile DSL to JSON plan
const json = await compileDsl('csv | filter "col(age) > 25" | sort -age | csv')

// Save / load recipes
await saveRecipe([codec.csv(), ops.head(10), codec.csvEncode()], 'preview.tranfi')
const p = await loadRecipe('preview.tranfi')
const result = await p.run({ inputFile: 'data.csv' })
```

### Side channels

Every pipeline produces four output channels:

- **output** -- main pipeline result
- **errors** -- rows that failed processing
- **stats** -- pipeline execution statistics (rows in/out, timing)
- **samples** -- reserved for sampling operators

```js
const result = await p.run({ inputFile: 'data.csv' })
console.log(result.statsText)   // {"rows_in": 1000, "rows_out": 42, ...}
```

### Pipeline from JSON

```js
const p = await loadRecipe({
  steps: [
    { op: 'codec.csv.decode', args: {} },
    { op: 'head', args: { n: 5 } },
    { op: 'codec.csv.encode', args: {} },
  ]
})
```

### Backend selection

The package automatically selects the best backend:

1. **N-API** (Node.js) -- native C addon, fastest, used when available
2. **WASM** (browsers/fallback) -- same C core compiled to WebAssembly, ~313 KB single-file
3. **DuckDB** (opt-in) -- SQL execution via `{ engine: 'duckdb' }`, requires `npm install duckdb`

```js
// Force WASM backend (e.g., for testing)
import createTranfi from 'tranfi/wasm'
const tf = await createTranfi()
```

## Architecture

The Node.js package wraps the same C11 core used by the CLI, Python, and WASM targets. Data flows through columnar batches with typed columns (`bool`, `int64`, `float64`, `string`, `date`, `timestamp`) and per-cell null bitmaps. All operators are streaming with bounded memory, except those that require full input (sort, unique, stats, tail, top, group-agg, frequency, pivot).
