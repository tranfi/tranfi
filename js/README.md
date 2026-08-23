# tranfi (Node.js / WASM)

Streaming-first ETL in JavaScript, powered by a native C11 core via N-API
(Node.js) or WASM (browsers). Tranfi processes CSV, JSONL, and text byte streams;
it is not an in-memory DataFrame API. Row-local operations stream, bounded
operators declare their limits, and full-input operators require an explicit
blocking or spill policy.

> **Unreleased main:** The prepared-transform API documented below targets
> Tranfi 0.2. Current npm 0.1.x installs do not include it; build this branch
> from source until 0.2 is published.

Save this example as `quickstart.mjs`:

```js
import tranfi from 'tranfi'

const { pipeline, codec, ops, expr } = tranfi

const result = await pipeline([
  codec.csv(),
  ops.filter(expr("col('age') > 25")),
  ops.top(100, 'age'),
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
const result = await pipeline('csv | filter "col(age) > 25" | top-k 100 age | csv')
  .run({ inputFile: 'data.csv' })
```

## Install

```bash
npm install tranfi
```

The default install compiles the N-API addon and reports a nonzero failure if
the native toolchain or synchronized C sources are unavailable. For an
intentional WASM-only/browser installation, set
`TRANFI_SKIP_NATIVE_BUILD=1` and import `tranfi/wasm` explicitly. The ordinary
pipeline can execute with WASM, but root-entry prepared transforms require the
native addon.

The native addon is currently built and tested on Linux. Windows is supported
through the packed package's `tranfi/wasm` entry with
`TRANFI_SKIP_NATIVE_BUILD=1`; the release CI runs streaming and prepared-transform
smokes in that configuration. This is not a Windows native-addon support claim.

Use `pipeline(...)` for byte-stream ETL. The separate
`TransformRecipe -> TransformAnalyzer -> TransformPlan -> TransformApply`
lifecycle is for typed batches whose learned state must be frozen and reused.
Calling `.run()` collects output for convenience; use `writeTo()` or
`toReadable()` for large outputs.

## CLI

Installing the package also provides the `tranfi` command:

```bash
# Via npx (no install)
echo 'name,age\nAlice,30\nBob,25' | npx tranfi -q 'csv | filter "age > 25" | csv'

# Or install globally
npm i -g tranfi
tranfi -q 'csv | filter "age > 25" | top-k 100 age | csv' < data.csv
tranfi profile < data.csv
tranfi -R  # list recipes
```

Run `tranfi -h` for all options.

## Quick start

### Two APIs

**Builder API** -- composable, structured, and IDE-friendly:

```js
const p = pipeline([
  codec.csv(),
  ops.filter(expr("col('score') >= 80")),
  ops.derive({ grade: expr("if(col('score')>=90, 'A', 'B')") }),
  ops.top(10, 'score'),
  codec.csvEncode(),
])
const result = await p.run({ inputFile: 'students.csv' })
```

**DSL strings** -- compact, suitable for CLI-like use:

```js
const p = pipeline('csv | filter "col(score) >= 80" | top-k 10 score | csv')
const result = await p.run({ inputFile: 'students.csv' })
```

Both produce identical pipelines under the hood.

Dataframe-style DSL aliases are accepted and normalize to canonical ops: `mutate` -> `derive`, `summarise`/`summarize` -> `group-agg`, `distinct` -> `unique`, and `arrange` -> `sort`.

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

For large outputs, drain chunks instead of collecting `result.output`:

```js
const { createReadStream, createWriteStream } = require("fs")

// Write to a Node Writable and wait for `drain` when the sink applies backpressure.
const out = createWriteStream("out.csv")
const result = await p.writeTo(out, { inputFile: "data.csv" })

// Or consume Tranfi output as a Node Readable.
for await (const chunk of p.toReadable({ inputFile: "data.csv" })) {
  // process each output chunk
}

// Existing input streams can feed the native pipeline without readFile() materialization.
const result2 = await p.run({ inputStream: createReadStream("data.csv") })

// .gz input files are decompressed as streaming sources by default.
const gz = await p.run({ inputFile: "events.jsonl.gz" })
const raw = await p.run({ inputFile: "events.jsonl.gz", compression: "none" })
const streamGz = await p.run({ inputStream: createReadStream("events.jsonl.gz"), compression: "gzip" })

// Multiple files stream sequentially through one pipeline.
const inputFiles = ["part-a.csv", "part-b.csv"]
const combined = await p.run({ inputFiles, sourceColumn: "src" })

// Low-level async iteration is still available.
for await (const chunk of p.iterChunks({ inputFiles, sourceColumn: "src" })) {
  // process each output chunk
}
```

`sourceColumn` appends the path for each input row without preloading file contents. Tranfi flushes decoder input at each file boundary so an unterminated final record belongs to the correct source file. It does not remove repeated CSV headers from later files; use shards without repeated headers, `header: false`, or a pre-cleaning step when every file has its own header.

Standalone WASM exposes the same non-collecting shape for in-memory data: `tf.run(dsl, data, { onOutput, collectOutput: false })` and `tf.iterChunks(dsl, data)`.

For browser UI work, keep Worker placement outside the core pipeline and use the optional WASM Worker adapter:

```js
// main thread
import { createWorkerClient } from 'tranfi/wasm/worker'

const client = createWorkerClient(new Worker(new URL('./tranfi-worker.js', import.meta.url), { type: 'module' }))
const result = await client.runFile('csv | filter "col(age) >= 18" | csv', file, {
  chunkSize: 64 * 1024,
  collectOutput: false,
  onOutput: chunk => downloadSink.write(chunk),
  onProgress: p => updateProgress(p),
  signal: abortController.signal
})

// tranfi-worker.js, bundled by the app
import { runWorkerServer } from 'tranfi/wasm/worker'
runWorkerServer()
```

The Worker protocol streams chunks with transferable buffers and sends progress, stats, errors, and cancellation messages around the same WASM `create/push/pull/finish/free` API; it is not a separate IR target.

The bundled Tranfi app runner is also preview-bounded by default: file chunks are streamed into WASM, main output is drained incrementally into a table preview capped by hidden `preview_rows` (default 200), and full output text is only materialized when `collect_output` is explicitly true in the schema.

### Prepared reusable transforms

Prepared transforms are a separate typed-table API for operations whose parameters must be learned from reference data. `analyze` accumulates bounded statistics over one or more batches, `finalize` freezes an immutable plan and output schema, and `apply` runs that plan either over a second pass of the original data (`fit_transform`-style) or over later compatible batches. It does not replace the byte-stream pipeline API.

The current slice accepts declared `float32`/`float64` columns. It supports numeric none/zero/constant/mean/exact-median imputation and none/standard/min-max normalization. Declared categorical columns support mode imputation with `allMissing: 'error' | 'zero'` or `impute.op: 'none'`; encoders discover finite typed categories, while the no-imputation/no-encoding combination needs no learned dictionary. A column may instead provide both branches with `kind.op: 'infer'`, `kind.rule: 'finite-integer-cardinality-v1'`, and `kind.maxCategories >= 2`: missing/NaN values are ignored, `2..maxCategories` distinct finite integers resolve categorical, while zero/one distinct value, any noninteger, or the next distinct value resolves numeric. `impute.op: 'none'` with `encode.op: 'none'` passes every finite value through and emits canonical qNaN for missing input. Mode with `encode.op: 'none'` retains its learned dictionary and rejects unseen finite values with code `108`. `encode.op: 'label'` freezes zero-based sorted ordinals and supports `unknown: 'error' | 'sentinel' | 'other'`; `encode.op: 'onehot'` emits source-ordered category blocks and supports `unknown: 'error' | 'all_zero' | 'other'`. Without categorical imputation, missing label/one-hot input follows that encoder's unknown policy. The label sentinel must be a safe integer outside the learned ordinal range; label/one-hot `other` appends the reserved ordinal/field after known categories. Generated output IDs/names and one-hot category metadata are deterministic, and collisions fail with code `102`. Exact median obeys the configured allocation and resident-state limits; inference, categorical discovery, and output expansion obey category, output-width, allocation, and resident-state limits. Limit failures use resource code `104`. Prepared-transform host-policy and spill fields are reserved but not implemented; nonempty use fails with unsupported-runtime code `113` instead of being silently ignored. Fixed dictionaries and categorical constant imputation are not implemented yet.

Prepared runtime option objects reject unknown fields. Native Node accepts
`limits`, `cancelFlag`, `hostPolicy`, and `spillDir`; standalone WASM accepts
`limits`, `cancelToken`, `hostPolicy`, and `spillDir`. The host/spill fields are
reserved as described above, and cancellation spellings are not interchangeable.

```js
const tf = require('tranfi')

const recipeSpec = {
  format: 'tranfi.transform-recipe',
  version: 1,
  policyVersion: 1,
  outputDtype: 'float64',
  semanticLimits: {
    maxOutputColumns: 65536,
    maxOutputElementsPerApply: 134217728
  },
  columns: [{
    sourceId: 'x0',
    kind: { op: 'declared', value: 'numeric', rule: null, maxCategories: null },
    numeric: {
      impute: { op: 'mean', constant: null, allMissing: 'zero' },
      normalize: { op: 'standard', ddof: 0 }
    },
    categorical: null
  }]
}
const schema = [{ id: 'x0', dtype: 'float64' }]

const recipe = tf.TransformRecipe.fromJSON(recipeSpec)
const analyzer = recipe.analyzer(schema)
analyzer.push({ rows: 3, columns: [new Float64Array([1, NaN, 3])] })
const plan = analyzer.finalize()

const apply = plan.apply(schema)
const fittedReference = apply.run({
  rows: 3,
  columns: [new Float64Array([1, NaN, 3])]
})
const planBytes = plan.toBytes() // canonical TFTR artifact
const recipeSha256 = plan.recipeSha256() // canonical recipe + input-schema identity

apply.close()
plan.close()
analyzer.close()
recipe.close()
```

`tranfi/wasm` exposes the same classes on the initialized module. The Worker adapter adds `analyzeTransform()` and `applyTransform()`. With `SharedArrayBuffer`, an `AbortSignal` interrupts a synchronous C call through an atomic poll cell. Without it, cancellation terminates the whole worker and reclaims its WASM heap. Pass a worker URL directly so the client can recreate it, or supply an owned worker plus `workerFactory`:

```js
const workerUrl = new URL('./tranfi-worker.js', import.meta.url)
const makeWorker = () => new Worker(workerUrl, { type: 'module' })
const client = createWorkerClient(makeWorker(), {
  workerFactory: makeWorker,
  terminateOnDispose: true
})
```

All prepared-transform failures use `TranfiTransformError`; its numeric `code` is stable across native Node and WASM. Native Node accepts a SharedArrayBuffer-backed `Int32Array` as `cancelFlag`: cell 0 is the cancellation request, and an optional cell 1 is incremented modulo 2^32 at every native poll so another realm can observe operation progress without a timer.

## Codecs

Codecs convert between raw bytes and columnar batches. Every pipeline starts with a decoder and ends with an encoder.

| Method | Description |
|--------|-------------|
| `codec.csv({ delimiter, header, batchSize, repair, mode, strict, maxErrorBytes, maxRecordBytes, maxColumns, nulls, quotedNulls, skip, nMax, maxRows, comment, trimWs, skipEmptyRows, audit, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes })` | CSV decoder. `mode: 'strict'` fails on field-count mismatches; `repair: true` / `mode: 'repair'` emits repair diagnostics; `maxRecordBytes` bounds buffered records; `nulls: ['NA']` adds null sentinels; `skip: 2`, `nMax: 100`, `comment: '#'`, `trimWs: false`, and `skipEmptyRows: true` control row-local parsing; repair audit/raw diagnostics support privacy controls with pseudo-column `raw` |
| `codec.csvEncode({ delimiter })` | CSV encoder |
| `codec.jsonl({ batchSize, onError, maxErrorBytes, maxRecordBytes })` | JSON Lines decoder. `onError` is `skip`, `fail`, `warn`, or `quarantine` |
| `codec.jsonlEncode()` | JSON Lines encoder |
| `codec.text({ batchSize, maxErrorBytes, maxRecordBytes })` | Line-oriented text decoder (single `_line` column) |
| `codec.textEncode()` | Text encoder |
| `codec.tableEncode({ maxWidth, maxRows })` | Pretty-print Markdown table |

CSV treats unquoted empty fields as null by default. Pass `nulls: ['NA', 'NULL']` to add sentinel strings; pass `quotedNulls: false` when quoted sentinels like `"NA"` or `""` should remain strings. Default field-count handling is permissive for compatibility. Use `mode: 'strict'` or `strict: true` to fail on row/header width mismatches. Use `repair: true` or `mode: 'repair'` to pad/truncate and collect JSONL diagnostics in `result.errors`; `maxErrorBytes` bounds raw previews, and `auditIncludeRow`, `auditColumns`, `auditRedact`, `auditHashColumns`, `auditMaxBytes`, and `auditMaxCellBytes` govern repair audit/raw payloads with the raw preview exposed as pseudo-column `raw`. `maxRecordBytes` defaults to `67108864` bytes, caps the current record buffer before a newline is seen, and rejects with a `csv_record_too_large` diagnostic when exceeded; pass `0` to disable the guard. `maxColumns` defaults to `8192`; records above the cap reject with a bounded `csv_too_many_columns` diagnostic instead of silently dropping columns. Decoder size options are checked before execution: `batchSize` must be `1..65536`, `maxErrorBytes` must be `0..67108864`, `maxRecordBytes` must be `0..1073741824`, and `maxColumns` must be `1..65536`. `skip: 2` discards preamble records before header/schema discovery; comments are applied after skipped rows. `nMax: 100` / `maxRows: 100` keeps at most that many decoded data rows after skip/comment/header handling and uses only one counter; `nMax: 0` preserves a header-only schema batch. `comment: '#'` removes text after an unquoted marker and skips comment-only rows; quoted markers are preserved. Unquoted spaces/tabs are trimmed by default; pass `trimWs: false` to preserve them. Blank physical rows after the header are preserved as all-null rows by default; pass `skipEmptyRows: true` to drop them.

Malformed JSONL records are skipped by default. Set `onError: 'warn'` or `onError: 'quarantine'` to keep valid rows and collect JSONL diagnostics in `result.errors`; set `onError: 'fail'` to reject on the first malformed line. `maxErrorBytes` bounds the raw preview stored in diagnostics, and `maxRecordBytes` applies the same current-line guard as CSV/text.

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
| `ops.filter(expr, { audit, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes }?)` | Keep rows matching expression; optional dropped-row audit records support row omission, column allowlists, redaction, hashes, and payload caps |
| `ops.head(n)` | First N rows |
| `ops.tail(n)` | Last N rows |
| `ops.skip(n)` | Skip first N rows |
| `ops.top(n, column, desc?)` | Top N by column value |
| `ops.sample(n, { seed })` | Deterministic bounded reservoir sampling; use `seed: 'random'` for nondeterministic mode |
| `ops.grep(pattern, { invert, column, regex })` | Substring/regex filter |
| `ops.validate(expr, { rules, rulesFile, audit, auditLimit, maxFailures, warnFailureRate, maxFailureRate, name, message, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes }?)` | Add `_valid` boolean column, keep all rows; supports inline rules or local JSON rule-suite files; bounded failure audit records support count/rate thresholds plus privacy controls |
| `ops.assert(expr, { action, name, message, result, aggregate, op, value, column, tolerance, rel, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes }?)` | Row-local data-quality rule or finish-time O(1) aggregate assertion; row-local failure side-channel records support privacy controls |
| `ops.quarantine(expr, { name, message, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes }?)` | Route rows matching expression to `errors` and drop them from main output; row payloads support privacy controls |
| `ops.schema({ columns, required, nonNull, nullable, values, min, max, regex, mode='fail', result='_schema', maxRegexPatternBytes, maxRegexCellBytes, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes })` | Row-local table schema contract; fail, warn, filter, quarantine, or annotate; regex budgets default to 4096-byte patterns and 65536-byte cells; schema audit/error records support row omission, column allowlists, redaction, stable non-cryptographic hashes, and row/cell payload caps |
| `ops.schemaInfer({ rows })` | Bounded decoded-type/nullability schema report; defaults to 10000 sampled rows |
| `ops.tee({ expr, channel, columns, limit, every, name, includeRow, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes })` | Preserve main rows and write bounded JSONL row snapshots to a side channel; row payloads support privacy controls |

### Column operations

| Method | Description |
|--------|-------------|
| `ops.select(columns)` | Keep and reorder columns |
| `ops.relocate(columns, { before, after })` | Move columns while preserving all columns |
| `ops.rename(mapping)` | Rename columns: `rename({ name: 'full_name' })` |
| `ops.derive(columns)` | Computed columns: `derive({ total: expr("col('a')*col('b')") })` |
| `ops.sourceName({ result, defaultValue })` | Append the current host source path/name as a row-local string column |
| `ops.across(columns, { fn, functions, names, replace })` | Apply row-local functions over selected columns |
| `ops.cast(mapping, { audit, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes })` | Type conversion; optional bounded value/coercion audit records support privacy controls |
| `ops.trim(columns?)` | Strip whitespace |
| `ops.fillNull(mapping, { audit, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes })` | Replace nulls; optional bounded audit records support privacy controls |
| `ops.fillDown(columns?)` | Forward-fill nulls |
| `ops.clip(column, { min, max })` | Clamp numeric values |
| `ops.replace(column, pattern, replacement, { regex, audit, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes })` | String find/replace; optional bounded audit records support privacy controls |
| `ops.hash(columns?)` | Add `_hash` column (DJB2) |
| `ops.bin(column, boundaries, { missing, onTypeError })` | Discretize into bins with strict numeric-source defaults |
| `ops.ewma(column, alpha, { result, missing, onTypeError })` | Exponentially weighted moving average |
| `ops.anomaly(column, { threshold, result, missing, onTypeError })` | Streaming z-score anomaly flag |
| `ops.normalize(columns, { method, audit, auditLimit, missing, onTypeError, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes })` | Blocking minmax/zscore normalization; optional audit records support privacy controls |
| `ops.acf(column, { lags, missing, onTypeError })` | Blocking autocorrelation table |

`columns` may contain exact names or selector strings: `id:score`, `starts_with(score_)`, `ends_with(_id)`, `contains(temp)`, `matches(^score_)`, `where(numeric)`, strict `all_of(score,name)`, lenient `any_of(optional,score)`, exclusions with `!name` or `-name`, and boolean selector algebra such as `starts_with(score_)&where(numeric)`, `starts_with(score_)&!ends_with(raw)`, or `!(id:score)`. Ranges use input schema order and can be reversed. Helper matching is case-insensitive; exact names are case-sensitive. These selectors are resolved by the native schema-aware `select`, `relocate`, and `across` ops in `O(columns)` without retaining rows; SQL lowering rejects selector helpers without a known schema.

Example: `ops.across(['starts_with(score_)'], { fn: 'round' })` replaces selected numeric columns per row; `ops.across(['name'], { functions: ['lower'], replace: false, names: '{col}_{fn}' })` appends templated columns. This is native/WASM row-local execution, not arbitrary lambdas or grouped dplyr evaluation.

### Sorting and deduplication

| Method | Description |
|--------|-------------|
| `ops.sort(columns)` | Sort rows. Prefix `-` for descending: `sort(['-age', 'name'])` |
| `ops.unique(columns?)` | Deduplicate on specified columns |

### Aggregation

| Method | Description |
|--------|-------------|
| `ops.stats(statsList?)` | Column statistics. Stats: `count`, `min`, `max`, `sum`, `avg`, `stddev`, `variance`, `median`, `p25`, `p75`, `p90`, `p99`, `distinct`, `hist`, `sample` |
| `ops.frequency(columns?, { maxValues, maxStateBytes, overflow, other, audit, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes }?)` | Value counts; `overflow: "other"` can emit bounded category-overflow audit records with privacy controls |
| `ops.groupAgg(groupBy, aggs)` | Group by + aggregate. `count` on a column counts non-null values; `column: '*'` counts rows. |

```js
// Group aggregation
ops.groupAgg(['city'], [
  { column: 'price', func: 'sum', name: 'total' },
  { column: 'price', func: 'avg', name: 'avg_price' },
  { column: '*', func: 'count', name: 'rows' },
])
```

### Sequential / window

| Method | Description |
|--------|-------------|
| `ops.step(column, func, result?)` | Running aggregation: `running-sum`, `running-avg`, `running-min`, `running-max`, `lag` |
| `ops.window(column, size, func, resultOrOptions?)` | Sliding window: `avg`, `sum`, `min`, `max`; options include `result`, `missing`, `onTypeError` |
| `ops.rollingSum/rollingMean/rollingMin/rollingMax(column, size, { result, missing, onTypeError })` | Named trailing fixed-row numeric windows; default missing/non-numeric source fails |
| `ops.rollingAny/rollingAll(column, size, { result, nulls })` | Boolean trailing windows; `nulls`: `ignore`, `false`, `true`, `propagate` |
| `ops.lead(column, { offset, result })` | Lookahead N rows |
| `ops.lag(column, { offset, result })` | Previous-row shift |
| `ops.shift(column, { offset, result, type })` | Shift alias; `type='lag'` default or `type='lead'` |
| `ops.rowid(columns, { result, sorted, maxKeys })` | Global or per-key 1-based row ids |
| `ops.rleid(columns, { result })` | Consecutive run id by selected column(s) |

### Reshape

| Method | Description |
|--------|-------------|
| `ops.explode(column, delimiter?, { maxTokensPerRow, maxOutputRowsPerInputRow, maxOutputRowsPerBatch, maxTokenBytes })` | Split delimited string into rows with optional expansion caps |
| `ops.split(column, names, delimiter?)` | Split column into multiple columns |
| `ops.unpivot(columns, { maxOutputRowsPerInputRow, maxOutputRowsPerBatch })` | Wide to long (melt) with optional expansion caps |
| `ops.stack(file, { tag, tagValue })` | Vertically concatenate another CSV file |

`explode` caps fail fast with `maxTokensPerRow`, `maxOutputRowsPerInputRow`, `maxOutputRowsPerBatch`, or `maxTokenBytes` when a single row or batch would expand beyond the configured limit. `unpivot` supports `maxOutputRowsPerInputRow` and `maxOutputRowsPerBatch`.

### Date/time

| Method | Description |
|--------|-------------|
| `ops.datetime(column, extractOrOptions?)` | Extract parts: `year`, `month`, `day`, `hour`, `minute`, `second`, `weekday`; accepts `{ extract, missing, onTypeError }` |
| `ops.dateTrunc(column, trunc, { result, missing, onTypeError })` | Truncate to: `year`, `month`, `day`, `hour`, `minute`, `second` |

### Other

| Method | Description |
|--------|-------------|
| `ops.flatten()` | Flatten nested columns |
| `ops.interpolate(column, { method, missing, onTypeError })` | Fill nulls in a numeric column; default missing/non-numeric source fails |
| `ops.jsonExtract(path, result, { column, type })` | Extract JSON Pointer/simple JSONPath value into a new column |
| `ops.jsonFilter(path, { op, value, column, type })` | Filter rows by JSON Pointer/simple JSONPath predicate |
| `ops.jsonSchema(schema, { column, mode, result, audit, auditLimit, auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes })` | Validate JSON text with a supported JSON Schema subset; filter-mode audit records support privacy controls |
| `ops.jsonFlatten(fields, { column })` | Append declared JSON Pointer/simple JSONPath fields as bounded output columns |
| `ops.reorder(columns)` | Alias for `select` |
| `ops.dedup(columns?)` | Alias for `unique` |

## Expressions

Used in `filter`, `derive`, `validate`, and `assert`. Reference columns with `col('name')`.

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
| Predicates | `starts_with(s,prefix)` `ends_with(s,suffix)` `contains(s,sub)` `between(x,left,right)` `inrange(x,left,right)` |
| Date/time | `year(x)` `month(x)` `day(x)` `hour(x)` `minute(x)` `second(x)` `weekday(x)` `epoch(x)` `date_trunc(x,unit)` |
| Conditional | `if(cond,then,else)` `case_when(cond,value,...,default)` `case_match(value,key,result,...,default)` `if_any(pred,...)` `if_all(pred,...)` `coalesce(a,b,...)` `nullif(a,b)` |
| Math | `abs(x)` `round(x)` `floor(x)` `ceil(x)` `sign(x)` `pow(x,y)` `sqrt(x)` `log(x)` `exp(x)` `mod(a,b)` `greatest(a,b,...)` `least(a,b,...)` |

Aliases: `substr`=`slice`, `length`=`len`, `lpad`=`pad_left`, `rpad`=`pad_right`, `min`=`least`, `max`=`greatest`. Date/time functions are row-local and accept date/timestamp values plus parseable date/timestamp strings; `weekday()` returns `0=Sunday` through `6=Saturday`.

```js
ops.derive({
  year: expr("year(col('date'))"),
  monthStart: expr("date_trunc(col('date'), 'month')"),
})
```

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
| `sniff` | `csv \| schema infer rows=1000 \| csv` | Bounded-memory schema/type/nullability sniff |
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


## Memory policy

Native Node/WASM execution rejects full-input blocking steps such as `sort`, `pivot`, `normalize`, `acf`, and table encoding unless you opt in for known-small data:

```js
await pipeline('csv | sort age | csv').run({ inputFile: 'small.csv', allowBlocking: true })
```

For capped key-state operators, pass `memory` to validate the conservative native state estimate before execution:

```js
await pipeline('csv | unique city max_keys=10000 | csv').run({ inputFile: 'data.csv', memory: '64MB' })
```

Native Node execution supports spill-backed `sort`, capped unsorted `pivot`, unsorted `unique`/`dedup`, unsorted `group-agg`, capped unsorted `join` inner/left, unsorted `semi-join`/`anti-join`, unsorted set operations, and duplicate-eliminating `union` when `spillDir` is provided. `run()`, `iterChunks()`, `toReadable()`, and `writeTo()` drain finish-time merge output through N-API `finishStep()` instead of waiting for one whole `finish()`. Standalone WASM supports `allowBlocking` and `memory`, but rejects `spillDir` because browser/WASM spill storage would occupy WASM memory. Use the Node native addon, CLI/direct C, or `{ engine: 'duckdb' }` for external spill; with DuckDB, `memory` maps to `memory_limit` and `spillDir` maps to `temp_directory`.

Plan-internal file reads are denied by default in Node and WASM. Pass `allowFs: true` for trusted local lookup files used by `join`, set operations, `union`, or `stack`; pass both `allowFs: true` and `allowRulesFile: true` for `validate rules_file=...`; pass `workspaceRoot` to pin resolved core plan paths inside a trusted directory. Supplying `spillDir` opts into local filesystem spill for Node native execution; standalone WASM still rejects spill. `inputFile` and `inputFiles` are host source adapters and are not controlled by `allowFs`.

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
const sql = await compileToSql(
  'csv | filter "col(age) > 25" | sort -age | head 10 | csv',
  { dialect: 'duckdb' }
)
console.log(sql)
// WITH
//   step_1 AS (SELECT * FROM input_data WHERE ("age" > 25)),
//   step_2 AS (SELECT * FROM step_1 ORDER BY "age" DESC LIMIT 10)
// SELECT * FROM step_2
```

DuckDB is the only implemented SQL dialect today. `dialect: 'sqlite'` and
`dialect: 'postgres'` are recognized but rejected until those dialects have
their own compatibility tests and SQL-generation rules.

### Browser (WASM + DuckDB-WASM)

In the browser, use `@duckdb/duckdb-wasm` with the tranfi WASM module:

```js
import createTranfi from 'tranfi/wasm'
import * as duckdb from '@duckdb/duckdb-wasm'

const tf = await createTranfi()

// SQL generation (synchronous, no DuckDB needed)
const sql = tf.compileToSql('csv | filter "age > 25" | csv', { dialect: 'duckdb' })

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
- **stats** -- newline-delimited execution statistics: run summary plus per-step counters, state estimates, and warnings
- **samples** -- reserved for sampling operators

```js
const result = await p.run({ inputFile: 'data.csv' })
console.log(result.statsText)   // newline-delimited JSON: summary plus step_stats/state_bytes_estimate/warnings
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
2. **WASM** (browsers/fallback) -- same C core compiled to WebAssembly, ~500 KB single-file
3. **DuckDB** (opt-in) -- SQL execution via `{ engine: 'duckdb' }`, requires `npm install duckdb`

```js
// Force WASM backend (e.g., for testing)
import createTranfi from 'tranfi/wasm'
const tf = await createTranfi()
```

## Architecture

The Node.js package wraps the same C11 core used by the CLI, Python, and WASM targets. Data flows through columnar batches with typed columns (`bool`, `int64`, `float64`, `string`, `date`, `timestamp`) and per-cell null bitmaps. `run({ inputFile })` streams files with `createReadStream()` and drains native/WASM main output after each push and each incremental finish boundary; by default it still collects the final output for convenience. `.gz` input files are decompressed through a `zlib.createGunzip()` source transform; use `compression: "none"` to force raw bytes or `compression: "gzip"` to force gzip for `inputFile`/`inputStream`. Use `inputStream`, `toReadable()`, `writeTo()`, `onOutput` with `collectOutput: false`, or `iterChunks()` for large inputs/outputs and backpressure-aware sinks. Native execution is strict by default: row-local and bounded-state operators stream, blocking operators require `allowBlocking: true` or supported `spillDir`, capped key-state plans can be checked with `memory: "64MB"`, and core plan file reads require explicit host-policy options.
