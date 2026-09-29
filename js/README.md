# tranfi (Node.js / WASM)

Tranfi transforms CSV, JSON Lines and plain text as streams. In Node.js it runs
on a native C core; in the browser it runs on WebAssembly. Data flows through in
chunks, so most pipelines use a small, fixed amount of memory however large the
input is. Operations that need the whole input, such as `sort`, must be allowed
explicitly. Tranfi works on byte streams; it is not an in-memory DataFrame
library.

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

<!-- readme-test: js-api -->
```js
const result = await pipeline('csv | filter "col(age) > 25" | top-k 100 age | csv')
  .run({ inputFile: 'data.csv' })
```

## Install

```bash
npm install tranfi
```

Installation compiles Tranfi's C core as a native Node.js addon, so you need a C
compiler. If that compile fails, the install fails.

In the browser, import `tranfi/wasm`: it provides pipelines and prepared
transforms without the native addon. To install only the WebAssembly build, run
`TRANFI_SKIP_NATIVE_BUILD=1 npm install tranfi`. The root `tranfi` entry then
runs pipelines through WebAssembly, but its prepared-transform classes need the
native addon, so use `tranfi/wasm` for those.

| | Linux | Windows | macOS |
|---|---|---|---|
| Native addon | Tested | Not tested | Not tested |
| WebAssembly (`tranfi/wasm`) | Tested in Node.js and Chromium | Tested in Node.js | Not tested |

Use `pipeline(...)` for byte-stream ETL. The separate
`TransformRecipe -> TransformAnalyzer -> TransformPlan -> TransformApply`
lifecycle is for typed batches whose learned state must be frozen and reused.
Calling `.run()` collects output for convenience; use `writeTo()` or
`toReadable()` for large outputs.

## CLI

Installing the package also provides the `tranfi` command:

```bash
# Via npx (no install)
printf 'name,age\nAlice,30\nBob,25\n' | npx tranfi -q 'csv | filter "age > 25" | csv'

# Or install globally
npm i -g tranfi
tranfi -q 'csv | filter "age > 25" | top-k 100 age | csv' < data.csv
tranfi profile < data.csv
tranfi -R  # list recipes
```

Run `tranfi -h` for the npm CLI options. Use `--allow-blocking` for known-small
full-input operations, `--memory max:64MB` for a memory policy, and
`--spill-dir DIR` for an existing private spill directory. `--stats-json FILE`
writes the stats channel; `--target json|sql` compiles without executing.
The detailed `--explain` report requires the standalone C CLI built from source.

## Quick start

### Two APIs

**Builder API** -- composable, structured, and IDE-friendly:

<!-- readme-test: js-api -->
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

<!-- readme-test: js-api -->
```js
const p = pipeline('csv | filter "col(score) >= 80" | top-k 10 score | csv')
const result = await p.run({ inputFile: 'students.csv' })
```

Both produce identical pipelines under the hood.

Dataframe-style DSL aliases are accepted and normalize to canonical ops: `mutate` -> `derive`, `summarise`/`summarize` -> `group-agg`, `distinct` -> `unique`, and `arrange` -> `sort`.

### Running pipelines

<!-- readme-test: js-pipeline -->
```js
// From string or Buffer
const result = await p.run({ input: 'name,age\nAlice,30\n' })

// From file (streamed in 64 KB chunks)
const fileResult = await p.run({ inputFile: 'data.csv' })

// Access results
result.output         // Buffer
result.outputText     // string (UTF-8 decoded)
result.errors         // Buffer (error channel)
result.stats          // Buffer (pipeline stats)
result.statsText      // string
result.samples        // Buffer (sample channel)
```

For large outputs, drain chunks instead of collecting `result.output`:

<!-- readme-test: js-pipeline -->
```js
import { createReadStream, createWriteStream } from 'node:fs'

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

`sourceColumn` appends the path for each input row without preloading file contents. Tranfi flushes decoder input at each file boundary so an unterminated final record belongs to the correct source file. Repeated headers are kept by default. Use `codec.csv({ skipRepeatedHeader: true })` to skip a later file's first non-comment record when it exactly matches the original header; matching rows inside a file remain data.

Standalone WASM exposes the same non-collecting shape for in-memory data: `tf.run(dsl, data, { onOutput, collectOutput: false })` and `tf.iterChunks(dsl, data)`.

For browser UI work, keep Worker placement outside the core pipeline and use the optional WASM Worker adapter:

<!-- readme-test: browser -->
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

Learn imputation, scaling or category mappings from reference data, then apply
the same plan to new batches. Analyze the reference batches, finalize the plan,
and apply it. Replaying the reference data through that plan gives a second-pass
`fit_transform` workflow.

Inputs are typed `float32`/`float64` columns. This example learns mean imputation
and standard scaling, applies them to the reference batch, and exports the plan:

```js
import tf from 'tranfi'

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

<details markdown="1">
<summary>Available methods, missing values and kind inference</summary>

Recipe fields use the same names in all bindings.

| Recipe field | Choices / behavior |
|--------------|--------------------|
| Numeric `impute.op` | `none`, `zero`, `constant`, `mean`, exact `median` |
| Numeric `normalize.op` | `none`, `standard`, `minmax` |
| Categorical `impute.op` | `none` or mode; mode supports `allMissing` of `error` or `zero` |
| Categorical `encode.op` | `none`, `label`, `onehot` |
| Label unknown policy | `error`, `sentinel`, `other` |
| One-hot unknown policy | `error`, `all_zero`, `other` |

**No imputation or encoding:** finite values pass through; missing input becomes
canonical qNaN. This combination needs no learned category dictionary.
Mode imputation with no encoding retains a dictionary and rejects unseen finite
values with error `108`.

**Encoding:** label ordinals are zero-based and sorted; one-hot blocks follow
source order. A label sentinel must be a safe integer outside the known ordinal
range. `other` appends an ordinal or field after known categories. Without
imputation, missing input follows the encoder's unknown policy.

Generated IDs, names and category metadata are deterministic. Collisions fail
with error `102`.

**Kind inference:** configure both numeric and categorical branches, set
`kind.op` to `infer`, `kind.rule` to `finite-integer-cardinality-v1`, and
`kind.maxCategories` to at least `2`. Missing/NaN values are ignored.

| Observed reference values | Selected branch |
|---------------------------|-----------------|
| `2..maxCategories` distinct finite integers | Categorical |
| Zero or one distinct value, any noninteger, or more than `maxCategories` | Numeric |

</details>

<details markdown="1">
<summary>Fixed category dictionaries</summary>

Set `encode.categories` to a nonempty, sorted, unique array of finite typed tags.
For example, `{ "t": "f64", "v": "4000000000000000" }` represents `2`.
Tags must match the input dtype; represent negative zero as positive zero.

- Unobserved categories stay in the dictionary.
- Unknown training values follow the encoder policy and never vote for mode.
- An all-missing zero fallback must exist in the dictionary.
- Fixed encoding without imputation can finalize without training rows.

String categories, categorical constant imputation and fixed dictionaries
combined with kind inference are not supported.

</details>

<details markdown="1">
<summary>Limits, errors and cancellation</summary>

Exact median obeys allocation and resident-state limits. Inference, category
discovery and output expansion also enforce category/output-width limits.
Resource-limit failures use error `104`.

Host-policy and spill settings are reserved. A non-null host policy or nonempty
spill path fails with unsupported-runtime error `113`.

Runtime options reject unknown fields:

| Runtime | Options |
|---------|---------|
| Native Node | `limits`, `cancelFlag`, `hostPolicy`, `spillDir` |
| Standalone WASM | `limits`, `cancelToken`, `hostPolicy`, `spillDir` |

Cancellation spellings are not interchangeable. WASM tokens created with
`createTransformCancelToken()` expose a read-only `requested` boolean for host
conversion loops; reading a closed token fails.

`tranfi/wasm` exposes the same classes on the initialized module. The Worker adapter adds `analyzeTransform()` and `applyTransform()`. With `SharedArrayBuffer`, an `AbortSignal` interrupts a synchronous C call through an atomic poll cell. Without it, cancellation terminates the whole worker and reclaims its WASM heap. Pass a worker URL directly so the client can recreate it, or supply an owned worker plus `workerFactory`:

<!-- readme-test: browser -->
```js
const workerUrl = new URL('./tranfi-worker.js', import.meta.url)
const makeWorker = () => new Worker(workerUrl, { type: 'module' })
const client = createWorkerClient(makeWorker(), {
  workerFactory: makeWorker,
  terminateOnDispose: true
})
```

All prepared-transform failures use `TranfiTransformError`; its numeric `code` is stable across native Node and WASM. Native Node accepts a SharedArrayBuffer-backed `Int32Array` as `cancelFlag`: cell 0 is the cancellation request, and an optional cell 1 is incremented modulo 2^32 at every native poll so another realm can observe operation progress without a timer.

</details>

## Codecs

Start with a decoder and finish with an encoder. Input and output formats can differ.

| Format | Decoder | Encoder |
|--------|---------|---------|
| CSV | `codec.csv(options)` | `codec.csvEncode({ delimiter })` |
| JSON Lines | `codec.jsonl({ batchSize, onError, maxErrorBytes, maxRecordBytes })` | `codec.jsonlEncode()` |
| Text (one `_line` column) | `codec.text({ batchSize, maxErrorBytes, maxRecordBytes })` | `codec.textEncode()` |
| Markdown table | — | `codec.tableEncode({ maxWidth, maxRows })` |

**CSV defaults to permissive field-count handling.** Choose `mode: 'strict'`
(or `strict: true`) to reject width mismatches. Choose `repair: true`
(or `mode: 'repair'`) to pad/truncate rows and collect diagnostics in `result.errors`.

**Malformed JSONL is skipped by default.** Set `onError` to `fail` to stop,
`warn` to keep valid rows with diagnostics, or `quarantine` to route bad records
to error diagnostics. Read those diagnostics from `result.errors`.

Cross-codec pipelines work naturally:

<!-- readme-test: js-api -->
```js
// CSV in, JSONL out
pipeline([codec.csv(), ops.head(5), codec.jsonlEncode()])

// JSONL in, CSV out
pipeline([codec.jsonl(), ops.sort(['name']), codec.csvEncode()])
```

<details markdown="1">
<summary>CSV options for nulls, row limits and whitespace</summary>

| Need | Option | Behavior |
|------|--------|----------|
| Field separator / header | `delimiter`, `header` | Configure CSV decoding; encoder accepts `delimiter` |
| Add null markers | `nulls: ['NA', 'NULL']` | Adds to default unquoted-empty-field null handling |
| Preserve quoted markers | `quotedNulls: false` | Keeps quoted `"NA"` and `""` as strings |
| Skip a preamble | `skip: 2` | Before schema/header discovery; comments are handled afterward |
| Limit rows | `nMax: 100` or `maxRows: 100` | One shared counter after skip/comment/header handling |
| Keep only the schema | `nMax: 0` | Preserves a header-only batch |
| Comments | `comment: '#'` | Removes text after unquoted markers and skips comment-only rows; quoted markers remain |
| Preserve spaces/tabs | `trimWs: false` | Disables default trimming of unquoted values |
| Drop blank rows | `skipEmptyRows: true` | Otherwise blank physical rows after the header become all-null rows |
| Read CSV shards | `skipRepeatedHeader: true` | Skips matching headers only at explicit file boundaries |

</details>

<details markdown="1">
<summary>Decoder limits and repair diagnostics</summary>

All decoder size options are checked integers:

| Option | Range | Behavior |
|--------|-------|----------|
| `batchSize` | `1..65536` | Rows per batch |
| `maxErrorBytes` | `0..67108864` | Bounds raw diagnostic previews |
| `maxRecordBytes` | `0..1073741824` | Default `67108864` bytes (64 MiB); `0` disables the record guard |
| `maxColumns` (CSV) | `1..65536` | Default `8192`; overflow fails with `csv_too_many_columns` |

Record limits apply before a newline is seen, including CSV, JSONL and text.
CSV record overflow fails with `csv_record_too_large`.

Repair audit/raw payloads accept `auditIncludeRow, auditColumns, auditRedact, auditHashColumns, auditMaxBytes, auditMaxCellBytes`. The raw preview is exposed to
those controls as pseudo-column `raw`. CSV repair also accepts
`audit, auditLimit`.

</details>

<details markdown="1">
<summary>JSONL record and schema rules</summary>

Each record must be one complete UTF-8 JSON object. Invalid number syntax,
trailing content, embedded NULs and numbers outside finite float64 range follow
the configured malformed-record policy. The encoder rejects nonfinite values.

The first valid record determines schema order. Repeated keys use their first
value. `maxErrorBytes` bounds diagnostic previews, and `maxRecordBytes` caps the current line.

</details>

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

<!-- readme-test: js-api -->
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

<!-- readme-test: js-api -->
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

<!-- readme-test: js-api -->
```js
ops.derive({
  year: expr("year(col('date'))"),
  monthStart: expr("date_trunc(col('date'), 'month')"),
})
```

## Recipes

Built-in named pipelines for common tasks. Use by name:

<!-- readme-test: js-api -->
```js
const result = await pipeline('preview').run({ inputFile: 'data.csv' })
const frequencies = await pipeline('freq').run({ inputFile: 'data.csv' })
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

<!-- readme-test: js-api -->
```js
await pipeline('csv | sort age | csv').run({ inputFile: 'small.csv', allowBlocking: true })
```

For capped key-state operators, pass `memory` to validate the conservative native state estimate before execution:

<!-- readme-test: js-api -->
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

<!-- readme-test: js-data -->
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

<!-- readme-test: js-sql -->
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

<!-- readme-test: browser -->
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
import { compileDsl, saveRecipe, loadRecipe, codec, ops } from 'tranfi'

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

<!-- readme-test: js-pipeline -->
```js
const result = await p.run({ inputFile: 'data.csv' })
console.log(result.statsText)   // newline-delimited JSON: summary plus step_stats/state_bytes_estimate/warnings
```

### Pipeline from JSON

```js
import { loadRecipe } from 'tranfi'

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
2. **WASM** (browsers/fallback) -- same C core compiled to WebAssembly, single-file module
3. **DuckDB** (opt-in) -- SQL execution via `{ engine: 'duckdb' }`, requires `npm install duckdb`

```js
// Force WASM backend (e.g., for testing)
import createTranfi from 'tranfi/wasm'
const tf = await createTranfi()
```

## Architecture

Node, Python, CLI and WASM use the same C11 engine. It processes columnar batches
with typed columns (`bool`, `int64`, `float64`, `string`, `date`, `timestamp`)
and per-cell null bitmaps.

| Need | Node API behavior |
|------|-------------------|
| Stream input | `run({ inputFile })` uses `createReadStream()` and drains output after each push and incremental finish boundary |
| Avoid collecting all output | Use `inputStream`, `toReadable()`, `writeTo()`, `iterChunks()`, or `onOutput` with `collectOutput: false` |
| Read gzip | `.gz` files use `zlib.createGunzip()`; override with `compression: "none"` or `"gzip"` for `inputFile`/`inputStream` |
| Permit a blocking operation | Set `allowBlocking: true` or use a supported `spillDir` path |
| Cap retained key state | Supply caps and check the plan with `memory: "64MB"` |
| Allow plan file reads | Supply explicit host-policy options |

Input streaming still collects the final output by default. Choose a streaming
sink for large outputs; row-local and bounded-state operators stream under the
default strict native memory policy.
