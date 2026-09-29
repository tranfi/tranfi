# tranfi (Python)

Tranfi transforms CSV, JSON Lines and plain text as streams, on a native C core.
Data flows through in chunks, so most pipelines use a small, fixed amount of
memory however large the input is. Operations that need the whole input, such as
`sort`, must be allowed explicitly. Tranfi works on byte streams; it is not a
pandas-style DataFrame library.

```python
import tranfi as tf

result = tf.pipeline([
    tf.codec.csv(),
    tf.ops.filter(tf.expr("col('age') > 25")),
    tf.ops.top(100, 'age'),
    tf.ops.derive({'label': tf.expr("if(col('age')>30, 'senior', 'junior')")}),
    tf.ops.select(['name', 'age', 'label']),
    tf.codec.csv_encode(),
]).run(input=b'name,age\nAlice,30\nBob,25\nCharlie,35\nDiana,28\n')

print(result.output_text)
# name,age,label
# Charlie,35,senior
# Alice,30,junior
# Diana,28,junior
```

Or use the pipe DSL for one-liners:

<!-- readme-test: python-api -->
```python
result = tf.pipeline('csv | filter "col(age) > 25" | top-k 100 age | csv').run(input_file='data.csv')
```

## Install

```bash
pip install tranfi
```

Requires Python 3.9 or newer. Tranfi is distributed as source and compiled
during installation, so you need a C compiler. The Python package is tested on
Linux; Windows and macOS are not tested yet.

Or from source:

```bash
cmake -S . -B build
cmake --build build
pip install -e py/
```

Use `pipeline(...)` for byte-stream ETL. The separate
`TransformRecipe -> TransformAnalyzer -> TransformPlan -> TransformApply`
lifecycle is for typed batches whose learned state must be frozen and reused.
Calling `.run()` collects output for convenience; use `iter_chunks()` or an
`on_output` callback with `collect_output=False` for large outputs.

## CLI

Installing the package also installs the `tranfi` command:

```bash
# Filter and retain the top 100 rows by age
tranfi -q 'csv | filter "age > 25" | top-k 100 age | csv' < data.csv

# Built-in recipe
tranfi profile < data.csv

# File I/O
tranfi -i input.csv -o output.csv 'csv | select name,age | csv'

# List recipes
tranfi -R
```

Run `tranfi -h` for the pip CLI options. Use `--allow-blocking` for known-small
full-input operations, `--memory max:64MB` for a memory policy, and
`--spill-dir DIR` for an existing private spill directory. `--stats-json FILE`
writes the stats channel; `--target json|sql` compiles without executing.
The detailed `--explain` report requires the standalone C CLI built from source.

## Quick start

### Two APIs

**Builder API** -- composable and structured:

<!-- readme-test: python-api -->
```python
p = tf.pipeline([
    tf.codec.csv(),
    tf.ops.filter(tf.expr("col('score') >= 80")),
    tf.ops.derive({'grade': tf.expr("if(col('score')>=90, 'A', 'B')")}),
    tf.ops.top(10, 'score'),
    tf.codec.csv_encode(),
])
result = p.run(input_file='students.csv')
```

**DSL strings** -- compact, suitable for CLI-like use:

<!-- readme-test: python-api -->
```python
p = tf.pipeline('csv | filter "col(score) >= 80" | top-k 10 score | csv')
result = p.run(input_file='students.csv')
```

Both produce identical pipelines under the hood.

Dataframe-style DSL aliases are accepted and normalize to canonical ops: `mutate` -> `derive`, `summarise`/`summarize` -> `group-agg`, `distinct` -> `unique`, and `arrange` -> `sort`.

### Running pipelines

<!-- readme-test: python-pipeline -->
```python
# From bytes
result = p.run(input=b'name,age\nAlice,30\n')

# From file (streamed in 64 KB chunks)
result = p.run(input_file='data.csv')

# Access results
result.output         # bytes
result.output_text    # str (UTF-8 decoded)
result.errors         # bytes (error channel)
result.stats          # bytes (pipeline stats)
result.stats_text     # str
result.samples        # bytes (sample channel)
```

For large outputs, drain chunks instead of collecting `result.output`:

<!-- readme-test: python-pipeline -->
```python
# Callback sink; result.output is empty when collect_output=False
with open('out.csv', 'wb') as out:
    result = p.run(input_file='data.csv', on_output=out.write, collect_output=False)

# Iterator form
for chunk in p.iter_chunks(input_file='data.csv'):
    process(chunk)

# .gz input files are decompressed as streaming sources by default
result = p.run(input_file='events.jsonl.gz')
raw = p.run(input_file='events.jsonl.gz', compression='none')

# Multiple files stream sequentially through one pipeline.
parts = ['part-a.csv', 'part-b.csv']
result = p.run(input_files=parts, source_column='src')
for chunk in p.iter_chunks(input_files=parts, source_column='src'):
    process(chunk)
```

`source_column` appends the path for each input row without preloading file contents. Tranfi flushes decoder input at each file boundary so an unterminated final record belongs to the correct source file. Repeated headers are kept by default. Use `tf.codec.csv(skip_repeated_header=True)` to skip a later file's first non-comment record when it exactly matches the original header; matching rows inside a file remain data.

### Prepared reusable transforms

Learn imputation, scaling or category mappings from reference data, then apply
the same plan to new batches. Analyze the reference batches, finalize the plan,
and apply it. Replaying the reference data through that plan gives a second-pass
`fit_transform` workflow.

Inputs are typed `float32`/`float64` columns. This example learns mean imputation
and standard scaling, applies them to the reference batch, and exports the plan:

```python
from array import array
import json
import tranfi as tf

recipe_spec = {
    'format': 'tranfi.transform-recipe',
    'version': 1,
    'policyVersion': 1,
    'outputDtype': 'float64',
    'semanticLimits': {
        'maxOutputColumns': 65536,
        'maxOutputElementsPerApply': 134217728,
    },
    'columns': [{
        'sourceId': 'x0',
        'kind': {
            'op': 'declared', 'value': 'numeric',
            'rule': None, 'maxCategories': None,
        },
        'numeric': {
            'impute': {'op': 'mean', 'constant': None, 'allMissing': 'zero'},
            'normalize': {'op': 'standard', 'ddof': 0},
        },
        'categorical': None,
    }],
}
schema = [{'id': 'x0', 'dtype': 'float64'}]
reference = {
    'rows': 3,
    'columns': [array('d', [1.0, float('nan'), 3.0])],
}

with tf.TransformRecipe.from_json(json.dumps(recipe_spec)) as recipe:
    with recipe.analyzer(schema) as analyzer:
        analyzer.push(reference)
        with analyzer.finalize() as plan:
            with plan.apply(schema) as apply:
                fitted_reference = apply.run(reference)
            plan_bytes = plan.to_bytes()  # canonical TFTR artifact
            recipe_sha256 = plan.recipe_sha256()
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

Prepared-transform failures raise `TranfiTransformError`; its numeric `code` is stable across the C and Python APIs. `TransformLimits` starts from the C-owned safe profile and applies explicit overrides.
For cooperative cancellation, create a `TransformCancelToken`, pass it to `recipe.analyzer(..., cancel_token=token)`, `plan.apply(..., cancel_token=token)`, or `TransformPlan.from_bytes(..., cancel_token=token)`, and call `token.request()` from another Python thread. Active analyzer/apply sessions retain the token for their lifetime.

</details>

## Codecs

Start with a decoder and finish with an encoder. Input and output formats can differ.

| Format | Decoder | Encoder |
|--------|---------|---------|
| CSV | `codec.csv(...)` | `codec.csv_encode(delimiter)` |
| JSON Lines | `codec.jsonl(batch_size, on_error, max_error_bytes, max_record_bytes)` | `codec.jsonl_encode()` |
| Text (one `_line` column) | `codec.text(batch_size, max_error_bytes, max_record_bytes)` | `codec.text_encode()` |
| Markdown table | — | `codec.table_encode(max_width, max_rows)` |

**CSV defaults to permissive field-count handling.** Choose `mode='strict'`
(or `strict=True`) to reject width mismatches. Choose `repair=True`
(or `mode='repair'`) to pad/truncate rows and collect diagnostics in `result.errors`.

**Malformed JSONL is skipped by default.** Set `on_error` to `fail` to stop,
`warn` to keep valid rows with diagnostics, or `quarantine` to route bad records
to error diagnostics. Read those diagnostics from `result.errors`.

Cross-codec pipelines work naturally:

<!-- readme-test: python-api -->
```python
# CSV in, JSONL out
tf.pipeline([tf.codec.csv(), tf.ops.head(5), tf.codec.jsonl_encode()])

# JSONL in, CSV out
tf.pipeline([tf.codec.jsonl(), tf.ops.sort(['name']), tf.codec.csv_encode()])
```

<details markdown="1">
<summary>CSV options for nulls, row limits and whitespace</summary>

| Need | Option | Behavior |
|------|--------|----------|
| Field separator / header | `delimiter`, `header` | Configure CSV decoding; encoder accepts `delimiter` |
| Add null markers | `nulls=['NA', 'NULL']` | Adds to default unquoted-empty-field null handling |
| Preserve quoted markers | `quoted_nulls=False` | Keeps quoted `"NA"` and `""` as strings |
| Skip a preamble | `skip=2` | Before schema/header discovery; comments are handled afterward |
| Limit rows | `n_max=100` or `max_rows=100` | One shared counter after skip/comment/header handling |
| Keep only the schema | `n_max=0` | Preserves a header-only batch |
| Comments | `comment='#'` | Removes text after unquoted markers and skips comment-only rows; quoted markers remain |
| Preserve spaces/tabs | `trim_ws=False` | Disables default trimming of unquoted values |
| Drop blank rows | `skip_empty_rows=True` | Otherwise blank physical rows after the header become all-null rows |
| Read CSV shards | `skip_repeated_header=True` | Skips matching headers only at explicit file boundaries |

</details>

<details markdown="1">
<summary>Decoder limits and repair diagnostics</summary>

All decoder size options are checked integers:

| Option | Range | Behavior |
|--------|-------|----------|
| `batch_size` | `1..65536` | Rows per batch |
| `max_error_bytes` | `0..67108864` | Bounds raw diagnostic previews |
| `max_record_bytes` | `0..1073741824` | Default `67108864` bytes (64 MiB); `0` disables the record guard |
| `max_columns` (CSV) | `1..65536` | Default `8192`; overflow fails with `csv_too_many_columns` |

Record limits apply before a newline is seen, including CSV, JSONL and text.
CSV record overflow fails with `csv_record_too_large`.

Repair audit/raw payloads accept `audit_include_row, audit_columns, audit_redact, audit_hash_columns, audit_max_bytes, audit_max_cell_bytes`. The raw preview is exposed to
those controls as pseudo-column `raw`. CSV repair also accepts
`audit, audit_limit`.

</details>

<details markdown="1">
<summary>JSONL record and schema rules</summary>

Each record must be one complete UTF-8 JSON object. Invalid number syntax,
trailing content, embedded NULs and numbers outside finite float64 range follow
the configured malformed-record policy. The encoder rejects nonfinite values.

The first valid record determines schema order. Repeated keys use their first
value. `max_error_bytes` bounds diagnostic previews, and `max_record_bytes` caps the current line.

</details>

## Operators

### Row filtering

| Method | Description |
|--------|-------------|
| `ops.filter(expr, audit=False, audit_limit=None, audit_include_row=None, audit_columns=None, audit_redact=None, audit_hash_columns=None, audit_max_bytes=None, audit_max_cell_bytes=None)` | Keep rows matching expression; optional dropped-row audit records support row omission, column allowlists, redaction, hashes, and payload caps |
| `ops.head(n)` | First N rows |
| `ops.tail(n)` | Last N rows |
| `ops.skip(n)` | Skip first N rows |
| `ops.top(n, column, desc=True)` | Top N by column value |
| `ops.sample(n, seed=0)` | Deterministic bounded reservoir sampling; use `seed='random'` for nondeterministic mode |
| `ops.grep(pattern, invert, column, regex)` | Substring/regex filter |
| `ops.validate(expr=None, rules=None, rules_file=None, audit=False, audit_limit=None, max_failures=None, warn_failure_rate=None, max_failure_rate=None, name=None, message=None, audit_include_row=None, audit_columns=None, audit_redact=None, audit_hash_columns=None, audit_max_bytes=None, audit_max_cell_bytes=None)` | Add `_valid` boolean column, keep all rows; supports inline rules or local JSON rule-suite files; bounded failure audit records support count/rate thresholds plus privacy controls |
| `ops.assert_(expr=None, action='fail', name='assert', message='', result='_assert', aggregate=None, op=None, value=None, column=None, tolerance=None, rel=None, audit_include_row=None, audit_columns=None, audit_redact=None, audit_hash_columns=None, audit_max_bytes=None, audit_max_cell_bytes=None)` | Row-local data-quality rule or finish-time O(1) aggregate assertion; row-local failure side-channel records support privacy controls |
| `ops.quarantine(expr, name=None, message=None, audit_include_row=None, audit_columns=None, audit_redact=None, audit_hash_columns=None, audit_max_bytes=None, audit_max_cell_bytes=None)` | Route rows matching expression to `errors` and drop them from main output; row payloads support privacy controls |
| `ops.schema(columns=None, required=None, non_null=None, nullable=None, values=None, min=None, max=None, regex=None, mode='fail', result='_schema', max_regex_pattern_bytes=None, max_regex_cell_bytes=None, audit_include_row=None, audit_columns=None, audit_redact=None, audit_hash_columns=None, audit_max_bytes=None, audit_max_cell_bytes=None)` | Row-local table schema contract; fail, warn, filter, quarantine, or annotate; regex budgets default to 4096-byte patterns and 65536-byte cells; schema audit/error records support row omission, column allowlists, redaction, stable non-cryptographic hashes, and row/cell payload caps |
| `ops.schema_infer(rows=None)` | Bounded decoded-type/nullability schema report; defaults to 10000 sampled rows |
| `ops.tee(expr=None, channel='samples', columns=None, limit=1000, every=1, name='tee', include_row=True, audit_include_row=None, audit_columns=None, audit_redact=None, audit_hash_columns=None, audit_max_bytes=None, audit_max_cell_bytes=None)` | Preserve main rows and write bounded JSONL row snapshots to a side channel; row payloads support privacy controls |

### Column operations

| Method | Description |
|--------|-------------|
| `ops.select(columns)` | Keep and reorder columns |
| `ops.relocate(columns, before=None, after=None)` | Move columns while preserving all columns |
| `ops.rename(**mapping)` | Rename columns: `rename(name='full_name')` |
| `ops.derive(columns)` | Computed columns: `derive({'total': expr("col('a')*col('b')")})` |
| `ops.source_name(result='_source', default='')` | Append the current host source path/name as a row-local string column |
| `ops.across(columns, fn=..., functions=..., names=..., replace=...)` | Apply row-local functions over selected columns |
| `ops.cast(audit=False, audit_limit=None, audit_include_row=None, audit_columns=None, audit_redact=None, audit_hash_columns=None, audit_max_bytes=None, audit_max_cell_bytes=None, **mapping)` | Type conversion; optional bounded value/coercion audit records support privacy controls |
| `ops.trim(columns)` | Strip whitespace |
| `ops.fill_null(audit=False, audit_limit=None, audit_include_row=None, audit_columns=None, audit_redact=None, audit_hash_columns=None, audit_max_bytes=None, audit_max_cell_bytes=None, **mapping)` | Replace nulls; optional bounded audit records support privacy controls |
| `ops.fill_down(columns)` | Forward-fill nulls |
| `ops.clip(column, min, max)` | Clamp numeric values |
| `ops.replace(column, pattern, replacement, regex=False, audit=False, audit_limit=None, audit_include_row=None, audit_columns=None, audit_redact=None, audit_hash_columns=None, audit_max_bytes=None, audit_max_cell_bytes=None)` | String find/replace; optional bounded audit records support privacy controls |
| `ops.hash(columns)` | Add `_hash` column (DJB2) |
| `ops.bin(column, boundaries, missing=None, on_type_error=None)` | Discretize into bins with strict numeric-source defaults |
| `ops.ewma(column, alpha, result=None, missing=None, on_type_error=None)` | Exponentially weighted moving average |
| `ops.anomaly(column, threshold=3.0, result=None, missing=None, on_type_error=None)` | Streaming z-score anomaly flag |
| `ops.normalize(columns, method='minmax', audit=False, audit_limit=None, missing=None, on_type_error=None, audit_include_row=None, audit_columns=None, audit_redact=None, audit_hash_columns=None, audit_max_bytes=None, audit_max_cell_bytes=None)` | Blocking minmax/zscore normalization; optional audit records support privacy controls |
| `ops.acf(column, lags=20, missing=None, on_type_error=None)` | Blocking autocorrelation table |

`columns` may contain exact names or selector strings: `id:score`, `starts_with(score_)`, `ends_with(_id)`, `contains(temp)`, `matches(^score_)`, `where(numeric)`, strict `all_of(score,name)`, lenient `any_of(optional,score)`, exclusions with `!name` or `-name`, and boolean selector algebra such as `starts_with(score_)&where(numeric)`, `starts_with(score_)&!ends_with(raw)`, or `!(id:score)`. Ranges use input schema order and can be reversed. Helper matching is case-insensitive; exact names are case-sensitive. These selectors are resolved by the native schema-aware `select`, `relocate`, and `across` ops in `O(columns)` without retaining rows; SQL lowering rejects selector helpers without a known schema.

Example: `tf.ops.across(['starts_with(score_)'], fn='round')` replaces selected numeric columns per row; `tf.ops.across(['name'], functions=['lower'], replace=False, names='{col}_{fn}')` appends templated columns. This is native/WASM row-local execution, not arbitrary lambdas or grouped dplyr evaluation.

### Sorting and deduplication

| Method | Description |
|--------|-------------|
| `ops.sort(columns)` | Sort rows. Prefix `-` for descending: `sort(['-age', 'name'])` |
| `ops.unique(columns)` | Deduplicate on specified columns |

### Aggregation

| Method | Description |
|--------|-------------|
| `ops.stats(stats_list)` | Column statistics. Stats: `count`, `min`, `max`, `sum`, `avg`, `stddev`, `variance`, `median`, `p25`, `p75`, `p90`, `p99`, `distinct`, `hist`, `sample` |
| `ops.frequency(columns, max_values=None, max_state_bytes=None, overflow=None, other=None, audit=False, audit_limit=None, audit_include_row=None, audit_columns=None, audit_redact=None, audit_hash_columns=None, audit_max_bytes=None, audit_max_cell_bytes=None)` | Value counts; `overflow="other"` can emit bounded category-overflow audit records with privacy controls |
| `ops.group_agg(group_by, aggs)` | Group by + aggregate. `count` on a column counts non-null values; `column='*'` counts rows. |

<!-- readme-test: python-api -->
```python
# Group aggregation
tf.ops.group_agg(['city'], [
    {'column': 'price', 'func': 'sum', 'name': 'total'},
    {'column': 'price', 'func': 'avg', 'name': 'avg_price'},
    {'column': '*', 'func': 'count', 'name': 'rows'},
])
```

### Sequential / window

| Method | Description |
|--------|-------------|
| `ops.step(column, func, result)` | Running aggregation: `running-sum`, `running-avg`, `running-min`, `running-max`, `lag` |
| `ops.window(column, size, func, result=None, missing=None, on_type_error=None)` | Sliding window: `avg`, `sum`, `min`, `max`; default missing/non-numeric source fails |
| `ops.rolling_sum/mean/min/max(column, size, result=None, missing=None, on_type_error=None)` | Named trailing fixed-row numeric windows; default missing/non-numeric source fails |
| `ops.rolling_any/all(column, size, result, nulls='ignore')` | Boolean trailing windows; `nulls`: `ignore`, `false`, `true`, `propagate` |
| `ops.lead(column, offset, result)` | Lookahead N rows |
| `ops.lag(column, offset, result)` | Previous-row shift |
| `ops.shift(column, offset, result, type)` | Shift alias; `type='lag'` default or `type='lead'` |
| `ops.rowid(columns, result, sorted, max_keys)` | Global or per-key 1-based row ids |
| `ops.rleid(columns, result)` | Consecutive run id by selected column(s) |

### Reshape

| Method | Description |
|--------|-------------|
| `ops.explode(column, delimiter, max_tokens_per_row, max_output_rows_per_input_row, max_output_rows_per_batch, max_token_bytes)` | Split delimited string into rows with optional expansion caps |
| `ops.split(column, names, delimiter)` | Split column into multiple columns |
| `ops.unpivot(columns, max_output_rows_per_input_row, max_output_rows_per_batch)` | Wide to long (melt) with optional expansion caps |
| `ops.stack(file, tag, tag_value)` | Vertically concatenate another CSV file |

`explode` caps fail fast with `max_tokens_per_row`, `max_output_rows_per_input_row`, `max_output_rows_per_batch`, or `max_token_bytes` when a single row or batch would expand beyond the configured limit. `unpivot` supports `max_output_rows_per_input_row` and `max_output_rows_per_batch`.

### Date/time

| Method | Description |
|--------|-------------|
| `ops.datetime(column, extract, missing=None, on_type_error=None)` | Extract parts: `year`, `month`, `day`, `hour`, `minute`, `second`, `weekday` |
| `ops.date_trunc(column, trunc, result=None, missing=None, on_type_error=None)` | Truncate to: `year`, `month`, `day`, `hour`, `minute`, `second` |

### Other

| Method | Description |
|--------|-------------|
| `ops.flatten()` | Flatten nested columns |
| `ops.interpolate(column, method='linear', missing=None, on_type_error=None)` | Fill nulls in a numeric column; default missing/non-numeric source fails |
| `ops.json_extract(path, result, column='_line', type='string')` | Extract JSON Pointer/simple JSONPath value into a new column |
| `ops.json_filter(path, op='exists', value=None, column='_line', type='auto')` | Filter rows by JSON Pointer/simple JSONPath predicate |
| `ops.json_schema(schema, column='_line', mode='annotate', result='_valid', audit=False, audit_limit=None, audit_include_row=None, audit_columns=None, audit_redact=None, audit_hash_columns=None, audit_max_bytes=None, audit_max_cell_bytes=None)` | Validate JSON text with a supported JSON Schema subset; filter-mode audit records support privacy controls |
| `ops.json_flatten(fields, column='_line')` | Append declared JSON Pointer/simple JSONPath fields as bounded output columns |
| `ops.reorder(columns)` | Alias for `select` |
| `ops.dedup(columns)` | Alias for `unique` |

## Expressions

Used in `filter`, `derive`, `validate`, and `assert_`. Reference columns with `col('name')`.

<!-- readme-test: python-api -->
```python
tf.ops.filter(tf.expr("col('age') > 25 and contains(col('name'), 'A')"))
tf.ops.derive({
    'full':  tf.expr("concat(col('first'), ' ', col('last'))"),
    'grade': tf.expr("if(col('score')>=90, 'A', if(col('score')>=80, 'B', 'C'))"),
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

<!-- readme-test: python-api -->
```python
tf.ops.derive({
    'year': tf.expr("year(col('date'))"),
    'month_start': tf.expr("date_trunc(col('date'), 'month')"),
})
```

## Recipes

Built-in named pipelines for common tasks. Use by name:

<!-- readme-test: python-api -->
```python
result = tf.pipeline('preview').run(input_file='data.csv')
result = tf.pipeline('freq').run(input_file='data.csv')
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

<!-- readme-test: python-api -->
```python
for r in tf.recipes():
    print(f"{r['name']:15} {r['description']}")
```


## Memory policy

Native execution rejects full-input blocking steps such as `sort`, `pivot`, `normalize`, `acf`, and table encoding unless you opt in for known-small data:

<!-- readme-test: python-api -->
```python
tf.pipeline('csv | sort age | csv').run(input_file='small.csv', allow_blocking=True)
```

For capped key-state operators, pass `memory` to validate the conservative native state estimate before execution:

<!-- readme-test: python-api -->
```python
tf.pipeline('csv | unique city max_keys=10000 | csv').run(input_file='data.csv', memory='64MB')
```

Native Python execution supports spill-backed `sort`, capped unsorted `pivot`, unsorted `unique`/`dedup`, unsorted `group-agg`, capped unsorted `join` inner/left, unsorted `semi-join`/`anti-join`, unsorted set operations, and duplicate-eliminating `union` when `spill_dir` is provided. For large outputs, use `on_output=...` with `collect_output=False` or `iter_chunks(spill_dir=...)`; those paths drain finish-time merge output through the C sink callback or `tf_pipeline_finish_step()` instead of retaining it in the C output buffer. With `engine='duckdb'`, `memory` maps to DuckDB `memory_limit` and `spill_dir` maps to `temp_directory`.

Plan-internal file reads are denied by default in Python. Pass `allow_fs=True` for trusted local lookup files used by `join`, set operations, `union`, or `stack`; pass both `allow_fs=True` and `allow_rules_file=True` for `validate rules_file=...`; pass `workspace_root=...` to pin resolved core plan paths inside a trusted directory. Supplying `spill_dir` opts into local filesystem spill for that host-provided directory. `input_file` and `input_files` are host source adapters and are not controlled by `allow_fs`.

## DuckDB engine

Run pipelines on DuckDB instead of the native C streaming core. The DSL is transpiled to SQL in C, then executed by DuckDB.

```bash
pip install tranfi[duckdb]
```

<!-- readme-test: python-data -->
```python
# Run a pipeline via DuckDB
result = tf.pipeline('csv | filter "age > 25" | sort -age | csv', engine='duckdb')
result.run(input_file='data.csv')

# Or with bytes input
result = tf.pipeline('csv | head 10 | csv', engine='duckdb').run(input=csv_bytes)
```

### SQL transpilation

Generate SQL directly from DSL strings:

<!-- readme-test: python-api -->
```python
sql = tf.compile_to_sql('csv | filter "col(age) > 25" | sort -age | head 10 | csv',
                        dialect='duckdb')
print(sql)
# WITH
#   step_1 AS (SELECT * FROM input_data WHERE ("age" > 25)),
#   step_2 AS (SELECT * FROM step_1 ORDER BY "age" DESC LIMIT 10)
# SELECT * FROM step_2
```

Use this to run queries with your own DuckDB connection. `dialect='sqlite'` and
`dialect='postgres'` are recognized but rejected until those dialects have
separate compatibility tests and SQL-generation rules.

## Advanced

### DSL compilation

<!-- readme-test: python-api -->
```python
# Compile DSL to JSON plan
json_plan = tf.compile_dsl('csv | filter "col(age) > 25" | sort -age | csv')

# Save / load recipes
tf.save_recipe([tf.codec.csv(), tf.ops.head(10), tf.codec.csv_encode()], 'preview.tranfi')
p = tf.load_recipe('preview.tranfi')
result = p.run(input_file='data.csv')
```

### Side channels

Every pipeline produces four output channels:

- **output** -- main pipeline result
- **errors** -- rows that failed processing
- **stats** -- newline-delimited execution statistics: run summary plus per-step counters, state estimates, and warnings
- **samples** -- reserved for sampling operators

<!-- readme-test: python-pipeline -->
```python
result = p.run(input_file='data.csv')
print(result.stats_text)   # newline-delimited JSON: summary plus step_stats/state_bytes_estimate/warnings
```

### Pipeline from JSON

<!-- readme-test: python-api -->
```python
p = tf.pipeline(recipe='{"steps":[{"op":"codec.csv.decode","args":{}},{"op":"head","args":{"n":5}},{"op":"codec.csv.encode","args":{}}]}')
```

## Architecture

Python calls `libtranfi.so` through ctypes. CLI, Node and WASM use the same C11
engine, with typed columns (`bool`, `int64`, `float64`, `string`, `date`,
`timestamp`) and per-cell null bitmaps.

| Need | Python API behavior |
|------|---------------------|
| Stream input | `run(input_file=...)` reads chunks and drains native output after each push |
| Avoid collecting all output | Use `on_output=...` with `collect_output=False`, or `iter_chunks()` |
| Read gzip | `.gz` files use `gzip.open()`; override with `compression='none'` or `'gzip'` |
| Permit a blocking operation | Set `allow_blocking=True` or use an external engine |
| Cap retained key state | Supply caps and check the plan with `memory='64MB'` |
| Allow plan file reads | Supply explicit host-policy options |

Input streaming still collects the final output by default. Choose a streaming
sink for large outputs; row-local and bounded-state operators stream under the
default strict native memory policy.
