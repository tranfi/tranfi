# tranfi (Python)

Streaming ETL in Python, powered by a native C11 core. Process CSV, JSONL, and text data with composable pipelines that run in constant memory, no matter how large the input.

```python
import tranfi as tf

result = tf.pipeline([
    tf.codec.csv(),
    tf.ops.filter(tf.expr("col('age') > 25")),
    tf.ops.sort(['-age']),
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

```python
result = tf.pipeline('csv | filter "col(age) > 25" | top-k 100 age | csv').run(input_file='data.csv')
```

## Install

```bash
pip install tranfi
```

Or from source:

```bash
cd build && cmake .. && make
pip install -e py/
```

## CLI

Installing the package also installs the `tranfi` command:

```bash
# Filter and sort
tranfi 'csv | filter "age > 25" | top-k 100 age | csv' < data.csv

# Built-in recipe
tranfi profile < data.csv

# File I/O
tranfi -i input.csv -o output.csv 'csv | select name,age | csv'

# List recipes
tranfi -R
```

Run `tranfi -h` for all options.

## Quick start

### Two APIs

**Builder API** -- composable, type-safe, IDE-friendly:

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

```python
p = tf.pipeline('csv | filter "col(score) >= 80" | top-k 10 score | csv')
result = p.run(input_file='students.csv')
```

Both produce identical pipelines under the hood.

Dataframe-style DSL aliases are accepted and normalize to canonical ops: `mutate` -> `derive`, `summarise`/`summarize` -> `group-agg`, `distinct` -> `unique`, and `arrange` -> `sort`.

### Running pipelines

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

`source_column` appends the path for each input row without preloading file contents. Tranfi flushes decoder input at each file boundary so an unterminated final record belongs to the correct source file. It does not remove repeated CSV headers from later files; use shards without repeated headers, `header=False`, or a pre-cleaning step when every file has its own header.

### Prepared reusable transforms

Prepared transforms are separate from byte-stream pipelines. They are for typed operations whose parameters must be learned from reference data: an analyzer consumes one or more bounded-memory batches, `finalize()` freezes an immutable plan and output schema, and an apply session runs that plan over either a second pass of the original data (`fit_transform`-style) or later compatible batches.

The current slice accepts declared `float32`/`float64` columns and implements numeric none/zero/constant/mean/exact-median imputation plus none/standard/min-max normalization. It also supports the first declared categorical profile: mode imputation with `allMissing` set to `error` or `zero`, discovered finite typed categories, and pass-through `encode.op = 'none'`. Mode ties choose the smallest category; missing values use the frozen mode; a later finite category outside the learned dictionary fails with code `108`. Exact median obeys the configured allocation and resident-state limits; categorical discovery also obeys the session category limits. Limit failures use resource code `104`, and neither operation spills without host authority. Label/one-hot encoding, fixed dictionaries, other unknown policies, and kind inference are not implemented yet.

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
```

Prepared-transform failures raise `TranfiTransformError`; its numeric `code` is stable across the C and Python APIs. `TransformLimits` starts from the C-owned safe profile and applies explicit overrides.
For cooperative cancellation, create a `TransformCancelToken`, pass it to `recipe.analyzer(..., cancel_token=token)`, `plan.apply(..., cancel_token=token)`, or `TransformPlan.from_bytes(..., cancel_token=token)`, and call `token.request()` from another Python thread. Active analyzer/apply sessions retain the token for their lifetime.

## Codecs

Codecs convert between raw bytes and columnar batches. Every pipeline starts with a decoder and ends with an encoder.

| Method | Description |
|--------|-------------|
| `codec.csv(delimiter, header, batch_size, repair, mode, strict, max_error_bytes, max_record_bytes, max_columns, nulls, quoted_nulls, skip, n_max, max_rows, comment, trim_ws, skip_empty_rows, audit, audit_limit, audit_include_row, audit_columns, audit_redact, audit_hash_columns, audit_max_bytes, audit_max_cell_bytes)` | CSV decoder. `mode='strict'` fails on field-count mismatches; `repair=True` / `mode='repair'` emits repair diagnostics; `max_record_bytes` bounds buffered records; `nulls=['NA']` adds null sentinels; `skip=2`, `n_max=100`, `comment='#'`, `trim_ws=False`, and `skip_empty_rows=True` control row-local parsing; repair audit/raw diagnostics support privacy controls with pseudo-column `raw` |
| `codec.csv_encode(delimiter)` | CSV encoder |
| `codec.jsonl(batch_size, on_error, max_error_bytes, max_record_bytes)` | JSON Lines decoder. `on_error` is `skip`, `fail`, `warn`, or `quarantine` |
| `codec.jsonl_encode()` | JSON Lines encoder |
| `codec.text(batch_size, max_error_bytes, max_record_bytes)` | Line-oriented text decoder (single `_line` column) |
| `codec.text_encode()` | Text encoder |
| `codec.table_encode(max_width, max_rows)` | Pretty-print Markdown table |

CSV treats unquoted empty fields as null by default. Pass `nulls=['NA', 'NULL']` to add sentinel strings; pass `quoted_nulls=False` when quoted sentinels like `"NA"` or `""` should remain strings. Default field-count handling is permissive for compatibility. Use `mode='strict'` or `strict=True` to fail on row/header width mismatches. Use `repair=True` or `mode='repair'` to pad/truncate and collect JSONL diagnostics in `result.errors`; `max_error_bytes` bounds raw previews, and `audit_include_row`, `audit_columns`, `audit_redact`, `audit_hash_columns`, `audit_max_bytes`, and `audit_max_cell_bytes` govern repair audit/raw payloads with the raw preview exposed as pseudo-column `raw`. `max_record_bytes` defaults to `67108864` bytes, caps the current record buffer before a newline is seen, and raises with a `csv_record_too_large` diagnostic when exceeded; pass `0` to disable the guard. `max_columns` defaults to `8192`; records above the cap raise with a bounded `csv_too_many_columns` diagnostic instead of silently dropping columns. Decoder size options are checked before execution: `batch_size` must be `1..65536`, `max_error_bytes` must be `0..67108864`, `max_record_bytes` must be `0..1073741824`, and `max_columns` must be `1..65536`. `skip=2` discards preamble records before header/schema discovery; comments are applied after skipped rows. `n_max=100` / `max_rows=100` keeps at most that many decoded data rows after skip/comment/header handling and uses only one counter; `n_max=0` preserves a header-only schema batch. `comment='#'` removes text after an unquoted marker and skips comment-only rows; quoted markers are preserved. Unquoted spaces/tabs are trimmed by default; pass `trim_ws=False` to preserve them. Blank physical rows after the header are preserved as all-null rows by default; pass `skip_empty_rows=True` to drop them.

Malformed JSONL records are skipped by default. Set `on_error='warn'` or `on_error='quarantine'` to keep valid rows and collect JSONL diagnostics in `result.errors`; set `on_error='fail'` to raise on the first malformed line. `max_error_bytes` bounds the raw preview stored in diagnostics, and `max_record_bytes` applies the same current-line guard as CSV/text.

Cross-codec pipelines work naturally:

```python
# CSV in, JSONL out
tf.pipeline([tf.codec.csv(), tf.ops.head(5), tf.codec.jsonl_encode()])

# JSONL in, CSV out
tf.pipeline([tf.codec.jsonl(), tf.ops.sort(['name']), tf.codec.csv_encode()])
```

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

```python
tf.ops.derive({
    'year': tf.expr("year(col('date'))"),
    'month_start': tf.expr("date_trunc(col('date'), 'month')"),
})
```

## Recipes

Built-in named pipelines for common tasks. Use by name:

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

```python
for r in tf.recipes():
    print(f"{r['name']:15} {r['description']}")
```


## Memory policy

Native execution rejects full-input blocking steps such as `sort`, `pivot`, `normalize`, `acf`, and table encoding unless you opt in for known-small data:

```python
tf.pipeline('csv | sort age | csv').run(input_file='small.csv', allow_blocking=True)
```

For capped key-state operators, pass `memory` to validate the conservative native state estimate before execution:

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

```python
# Run a pipeline via DuckDB
result = tf.pipeline('csv | filter "age > 25" | sort -age | csv', engine='duckdb')
result.run(input_file='data.csv')

# Or with bytes input
result = tf.pipeline('csv | head 10 | csv', engine='duckdb').run(input=csv_bytes)
```

### SQL transpilation

Generate SQL directly from DSL strings:

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

```python
result = p.run(input_file='data.csv')
print(result.stats_text)   # newline-delimited JSON: summary plus step_stats/state_bytes_estimate/warnings
```

### Pipeline from JSON

```python
p = tf.pipeline(recipe='{"steps":[{"op":"codec.csv.decode","args":{}},{"op":"head","args":{"n":5}},{"op":"codec.csv.encode","args":{}}]}')
```

## Architecture

The Python package is a thin ctypes wrapper around `libtranfi.so`, the same C11 core used by the CLI, Node.js, and WASM targets. Data flows through columnar batches with typed columns (`bool`, `int64`, `float64`, `string`, `date`, `timestamp`) and per-cell null bitmaps. `run(input_file=...)` streams input in chunks and drains native main output after each push; by default it still collects the final output for convenience. `.gz` input files are decompressed in the host source adapter with `gzip.open()`; use `compression='none'` to force raw bytes or `compression='gzip'` to force gzip. Use `on_output=...` with `collect_output=False` or `iter_chunks()` for large outputs. Native execution is strict by default: row-local and bounded-state operators stream, blocking operators require `allow_blocking=True` or an external engine, capped key-state plans can be checked with `memory='64MB'`, and core plan file reads require explicit host-policy options.
