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
result = tf.pipeline('csv | filter "col(age) > 25" | sort -age | csv').run(input_file='data.csv')
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

## Quick start

### Two APIs

**Builder API** -- composable, type-safe, IDE-friendly:

```python
p = tf.pipeline([
    tf.codec.csv(),
    tf.ops.filter(tf.expr("col('score') >= 80")),
    tf.ops.derive({'grade': tf.expr("if(col('score')>=90, 'A', 'B')")}),
    tf.ops.sort(['-score']),
    tf.ops.head(10),
    tf.codec.csv_encode(),
])
result = p.run(input_file='students.csv')
```

**DSL strings** -- compact, suitable for CLI-like use:

```python
p = tf.pipeline('csv | filter "col(score) >= 80" | sort -score | head 10 | csv')
result = p.run(input_file='students.csv')
```

Both produce identical pipelines under the hood.

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

## Codecs

Codecs convert between raw bytes and columnar batches. Every pipeline starts with a decoder and ends with an encoder.

| Method | Description |
|--------|-------------|
| `codec.csv(delimiter, header, batch_size, repair)` | CSV decoder. `repair=True` pads short / truncates long rows |
| `codec.csv_encode(delimiter)` | CSV encoder |
| `codec.jsonl(batch_size)` | JSON Lines decoder |
| `codec.jsonl_encode()` | JSON Lines encoder |
| `codec.text(batch_size)` | Line-oriented text decoder (single `_line` column) |
| `codec.text_encode()` | Text encoder |
| `codec.table_encode(max_width, max_rows)` | Pretty-print Markdown table |

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
| `ops.filter(expr)` | Keep rows matching expression |
| `ops.head(n)` | First N rows |
| `ops.tail(n)` | Last N rows |
| `ops.skip(n)` | Skip first N rows |
| `ops.top(n, column, desc=True)` | Top N by column value |
| `ops.sample(n)` | Reservoir sampling (uniform random) |
| `ops.grep(pattern, invert, column, regex)` | Substring/regex filter |
| `ops.validate(expr)` | Add `_valid` boolean column, keep all rows |

### Column operations

| Method | Description |
|--------|-------------|
| `ops.select(columns)` | Keep and reorder columns |
| `ops.rename(**mapping)` | Rename columns: `rename(name='full_name')` |
| `ops.derive(columns)` | Computed columns: `derive({'total': expr("col('a')*col('b')")})` |
| `ops.cast(**mapping)` | Type conversion: `cast(age='int', score='float')` |
| `ops.trim(columns)` | Strip whitespace |
| `ops.fill_null(**mapping)` | Replace nulls: `fill_null(age='0')` |
| `ops.fill_down(columns)` | Forward-fill nulls |
| `ops.clip(column, min, max)` | Clamp numeric values |
| `ops.replace(column, pattern, replacement, regex)` | String find/replace |
| `ops.hash(columns)` | Add `_hash` column (DJB2) |
| `ops.bin(column, boundaries)` | Discretize into bins |

### Sorting and deduplication

| Method | Description |
|--------|-------------|
| `ops.sort(columns)` | Sort rows. Prefix `-` for descending: `sort(['-age', 'name'])` |
| `ops.unique(columns)` | Deduplicate on specified columns |

### Aggregation

| Method | Description |
|--------|-------------|
| `ops.stats(stats_list)` | Column statistics. Stats: `count`, `min`, `max`, `sum`, `avg`, `stddev`, `variance`, `median`, `p25`, `p75`, `p90`, `p99`, `distinct`, `hist`, `sample` |
| `ops.frequency(columns)` | Value counts (descending) |
| `ops.group_agg(group_by, aggs)` | Group by + aggregate |

```python
# Group aggregation
tf.ops.group_agg(['city'], [
    {'column': 'price', 'func': 'sum', 'result': 'total'},
    {'column': 'price', 'func': 'avg', 'result': 'avg_price'},
])
```

### Sequential / window

| Method | Description |
|--------|-------------|
| `ops.step(column, func, result)` | Running aggregation: `running-sum`, `running-avg`, `running-min`, `running-max`, `lag` |
| `ops.window(column, size, func, result)` | Sliding window: `avg`, `sum`, `min`, `max` |
| `ops.lead(column, offset, result)` | Lookahead N rows |

### Reshape

| Method | Description |
|--------|-------------|
| `ops.explode(column, delimiter)` | Split delimited string into rows |
| `ops.split(column, names, delimiter)` | Split column into multiple columns |
| `ops.unpivot(columns)` | Wide to long (melt) |
| `ops.stack(file, tag, tag_value)` | Vertically concatenate another CSV file |

### Date/time

| Method | Description |
|--------|-------------|
| `ops.datetime(column, extract)` | Extract parts: `year`, `month`, `day`, `hour`, `minute`, `second`, `weekday` |
| `ops.date_trunc(column, trunc, result)` | Truncate to: `year`, `month`, `day`, `hour`, `minute`, `second` |

### Other

| Method | Description |
|--------|-------------|
| `ops.flatten()` | Flatten nested columns |
| `ops.reorder(columns)` | Alias for `select` |
| `ops.dedup(columns)` | Alias for `unique` |

## Expressions

Used in `filter`, `derive`, and `validate`. Reference columns with `col('name')`.

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
| Predicates | `starts_with(s,prefix)` `ends_with(s,suffix)` `contains(s,sub)` |
| Conditional | `if(cond,then,else)` `coalesce(a,b,...)` `nullif(a,b)` |
| Math | `abs(x)` `round(x)` `floor(x)` `ceil(x)` `sign(x)` `pow(x,y)` `sqrt(x)` `log(x)` `exp(x)` `mod(a,b)` `greatest(a,b,...)` `least(a,b,...)` |

Aliases: `substr`=`slice`, `length`=`len`, `lpad`=`pad_left`, `rpad`=`pad_right`, `min`=`least`, `max`=`greatest`.

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
- **stats** -- pipeline execution statistics (rows in/out, timing)
- **samples** -- reserved for sampling operators

```python
result = p.run(input_file='data.csv')
print(result.stats_text)   # {"rows_in": 1000, "rows_out": 42, ...}
```

### Pipeline from JSON

```python
p = tf.pipeline(recipe='{"steps":[{"op":"codec.csv.decode","args":{}},{"op":"head","args":{"n":5}},{"op":"codec.csv.encode","args":{}}]}')
```

## Architecture

The Python package is a thin ctypes wrapper around `libtranfi.so`, the same C11 core used by the CLI, Node.js, and WASM targets. Data flows through columnar batches with typed columns (`bool`, `int64`, `float64`, `string`, `date`, `timestamp`) and per-cell null bitmaps. All operators are streaming with bounded memory, except those that require full input (sort, unique, stats, tail, top, group-agg, frequency, pivot).
