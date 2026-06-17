#!/usr/bin/env bash
set -euo pipefail

bin=${1:?usage: test_cli_memory_policy.sh /path/to/tranfi}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

input="$tmp/input.csv"
cat > "$input" <<'CSV'
name,age
Bob,30
Alice,20
CSV

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# Native CLI is strict by default: blocking sort must not run silently.
if "$bin" 'csv | sort age | csv' < "$input" > "$tmp/out" 2> "$tmp/err"; then
  fail "blocking sort succeeded without --allow-blocking"
fi
grep -q "blocking step 'sort'" "$tmp/err" || fail "missing blocking sort error"

# Known-small blocking plans can still run when explicitly allowed.
"$bin" --allow-blocking 'csv | sort age | csv' < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Alice,20' "$tmp/out" || fail "allow-blocking sort did not emit sorted Alice row"
grep -q '^Bob,30' "$tmp/out" || fail "allow-blocking sort did not emit Bob row"

# Bounded plans accept a memory policy and report it in explain output.
"$bin" --explain --memory=max:64MB 'csv | head 1 | csv' > "$tmp/explain" 2> "$tmp/err"
grep -q '^normalized_dsl: csv | head n=1 | csv$' "$tmp/explain" || fail "explain missing normalized DSL"
grep -q '^execution_target: native$' "$tmp/explain" || fail "explain missing native target"
grep -q 'target=native memory=bounded_state' "$tmp/explain" || fail "explain missing native step target"
grep -q '^memory_policy: strict$' "$tmp/explain" || fail "explain missing strict policy"
grep -q '^memory_limit: 64.0MB$' "$tmp/explain" || fail "explain missing parsed memory limit"
grep -q '^ir_json: ' "$tmp/explain" || fail "explain missing serialized IR"
grep -q '"op":"head"' "$tmp/explain" || fail "explain IR missing head op"
grep -q '"memory_class":"bounded_state"' "$tmp/explain" || fail "explain IR missing memory metadata"
"$bin" --memory max:64MB 'csv | head 1 | csv' < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Bob,30' "$tmp/out" || fail "bounded head failed under memory policy"

# Explain shows canonical parser rewrites in the normalized DSL line.
"$bin" --explain 'csv | sort -age | head 1 | csv' > "$tmp/explain" 2> "$tmp/err"
grep -q '^normalized_dsl: csv | top n=1 column=age desc=true | csv$' "$tmp/explain" || fail "explain missing normalized sort/head rewrite"

# Stats side-channel can be written to a machine-readable file without mixing into stderr.
"$bin" --stats-json "$tmp/stats.ndjson" 'csv | filter "col(age) > 20" | csv' < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Bob,30' "$tmp/out" || fail "stats-json run missed filtered output"
grep -q '"type":"step_stats"' "$tmp/stats.ndjson" || fail "stats-json file missing step stats"
grep -q '"op":"filter"' "$tmp/stats.ndjson" || fail "stats-json file missing filter op"
grep -q '"rows_out":1' "$tmp/stats.ndjson" || fail "stats-json file missing filtered row count"
if grep -q '"type":"step_stats"' "$tmp/err"; then
  fail "stats-json file mode also wrote stats to stderr"
fi
"$bin" -q --stats-json "$tmp/stats-quiet.ndjson" 'csv | head 1 | csv' < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '"type":"step_stats"' "$tmp/stats-quiet.ndjson" || fail "stats-json should work with -q"

# Blocking native plans still cannot claim a byte memory cap.
if "$bin" --allow-blocking --memory max:64MB 'csv | sort age | csv' < "$input" > "$tmp/out" 2> "$tmp/err"; then
  fail "blocking sort accepted unenforced memory cap"
fi
grep -q "native byte caps are not implemented for blocking step 'sort'" "$tmp/err" || fail "missing blocking byte-cap error"

# Uncapped key-state plans still fail under --memory.
if "$bin" --memory max:64MB 'csv | unique name | csv' < "$input" > "$tmp/out" 2> "$tmp/err"; then
  fail "uncapped key-state unique accepted memory cap"
fi
grep -q "needs max_keys" "$tmp/err" || fail "missing uncapped key-state cap error"

# Sorted group aggregation has bounded previous-group state and does not need max_groups.
"$bin" --memory max:64KB 'csv batch_size=1 | group-agg name sum:age:total sorted=true | csv' < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Bob,30' "$tmp/out" || fail "sorted group-agg failed under memory policy"
grep -q '^Alice,20' "$tmp/out" || fail "sorted group-agg missed final group"
if "$bin" --memory max:64KB 'csv | group-agg name sum:age:total | csv' < "$input" > "$tmp/out" 2> "$tmp/err"; then
  fail "uncapped unsorted group-agg accepted memory cap"
fi
grep -q "needs max_groups, max_state_bytes, or sorted=true" "$tmp/err" || fail "missing uncapped group-agg cap error"

pivot_input="$tmp/pivot.csv"
cat > "$pivot_input" <<'CSV'
name,metric,value
A,x,1
A,y,2
B,x,3
CSV
"$bin" --explain --memory=max:64KB 'csv batch_size=1 | pivot metric value sum categories=x,y sorted=true | csv' > "$tmp/explain" 2> "$tmp/err"
grep -q '"op":"pivot"' "$tmp/explain" || fail "explain missing pivot op"
grep -q '"memory_class":"bounded_state"' "$tmp/explain" || fail "explain missing sorted pivot bounded metadata"
"$bin" --memory max:64KB 'csv batch_size=1 | pivot metric value sum categories=x,y sorted=true | csv' < "$pivot_input" > "$tmp/out" 2> "$tmp/err"
grep -q '^A,1,2' "$tmp/out" || fail "sorted pivot failed under memory policy"
grep -q '^B,3,' "$tmp/out" || fail "sorted pivot missed final group"
if "$bin" --memory max:64KB 'csv | pivot metric value sum | csv' < "$pivot_input" > "$tmp/out" 2> "$tmp/err"; then
  fail "unguarded pivot accepted memory cap"
fi
grep -q "blocking step 'pivot'" "$tmp/err" || fail "missing unguarded pivot blocking error"

# Capped key-state plans get conservative byte estimates and enforce the budget.
"$bin" --explain --memory=max:64KB 'csv | unique name max_keys=2 | csv' > "$tmp/explain" 2> "$tmp/err"
grep -q '^state_bytes_estimate:' "$tmp/explain" || fail "explain missing key-state byte estimate"
grep -q '"state_bytes_estimate":1600' "$tmp/explain" || fail "explain IR missing key-state byte estimate"
"$bin" --memory max:64KB 'csv | unique name max_keys=2 | csv' < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Bob,30' "$tmp/out" || fail "capped unique failed under sufficient memory policy"
if "$bin" --memory max:1KB 'csv | unique name max_keys=2 | csv' < "$input" > "$tmp/out" 2> "$tmp/err"; then
  fail "capped unique exceeded memory policy but succeeded"
fi
grep -q "estimated native key-state memory" "$tmp/err" || fail "missing key-state estimate overflow error"

"$bin" --memory max:64KB 'csv | onehot name categories=Bob,Alice unknown=null | csv' < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q 'name_Bob' "$tmp/out" || fail "declared-category onehot failed under memory policy"

lookup="$tmp/lookup.csv"
cat > "$lookup" <<'CSV'
name,city
Bob,NY
Alice,LA
CSV
"$bin" --memory max:16KB "csv | join $lookup on=name max_lookup_bytes=1024 | csv" < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Bob,30,NY' "$tmp/out" || fail "join with lookup byte cap failed under memory policy"
if "$bin" --memory max:1KB "csv | join $lookup on=name max_lookup_bytes=1024 | csv" < "$input" > "$tmp/out" 2> "$tmp/err"; then
  fail "join byte estimate exceeded memory policy but succeeded"
fi
grep -q "estimated native key-state memory" "$tmp/err" || fail "missing join estimate overflow error"

mkdir -p "$tmp/spill"
"$bin" --spill-dir "$tmp/spill" 'csv | sort age | csv' < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Alice,20' "$tmp/out" || fail "spill sort did not emit sorted Alice row"
grep -q '^Bob,30' "$tmp/out" || fail "spill sort did not emit Bob row"
if find "$tmp/spill" -type f | grep -q .; then
  fail "spill sort left temporary files behind"
fi
"$bin" --memory max:1KB --spill-dir "$tmp/spill" 'csv | sort age | csv' < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Alice,20' "$tmp/out" || fail "memory-capped spill sort failed"
"$bin" --explain --spill-dir "$tmp/spill" 'csv | sort age | csv' > "$tmp/explain" 2> "$tmp/err"
grep -q '^execution_target: native+spill$' "$tmp/explain" || fail "explain missing native+spill target"
grep -q 'target=native_spill memory=blocking' "$tmp/explain" || fail "explain missing native spill step target"
grep -q '^memory_policy: spill$' "$tmp/explain" || fail "explain missing spill policy"
grep -q '"spill_dir"' "$tmp/explain" || fail "explain IR missing injected spill_dir"

cat > "$tmp/dups.csv" <<'CSV'
id,name,score
3,C,30
1,A,10
2,B,20
1,A2,11
3,C2,31
4,D,40
CSV
"$bin" --memory max:1KB --spill-dir "$tmp/spill" 'csv batch_size=1 | unique id | csv' < "$tmp/dups.csv" > "$tmp/out" 2> "$tmp/err"
grep -q '^3,C,30' "$tmp/out" || fail "spill unique missed first id=3 row"
grep -q '^1,A,10' "$tmp/out" || fail "spill unique missed first id=1 row"
grep -q '^2,B,20' "$tmp/out" || fail "spill unique missed id=2 row"
grep -q '^4,D,40' "$tmp/out" || fail "spill unique missed id=4 row"
if grep -q 'A2\|C2' "$tmp/out"; then
  fail "spill unique emitted duplicate-key rows"
fi
if find "$tmp/spill" -type f | grep -q .; then
  fail "spill unique left temporary files behind"
fi
"$bin" --explain --spill-dir "$tmp/spill" 'csv | unique id | csv' > "$tmp/explain" 2> "$tmp/err"
grep -q 'target=native_spill memory=external emit=on_flush' "$tmp/explain" || fail "explain missing native spill unique metadata"
grep -q '"memory_class":"external"' "$tmp/explain" || fail "explain IR missing external unique metadata"

cat > "$tmp/groups.csv" <<'CSV'
city,sales
B,10
A,1
C,5
A,2
B,3
D,7
CSV
"$bin" --memory max:1KB --spill-dir "$tmp/spill" 'csv batch_size=1 | group-agg city sum:sales:total count:sales:n count:*:rows | csv' < "$tmp/groups.csv" > "$tmp/out" 2> "$tmp/err"
grep -q '^B,13,2,2' "$tmp/out" || fail "spill group-agg missed B aggregate"
grep -q '^A,3,2,2' "$tmp/out" || fail "spill group-agg missed A aggregate"
grep -q '^C,5,1,1' "$tmp/out" || fail "spill group-agg missed C aggregate"
grep -q '^D,7,1,1' "$tmp/out" || fail "spill group-agg missed D aggregate"
order=$(grep -E '^[ABCD],' "$tmp/out" | cut -d, -f1 | tr -d '\n')
[ "$order" = "BACD" ] || fail "spill group-agg changed first-seen group order: $order"
if find "$tmp/spill" -type f | grep -q .; then
  fail "spill group-agg left temporary files behind"
fi
"$bin" --explain --spill-dir "$tmp/spill" 'csv | group-agg city sum:sales:total | csv' > "$tmp/explain" 2> "$tmp/err"
grep -q 'target=native_spill memory=external emit=on_flush' "$tmp/explain" || fail "explain missing native spill group-agg metadata"
grep -q '"memory_class":"external"' "$tmp/explain" || fail "explain IR missing external group-agg metadata"

filter_lookup="$tmp/filter_lookup.csv"
cat > "$filter_lookup" <<'CSV'
name,label
Bob,keep
CSV
"$bin" --memory max:1KB --spill-dir "$tmp/spill" "csv batch_size=1 | semi-join $filter_lookup on=name | csv" < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Bob,30' "$tmp/out" || fail "spill semi-join missed matching row"
if grep -q '^Alice,20' "$tmp/out"; then
  fail "spill semi-join emitted non-matching row"
fi
"$bin" --memory max:1KB --spill-dir "$tmp/spill" "csv batch_size=1 | anti-join $filter_lookup on=name | csv" < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Alice,20' "$tmp/out" || fail "spill anti-join missed non-matching row"
if grep -q '^Bob,30' "$tmp/out"; then
  fail "spill anti-join emitted matching row"
fi
if find "$tmp/spill" -type f | grep -q .; then
  fail "spill filtering join left temporary files behind"
fi
"$bin" --explain --spill-dir "$tmp/spill" "csv | semi-join $filter_lookup on=name | csv" > "$tmp/explain" 2> "$tmp/err"
grep -q 'target=native_spill memory=external emit=on_flush' "$tmp/explain" || fail "explain missing native spill filtering join metadata"
grep -q '"op":"semi-join"' "$tmp/explain" || fail "explain IR missing semi-join op"
grep -q '"memory_class":"external"' "$tmp/explain" || fail "explain IR missing external semi-join metadata"

"$bin" --memory max:1KB --spill-dir "$tmp/spill" "csv batch_size=1 | intersect $filter_lookup name | csv" < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Bob,30' "$tmp/out" || fail "spill intersect missed matching row"
if grep -q '^Alice,20' "$tmp/out"; then
  fail "spill intersect emitted non-matching row"
fi
"$bin" --memory max:1KB --spill-dir "$tmp/spill" "csv batch_size=1 | setdiff $filter_lookup name | csv" < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Alice,20' "$tmp/out" || fail "spill setdiff missed non-matching row"
if grep -q '^Bob,30' "$tmp/out"; then
  fail "spill setdiff emitted matching row"
fi
if find "$tmp/spill" -type f | grep -q .; then
  fail "spill set ops left temporary files behind"
fi
"$bin" --explain --spill-dir "$tmp/spill" "csv | intersect $filter_lookup name | csv" > "$tmp/explain" 2> "$tmp/err"
grep -q 'target=native_spill memory=external emit=on_flush' "$tmp/explain" || fail "explain missing native spill set-op metadata"
grep -q '"op":"intersect"' "$tmp/explain" || fail "explain IR missing intersect op"
grep -q '"memory_class":"external"' "$tmp/explain" || fail "explain IR missing external intersect metadata"
union_lookup="$tmp/union_lookup.csv"
cat > "$union_lookup" <<'CSV'
name,age
Bob,300
Cara,40
CSV
"$bin" --memory max:1KB --spill-dir "$tmp/spill" "csv batch_size=1 | union $union_lookup name | csv" < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Bob,30' "$tmp/out" || fail "spill union missed left Bob row"
grep -q '^Alice,20' "$tmp/out" || fail "spill union missed left Alice row"
grep -q '^Cara,40' "$tmp/out" || fail "spill union missed file-only Cara row"
if grep -q '^Bob,300' "$tmp/out"; then
  fail "spill union emitted duplicate file Bob row"
fi
if find "$tmp/spill" -type f | grep -q .; then
  fail "spill union left temporary files behind"
fi
"$bin" --explain --spill-dir "$tmp/spill" "csv | union $union_lookup name | csv" > "$tmp/explain" 2> "$tmp/err"
grep -q 'target=native_spill memory=external emit=on_flush' "$tmp/explain" || fail "explain missing native spill union metadata"
grep -q '"op":"union"' "$tmp/explain" || fail "explain IR missing union op"
grep -q '"memory_class":"external"' "$tmp/explain" || fail "explain IR missing external union metadata"
if "$bin" --spill-dir "$tmp/spill" "csv | join $lookup on=name | csv" < "$input" > "$tmp/out" 2> "$tmp/err"; then
  fail "mutating join accepted native spill without max_matches_per_row"
fi
grep -q "max_matches_per_row" "$tmp/err" || fail "missing uncapped spill mutating join error"
"$bin" --memory max:1KB --spill-dir "$tmp/spill" "csv batch_size=1 | join $lookup on=name max_matches_per_row=1 | csv" < "$input" > "$tmp/out" 2> "$tmp/err"
grep -q '^Bob,30,NY' "$tmp/out" || fail "spill mutating join missed Bob row"
grep -q '^Alice,20,LA' "$tmp/out" || fail "spill mutating join missed Alice row"
if find "$tmp/spill" -type f | grep -q .; then
  fail "spill mutating join left temporary files behind"
fi
"$bin" --explain --spill-dir "$tmp/spill" "csv | join $lookup on=name max_matches_per_row=1 | csv" > "$tmp/explain" 2> "$tmp/err"
grep -q 'target=native_spill memory=external emit=on_flush' "$tmp/explain" || fail "explain missing native spill mutating join metadata"
grep -q 'schema=data_dependent' "$tmp/explain" || fail "explain missing mutating join data-dependent schema"

if "$bin" --spill-dir "$tmp/spill" 'csv | pivot name age | csv' < "$input" > "$tmp/out" 2> "$tmp/err"; then
  fail "uncapped spill pivot succeeded"
fi
grep -q "native spill is not implemented yet for blocking step 'pivot'" "$tmp/err" || fail "missing uncapped spill pivot error"
cat > "$tmp/pivot.csv" <<'CSV'
id,metric,value
B,y,4
A,x,1
B,x,3
A,y,2
A,x,5
CSV
"$bin" --memory max:1KB --spill-dir "$tmp/spill" 'csv batch_size=1 | pivot metric value sum max_categories=2 | csv' < "$tmp/pivot.csv" > "$tmp/out" 2> "$tmp/err"
grep -q '^id,y,x' "$tmp/out" || fail "spill pivot did not keep dynamic category order"
grep -q '^B,4,3' "$tmp/out" || fail "spill pivot missed B row"
grep -q '^A,2,6' "$tmp/out" || fail "spill pivot missed A aggregate"
if find "$tmp/spill" -type f | grep -q .; then
  fail "spill pivot left temporary files behind"
fi
"$bin" --explain --spill-dir "$tmp/spill" 'csv | pivot metric value sum max_categories=2 | csv' > "$tmp/explain" 2> "$tmp/err"
grep -q 'target=native_spill memory=external emit=on_flush' "$tmp/explain" || fail "explain missing native spill pivot metadata"
grep -q 'schema=data_dependent' "$tmp/explain" || fail "explain missing pivot data-dependent schema"
grep -q '"op":"pivot"' "$tmp/explain" || fail "explain IR missing pivot op"
grep -q '"memory_class":"external"' "$tmp/explain" || fail "explain IR missing external pivot metadata"

"$bin" --target sql --dialect duckdb 'csv | filter "col(age) > 25" | sort -age | head 10 | csv' > "$tmp/sql" 2> "$tmp/err"
grep -q '^WITH' "$tmp/sql" || fail "SQL target did not emit a CTE query"
grep -q '"age" > 25' "$tmp/sql" || fail "SQL target missing filter predicate"
grep -q 'ORDER BY "age" DESC' "$tmp/sql" || fail "SQL target missing descending sort"
grep -q 'LIMIT 10' "$tmp/sql" || fail "SQL target missing limit"
"$bin" --target=sql 'csv | head 1 | csv' > "$tmp/sql" 2> "$tmp/err"
grep -q 'LIMIT 1' "$tmp/sql" || fail "SQL target default dialect missing head limit"
"$bin" --target json 'csv | head 1 | csv' > "$tmp/ir.json" 2> "$tmp/err"
grep -q '"op":"head"' "$tmp/ir.json" || fail "--target json did not emit IR JSON"
"$bin" --explain --target sql 'csv | sort age | csv' > "$tmp/explain" 2> "$tmp/err"
grep -q '^execution_target: duckdb$' "$tmp/explain" || fail "--target sql explain missing duckdb target"
grep -q 'target=duckdb_sql memory=blocking' "$tmp/explain" || fail "--target sql explain missing SQL step target"
if "$bin" --target sql --dialect sqlite 'csv | head 1 | csv' > "$tmp/sql" 2> "$tmp/err"; then
  fail "sqlite SQL target succeeded before dialect lowering exists"
fi
grep -q "only duckdb lowering is available" "$tmp/err" || fail "sqlite dialect rejection did not explain current support"
if "$bin" --target sql --dialect mysql 'csv | head 1 | csv' > "$tmp/sql" 2> "$tmp/err"; then
  fail "unknown SQL dialect succeeded"
fi
grep -q "unknown SQL dialect" "$tmp/err" || fail "unknown dialect rejection missing diagnostic"

"$bin" --explain --engine duckdb 'csv | sort age | csv' > "$tmp/explain" 2> "$tmp/err"
grep -q '^execution_target: duckdb$' "$tmp/explain" || fail "explain missing duckdb target"
grep -q 'target=duckdb_sql memory=blocking' "$tmp/explain" || fail "explain missing duckdb SQL step target"
if "$bin" --engine duckdb 'csv | sort age | csv' < "$input" > "$tmp/out" 2> "$tmp/err"; then
  fail "C CLI duckdb execution succeeded unexpectedly"
fi
grep -q "C CLI cannot execute --engine duckdb" "$tmp/err" || fail "missing duckdb engine diagnostic"

if "$bin" --allow-blocking --fail-on-blocking 'csv | head 1 | csv' < "$input" > "$tmp/out" 2> "$tmp/err"; then
  fail "conflicting blocking flags succeeded"
fi
grep -q "conflicts" "$tmp/err" || fail "missing conflicting flags diagnostic"

echo "CLI memory policy tests passed"
