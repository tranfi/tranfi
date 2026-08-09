# Prepared-transform V1 vectors

These JSON files are the language-neutral contract corpus for Tranfi's generic
prepared-transform API.

- `prepared_transform_v1.json` defines exact recipes, logical float64 matrices,
  chunk splits, expected fitted state/output, error boundaries, and correctly
  rounded square-root cases.
- `tftr_malformed_v1.json` defines one canonical TFTR plan and named mutations for
  envelope, hash, canonical-JSON, semantic-state, resource, and runtime failures.

Float cells are big-endian IEEE-754 binary64 hex strings; JSON `null` is a missing
input cell. TFTR bytes are lowercase hex so no binary fixture is required.

Run `make test-vectors`. `test/validate_transform_vectors.py` uses only the Python
standard library, verifies exact hashes/envelope bytes, proves square-root rounding
against rational midpoint intervals, and checks that every named malformed mutation
has the frozen error code. C, Node/WASM, and Python binding tests must consume these
same files rather than copying their cases.
