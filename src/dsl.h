/*
 * dsl.h — Pipe-style DSL parser (L3 front-end).
 *
 * Parses text like:
 *   csv | filter "col(age) > 25" | select name,age | head 10 | csv
 *
 * Into an IR plan ready for validation and compilation.
 */

#ifndef TF_DSL_H
#define TF_DSL_H

#include "ir.h"

/*
 * Parse a pipe-style DSL string into an IR plan.
 * Positional codec resolution: bare "csv" at start = decode, at end = encode.
 * Returns NULL on error, sets *error (caller frees).
 */
tf_ir_plan *tf_dsl_parse(const char *text, size_t len, char **error);

#endif /* TF_DSL_H */
