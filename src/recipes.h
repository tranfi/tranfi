/*
 * recipes.h — Built-in named recipes.
 *
 * 20 pre-built DSL pipelines for common ETL operations:
 *   tranfi profile    → full data profiling
 *   tranfi preview    → first 10 rows
 *   tranfi csv2json   → format conversion
 *   ...
 */

#ifndef TF_RECIPES_H
#define TF_RECIPES_H

#include <stddef.h>

/* Number of built-in recipes. */
size_t tf_recipe_count(void);

/* Accessors by index (0-based). Return NULL if index out of range. */
const char *tf_recipe_name(size_t index);
const char *tf_recipe_dsl(size_t index);
const char *tf_recipe_description(size_t index);

/* Lookup by name (case-insensitive). Returns DSL string or NULL. */
const char *tf_recipe_find_dsl(const char *name);

#endif /* TF_RECIPES_H */
