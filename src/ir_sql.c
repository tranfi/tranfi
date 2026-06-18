/*
 * ir_sql.c — IR plan to SQL transpiler.
 *
 * Converts a validated IR plan to a DuckDB-compatible SQL query.
 * Each transform step becomes a CTE in a WITH chain.
 * Expressions are translated from tranfi syntax to SQL syntax.
 */

#include "internal.h"
#include "expr.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <ctype.h>

/* ---- Dynamic string builder ---- */

typedef struct {
  char  *data;
  size_t len;
  size_t cap;
  int    failed;
} strbuf;

static void sb_free(strbuf *sb);

static void sb_init(strbuf *sb) {
  sb->data = tf_mallocarray_checked(256, sizeof(char));
  sb->len = 0;
  sb->cap = 256;
  sb->failed = 0;
  if (sb->data) sb->data[0] = '\0';
  else { sb->cap = 0; sb->failed = 1; }
}

static int sb_ensure(strbuf *sb, size_t extra) {
  if (!sb || sb->failed) return TF_ERROR;
  size_t need = 0;
  if (tf_size_add(sb->len, extra, &need) != TF_OK ||
      tf_size_add(need, 1, &need) != TF_OK) {
    sb->failed = 1;
    return TF_ERROR;
  }
  if (need <= sb->cap) return TF_OK;
  size_t newcap = 0;
  if (tf_size_grow_pow2(sb->cap, need, 256, &newcap) != TF_OK) {
    sb->failed = 1;
    return TF_ERROR;
  }
  char *nd = tf_reallocarray_checked(sb->data, newcap, sizeof(char));
  if (!nd) {
    sb->failed = 1;
    return TF_ERROR;
  }
  sb->data = nd;
  sb->cap = newcap;
  return TF_OK;
}

static void sb_append(strbuf *sb, const char *s) {
  if (!sb || sb->failed || !s) return;
  size_t n = strlen(s);
  if (sb_ensure(sb, n) != TF_OK) return;
  memcpy(sb->data + sb->len, s, n);
  sb->len += n;
  sb->data[sb->len] = '\0';
}

static void sb_appendn(strbuf *sb, const char *s, size_t n) {
  if (!sb || sb->failed || (!s && n > 0)) {
    if (sb) sb->failed = 1;
    return;
  }
  if (sb_ensure(sb, n) != TF_OK) return;
  if (n == 0) return;
  memcpy(sb->data + sb->len, s, n);
  sb->len += n;
  sb->data[sb->len] = '\0';
}

static void sb_appendf(strbuf *sb, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

static void sb_appendf(strbuf *sb, const char *fmt, ...) {
  if (!sb || sb->failed || !fmt) return;
  char buf[512];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  if (n == 0) return;
  if (n < 0) {
    sb->failed = 1;
    return;
  }
  if ((size_t)n < sizeof(buf)) {
    sb_appendn(sb, buf, (size_t)n);
    return;
  }
  size_t dyn_size = 0;
  if (tf_size_add((size_t)n, 1, &dyn_size) != TF_OK) {
    sb->failed = 1;
    return;
  }
  char *dyn = tf_mallocarray_checked(dyn_size, sizeof(char));
  if (!dyn) {
    sb->failed = 1;
    return;
  }
  va_start(ap, fmt);
  int n2 = vsnprintf(dyn, dyn_size, fmt, ap);
  va_end(ap);
  if (n2 < 0 || (size_t)n2 >= dyn_size) {
    free(dyn);
    sb->failed = 1;
    return;
  }
  sb_appendn(sb, dyn, (size_t)n2);
  free(dyn);
}

static char *sb_detach(strbuf *sb) {
  if (!sb || sb->failed) {
    if (sb) sb_free(sb);
    return NULL;
  }
  char *s = sb->data;
  sb->data = NULL;
  sb->len = sb->cap = 0;
  sb->failed = 0;
  return s;
}

static void sb_free(strbuf *sb) {
  if (!sb) return;
  free(sb->data);
  sb->data = NULL;
  sb->len = sb->cap = 0;
  sb->failed = 0;
}

/* ---- Expression AST to SQL ---- */

static void sql_quote_ident_part(strbuf *sb, const char *name) {
  const char *safe = name ? name : "";
  for (const char *p = safe; *p; p++) {
    if (*p == '"') sb_append(sb, "\"\"");
    else sb_appendn(sb, p, 1);
  }
}

/* Append a SQL-quoted identifier: "name" */
static void sql_quote_ident(strbuf *sb, const char *name) {
  sb_append(sb, "\"");
  sql_quote_ident_part(sb, name);
  sb_append(sb, "\"");
}

static void sql_quote_joined_ident(strbuf *sb, const char *prefix,
                                   const char *middle, const char *suffix) {
  sb_append(sb, "\"");
  sql_quote_ident_part(sb, prefix);
  sql_quote_ident_part(sb, middle);
  sql_quote_ident_part(sb, suffix);
  sb_append(sb, "\"");
}

/* Append a SQL string literal: 'value' */
static void sql_quote_str(strbuf *sb, const char *s) {
  sb_append(sb, "'");
  for (const char *p = s; *p; p++) {
    if (*p == '\'') sb_append(sb, "''");
    else sb_appendn(sb, p, 1);
  }
  sb_append(sb, "'");
}

/* Function name mapping: tranfi → SQL */
static const char *map_func_name(const char *name) {
  if (strcmp(name, "len") == 0) return "length";
  if (strcmp(name, "pad_left") == 0) return "lpad";
  if (strcmp(name, "pad_right") == 0) return "rpad";
  if (strcmp(name, "mod") == 0) return NULL; /* special: a % b */
  return name; /* most map directly: upper, lower, abs, round, etc. */
}

static int expr_to_sql(const tf_expr *e, strbuf *sb);

static int expr_to_sql(const tf_expr *e, strbuf *sb) {
  if (!e) return -1;

  switch (e->kind) {
    case EXPR_LIT_INT:
      sb_appendf(sb, "%lld", (long long)e->lit_int);
      return 0;

    case EXPR_LIT_FLOAT:
      sb_appendf(sb, TF_FLOAT64_ROUNDTRIP_FORMAT, e->lit_float);
      return 0;

    case EXPR_LIT_STR:
      sql_quote_str(sb, e->lit_str);
      return 0;

    case EXPR_COL_REF:
      sql_quote_ident(sb, e->col_name);
      return 0;

    case EXPR_CMP: {
      sb_append(sb, "(");
      if (expr_to_sql(e->cmp.left, sb) != 0) return -1;
      switch (e->cmp.op) {
        case CMP_GT: sb_append(sb, " > ");  break;
        case CMP_GE: sb_append(sb, " >= "); break;
        case CMP_LT: sb_append(sb, " < ");  break;
        case CMP_LE: sb_append(sb, " <= "); break;
        case CMP_EQ: sb_append(sb, " = ");  break;
        case CMP_NE: sb_append(sb, " <> "); break;
      }
      if (expr_to_sql(e->cmp.right, sb) != 0) return -1;
      sb_append(sb, ")");
      return 0;
    }

    case EXPR_AND:
      sb_append(sb, "(");
      if (expr_to_sql(e->binary.left, sb) != 0) return -1;
      sb_append(sb, " AND ");
      if (expr_to_sql(e->binary.right, sb) != 0) return -1;
      sb_append(sb, ")");
      return 0;

    case EXPR_OR:
      sb_append(sb, "(");
      if (expr_to_sql(e->binary.left, sb) != 0) return -1;
      sb_append(sb, " OR ");
      if (expr_to_sql(e->binary.right, sb) != 0) return -1;
      sb_append(sb, ")");
      return 0;

    case EXPR_NOT:
      sb_append(sb, "(NOT ");
      if (expr_to_sql(e->child, sb) != 0) return -1;
      sb_append(sb, ")");
      return 0;

    case EXPR_NEG:
      sb_append(sb, "(- ");
      if (expr_to_sql(e->child, sb) != 0) return -1;
      sb_append(sb, ")");
      return 0;

    case EXPR_ADD:
      sb_append(sb, "(");
      if (expr_to_sql(e->binary.left, sb) != 0) return -1;
      sb_append(sb, " + ");
      if (expr_to_sql(e->binary.right, sb) != 0) return -1;
      sb_append(sb, ")");
      return 0;

    case EXPR_SUB:
      sb_append(sb, "(");
      if (expr_to_sql(e->binary.left, sb) != 0) return -1;
      sb_append(sb, " - ");
      if (expr_to_sql(e->binary.right, sb) != 0) return -1;
      sb_append(sb, ")");
      return 0;

    case EXPR_MUL:
      sb_append(sb, "(");
      if (expr_to_sql(e->binary.left, sb) != 0) return -1;
      sb_append(sb, " * ");
      if (expr_to_sql(e->binary.right, sb) != 0) return -1;
      sb_append(sb, ")");
      return 0;

    case EXPR_DIV:
      sb_append(sb, "(");
      if (expr_to_sql(e->binary.left, sb) != 0) return -1;
      sb_append(sb, " / ");
      if (expr_to_sql(e->binary.right, sb) != 0) return -1;
      sb_append(sb, ")");
      return 0;

    case EXPR_FUNC_CALL: {
      const char *name = e->func.name;
      int n = e->func.n_args;

      /* Special: if(cond, then, else) → CASE WHEN ... THEN ... ELSE ... END */
      if (strcmp(name, "if") == 0 && n == 3) {
        sb_append(sb, "(CASE WHEN ");
        if (expr_to_sql(e->func.args[0], sb) != 0) return -1;
        sb_append(sb, " THEN ");
        if (expr_to_sql(e->func.args[1], sb) != 0) return -1;
        sb_append(sb, " ELSE ");
        if (expr_to_sql(e->func.args[2], sb) != 0) return -1;
        sb_append(sb, " END)");
        return 0;
      }

      /* Special: case_when(cond, value, ..., default) → searched CASE */
      if (strcmp(name, "case_when") == 0 && n >= 2) {
        int pair_count = n / 2;
        int default_idx = (n % 2 == 1) ? n - 1 : -1;
        sb_append(sb, "(CASE");
        for (int i = 0; i < pair_count; i++) {
          sb_append(sb, " WHEN ");
          if (expr_to_sql(e->func.args[i * 2], sb) != 0) return -1;
          sb_append(sb, " THEN ");
          if (expr_to_sql(e->func.args[i * 2 + 1], sb) != 0) return -1;
        }
        sb_append(sb, " ELSE ");
        if (default_idx >= 0) {
          if (expr_to_sql(e->func.args[default_idx], sb) != 0) return -1;
        } else {
          sb_append(sb, "NULL");
        }
        sb_append(sb, " END)");
        return 0;
      }

      /* Special: case_match(value, key, result, ..., default) → simple CASE */
      if (strcmp(name, "case_match") == 0 && n >= 3) {
        int remaining = n - 1;
        int pair_count = remaining / 2;
        int default_idx = (remaining % 2 == 1) ? n - 1 : -1;
        sb_append(sb, "(CASE ");
        if (expr_to_sql(e->func.args[0], sb) != 0) return -1;
        for (int i = 0; i < pair_count; i++) {
          sb_append(sb, " WHEN ");
          if (expr_to_sql(e->func.args[1 + i * 2], sb) != 0) return -1;
          sb_append(sb, " THEN ");
          if (expr_to_sql(e->func.args[2 + i * 2], sb) != 0) return -1;
        }
        sb_append(sb, " ELSE ");
        if (default_idx >= 0) {
          if (expr_to_sql(e->func.args[default_idx], sb) != 0) return -1;
        } else {
          sb_append(sb, "NULL");
        }
        sb_append(sb, " END)");
        return 0;
      }

      /* Special: if_any(pred, ...) / if_all(pred, ...) → OR/AND reduction */
      if (strcmp(name, "if_any") == 0 || strcmp(name, "if_all") == 0) {
        int is_any = strcmp(name, "if_any") == 0;
        if (n == 0) {
          sb_append(sb, is_any ? "(FALSE)" : "(TRUE)");
          return 0;
        }
        sb_append(sb, "(");
        for (int i = 0; i < n; i++) {
          if (i > 0) sb_append(sb, is_any ? " OR " : " AND ");
          if (expr_to_sql(e->func.args[i], sb) != 0) return -1;
        }
        sb_append(sb, ")");
        return 0;
      }

      /* Special: between(x, left, right) / inrange(x, left, right) → inclusive SQL range */
      if ((strcmp(name, "between") == 0 || strcmp(name, "inrange") == 0) && n == 3) {
        sb_append(sb, "(");
        if (expr_to_sql(e->func.args[0], sb) != 0) return -1;
        sb_append(sb, " BETWEEN ");
        if (expr_to_sql(e->func.args[1], sb) != 0) return -1;
        sb_append(sb, " AND ");
        if (expr_to_sql(e->func.args[2], sb) != 0) return -1;
        sb_append(sb, ")");
        return 0;
      }

      /* Special: date/time component extraction */
      if ((strcmp(name, "year") == 0 || strcmp(name, "month") == 0 ||
           strcmp(name, "day") == 0 || strcmp(name, "hour") == 0 ||
           strcmp(name, "minute") == 0 || strcmp(name, "second") == 0 ||
           strcmp(name, "weekday") == 0 || strcmp(name, "epoch") == 0) && n == 1) {
        const char *part = name;
        if (strcmp(name, "weekday") == 0) part = "dow";
        sb_append(sb, "EXTRACT(");
        sb_append(sb, part);
        sb_append(sb, " FROM ");
        if (expr_to_sql(e->func.args[0], sb) != 0) return -1;
        sb_append(sb, ")");
        return 0;
      }

      /* Special: date_trunc(value, 'unit') → SQL date_trunc('unit', value) */
      if (strcmp(name, "date_trunc") == 0 && n == 2 && e->func.args[1]->kind == EXPR_LIT_STR) {
        sb_append(sb, "date_trunc(");
        sql_quote_str(sb, e->func.args[1]->lit_str);
        sb_append(sb, ", ");
        if (expr_to_sql(e->func.args[0], sb) != 0) return -1;
        sb_append(sb, ")");
        return 0;
      }

      /* Special: mod(a, b) → (a % b) */
      if (strcmp(name, "mod") == 0 && n == 2) {
        sb_append(sb, "(");
        if (expr_to_sql(e->func.args[0], sb) != 0) return -1;
        sb_append(sb, " % ");
        if (expr_to_sql(e->func.args[1], sb) != 0) return -1;
        sb_append(sb, ")");
        return 0;
      }

      /* Special: slice(s, start, len) → substr(s, start+1, len)
       * tranfi uses 0-based, SQL uses 1-based */
      if (strcmp(name, "slice") == 0 && n >= 2) {
        sb_append(sb, "substr(");
        if (expr_to_sql(e->func.args[0], sb) != 0) return -1;
        sb_append(sb, ", (");
        if (expr_to_sql(e->func.args[1], sb) != 0) return -1;
        sb_append(sb, ") + 1");
        if (n >= 3) {
          sb_append(sb, ", ");
          if (expr_to_sql(e->func.args[2], sb) != 0) return -1;
        }
        sb_append(sb, ")");
        return 0;
      }

      /* General function call with name mapping */
      const char *sql_name = map_func_name(name);
      if (!sql_name) sql_name = name;
      sb_append(sb, sql_name);
      sb_append(sb, "(");
      for (int i = 0; i < n; i++) {
        if (i > 0) sb_append(sb, ", ");
        if (expr_to_sql(e->func.args[i], sb) != 0) return -1;
      }
      sb_append(sb, ")");
      return 0;
    }
  }
  return -1;
}

/* Parse an expression string and convert to SQL.
 * Returns heap-allocated SQL string, or NULL on error. */
static char *translate_expr(const char *expr_str) {
  tf_expr *e = tf_expr_parse(expr_str);
  if (!e) return NULL;
  strbuf sb;
  sb_init(&sb);
  int rc = expr_to_sql(e, &sb);
  tf_expr_free(e);
  if (rc != 0 || sb.failed) { sb_free(&sb); return NULL; }
  return sb_detach(&sb);
}

/* ---- Op handlers ---- */

/* Helper: get cJSON string value */
static const char *jstr(const cJSON *obj, const char *key) {
  cJSON *v = cJSON_GetObjectItemCaseSensitive(obj, key);
  return (v && cJSON_IsString(v)) ? v->valuestring : NULL;
}

static int jint(const cJSON *obj, const char *key, int def) {
  cJSON *v = cJSON_GetObjectItemCaseSensitive(obj, key);
  return (v && cJSON_IsNumber(v)) ? v->valueint : def;
}

static int jbool(const cJSON *obj, const char *key, int def) {
  cJSON *v = cJSON_GetObjectItemCaseSensitive(obj, key);
  if (!v) return def;
  if (cJSON_IsTrue(v)) return 1;
  if (cJSON_IsFalse(v)) return 0;
  return def;
}

static int append_frequency_key_expr(strbuf *sb, cJSON *cols) {
  if (!sb || !cJSON_IsArray(cols) || cJSON_GetArraySize(cols) <= 0) return TF_ERROR;
  int n = cJSON_GetArraySize(cols);
  for (int i = 0; i < n; i++) {
    cJSON *c = cJSON_GetArrayItem(cols, i);
    if (!cJSON_IsString(c) || !c->valuestring || c->valuestring[0] == '\0') {
      return TF_ERROR;
    }
    if (i > 0) sb_append(sb, " || chr(1) || ");
    sb_append(sb, "COALESCE(CAST(");
    sql_quote_ident(sb, c->valuestring);
    sb_append(sb, " AS VARCHAR), ");
    sql_quote_str(sb, "\\N");
    sb_append(sb, ")");
    if (sb->failed) return TF_ERROR;
  }
  return TF_OK;
}

static int append_grep_match_expr(strbuf *sb, const char *column,
                                  const char *pattern, int regex) {
  if (!sb || !column || !pattern) return TF_ERROR;
  strbuf qcol;
  sb_init(&qcol);
  sql_quote_ident(&qcol, column);
  if (qcol.failed) {
    sb_free(&qcol);
    return TF_ERROR;
  }

  sb_append(sb, "(CASE WHEN typeof(");
  sb_append(sb, qcol.data);
  sb_append(sb, ") = 'VARCHAR' THEN COALESCE(");
  if (regex) {
    sb_append(sb, "regexp_matches(CAST(");
  } else {
    sb_append(sb, "contains(CAST(");
  }
  sb_append(sb, qcol.data);
  sb_append(sb, " AS VARCHAR), ");
  sql_quote_str(sb, pattern);
  sb_append(sb, "), FALSE) ELSE FALSE END)");
  sb_free(&qcol);
  return sb->failed ? TF_ERROR : TF_OK;
}

/* Emit a CTE for a transform op. prev is the name of the previous CTE/source.
 * Appends SQL like: step_N AS (SELECT ... FROM prev ...) */
static int emit_cte(strbuf *sb, const char *cte_name, const char *prev,
                    const char *op, const cJSON *args, char **error) {

  /* ---- filter ---- */
  if (strcmp(op, "filter") == 0) {
    const char *expr = jstr(args, "expr");
    if (!expr) { *error = strdup("filter: missing 'expr'"); return -1; }
    char *sql_expr = translate_expr(expr);
    if (!sql_expr) { *error = strdup("filter: failed to translate expression"); return -1; }
    sb_appendf(sb, "%s AS (SELECT * FROM %s WHERE %s)", cte_name, prev, sql_expr);
    free(sql_expr);
    return 0;
  }

  /* ---- relocate ---- */
  if (strcmp(op, "relocate") == 0) {
    cJSON *before = cJSON_GetObjectItemCaseSensitive(args, "before");
    cJSON *after = cJSON_GetObjectItemCaseSensitive(args, "after");
    if (before || after) {
      *error = strdup("relocate: SQL lowering for before/after requires known schema; default-front relocate is supported");
      return -1;
    }
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cols || !cJSON_IsArray(cols)) { *error = strdup("relocate: missing 'columns'"); return -1; }
    if (tf_column_selectors_have_syntax_json(cols)) {
      *error = strdup("relocate: selector helpers require known schema for SQL lowering");
      return -1;
    }
    strbuf moved;
    sb_init(&moved);
    int n = cJSON_GetArraySize(cols);
    for (int i = 0; i < n; i++) {
      cJSON *c = cJSON_GetArrayItem(cols, i);
      if (!cJSON_IsString(c)) continue;
      if (moved.len > 0) sb_append(&moved, ", ");
      sql_quote_ident(&moved, c->valuestring);
    }
    if (moved.len == 0) {
      sb_free(&moved);
      *error = strdup("relocate: missing 'columns'");
      return -1;
    }
    sb_appendf(sb, "%s AS (SELECT %s, * EXCLUDE (%s) FROM %s)",
               cte_name, moved.data, moved.data, prev);
    sb_free(&moved);
    return 0;
  }

  /* ---- select / reorder ---- */
  if (strcmp(op, "select") == 0 || strcmp(op, "reorder") == 0) {
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cols || !cJSON_IsArray(cols)) { *error = strdup("select: missing 'columns'"); return -1; }
    if (tf_column_selectors_have_syntax_json(cols)) {
      *error = strdup("select: selector helpers require known schema for SQL lowering");
      return -1;
    }
    strbuf sel;
    sb_init(&sel);
    int n = cJSON_GetArraySize(cols);
    for (int i = 0; i < n; i++) {
      if (i > 0) sb_append(&sel, ", ");
      cJSON *c = cJSON_GetArrayItem(cols, i);
      if (cJSON_IsString(c)) sql_quote_ident(&sel, c->valuestring);
    }
    sb_appendf(sb, "%s AS (SELECT %s FROM %s)", cte_name, sel.data, prev);
    sb_free(&sel);
    return 0;
  }

  /* ---- rename ---- */
  if (strcmp(op, "rename") == 0) {
    cJSON *mapping = cJSON_GetObjectItemCaseSensitive(args, "mapping");
    if (!mapping) { *error = strdup("rename: missing 'mapping'"); return -1; }
    /* Use DuckDB RENAME extension: SELECT * RENAME (old AS new, ...) */
    strbuf rn;
    sb_init(&rn);
    int first = 1;
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, mapping) {
      if (!cJSON_IsString(item)) continue;
      if (!first) sb_append(&rn, ", ");
      first = 0;
      sql_quote_ident(&rn, item->string);
      sb_append(&rn, " AS ");
      sql_quote_ident(&rn, item->valuestring);
    }
    sb_appendf(sb, "%s AS (SELECT * RENAME (%s) FROM %s)", cte_name, rn.data, prev);
    sb_free(&rn);
    return 0;
  }

  /* ---- derive ---- */
  if (strcmp(op, "derive") == 0) {
    cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!columns || !cJSON_IsArray(columns)) { *error = strdup("derive: missing 'columns'"); return -1; }
    strbuf der;
    sb_init(&der);
    int n = cJSON_GetArraySize(columns);
    for (int i = 0; i < n; i++) {
      cJSON *col = cJSON_GetArrayItem(columns, i);
      const char *name = jstr(col, "name");
      const char *expr = jstr(col, "expr");
      if (!name || !expr) continue;
      char *sql_expr = translate_expr(expr);
      if (!sql_expr) { sb_free(&der); *error = strdup("derive: failed to translate expression"); return -1; }
      sb_append(&der, ", ");
      sb_append(&der, sql_expr);
      sb_append(&der, " AS ");
      sql_quote_ident(&der, name);
      free(sql_expr);
    }
    sb_appendf(sb, "%s AS (SELECT *%s FROM %s)", cte_name, der.data, prev);
    sb_free(&der);
    return 0;
  }

  /* ---- validate ---- */
  if (strcmp(op, "validate") == 0) {
    const char *expr = jstr(args, "expr");
    if (!expr) { *error = strdup("validate: missing 'expr'"); return -1; }
    char *sql_expr = translate_expr(expr);
    if (!sql_expr) { *error = strdup("validate: failed to translate expression"); return -1; }
    sb_appendf(sb, "%s AS (SELECT *, (%s) AS \"_valid\" FROM %s)", cte_name, sql_expr, prev);
    free(sql_expr);
    return 0;
  }

  /* ---- unique / dedup ---- */
  if (strcmp(op, "unique") == 0 || strcmp(op, "dedup") == 0) {
    cJSON *sorted = cJSON_GetObjectItemCaseSensitive(args, "sorted");
    if (cJSON_IsBool(sorted) && cJSON_IsTrue(sorted)) {
      *error = strdup("unique: sorted=true adjacent-run mode cannot be lowered to SQL");
      return -1;
    }
    cJSON *mode = cJSON_GetObjectItemCaseSensitive(args, "mode");
    cJSON *approx = cJSON_GetObjectItemCaseSensitive(args, "approx");
    if ((cJSON_IsString(mode) && mode->valuestring && strcmp(mode->valuestring, "approx") == 0) ||
        (cJSON_IsBool(approx) && cJSON_IsTrue(approx))) {
      *error = strdup("unique: approximate mode cannot be lowered to SQL");
      return -1;
    }
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (cols && cJSON_IsArray(cols) && cJSON_GetArraySize(cols) > 0) {
      strbuf dcols;
      sb_init(&dcols);
      int n = cJSON_GetArraySize(cols);
      for (int i = 0; i < n; i++) {
        if (i > 0) sb_append(&dcols, ", ");
        cJSON *c = cJSON_GetArrayItem(cols, i);
        if (cJSON_IsString(c)) sql_quote_ident(&dcols, c->valuestring);
      }
      sb_appendf(sb, "%s AS (SELECT DISTINCT ON (%s) * FROM %s)", cte_name, dcols.data, prev);
      sb_free(&dcols);
    } else {
      sb_appendf(sb, "%s AS (SELECT DISTINCT * FROM %s)", cte_name, prev);
    }
    return 0;
  }

  /* ---- sort ---- */
  if (strcmp(op, "sort") == 0) {
    cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!columns || !cJSON_IsArray(columns)) { *error = strdup("sort: missing 'columns'"); return -1; }
    strbuf ord;
    sb_init(&ord);
    int n = cJSON_GetArraySize(columns);
    for (int i = 0; i < n; i++) {
      if (i > 0) sb_append(&ord, ", ");
      cJSON *c = cJSON_GetArrayItem(columns, i);
      const char *name = jstr(c, "name");
      int desc = jbool(c, "desc", 0);
      if (name) {
        sql_quote_ident(&ord, name);
        sb_append(&ord, desc ? " DESC" : " ASC");
      }
    }
    sb_appendf(sb, "%s AS (SELECT * FROM %s ORDER BY %s)", cte_name, prev, ord.data);
    sb_free(&ord);
    return 0;
  }

  /* ---- head ---- */
  if (strcmp(op, "head") == 0 || strcmp(op, "slice-head") == 0) {
    int n = jint(args, "n", 10);
    sb_appendf(sb, "%s AS (SELECT * FROM %s LIMIT %d)", cte_name, prev, n);
    return 0;
  }

  /* ---- skip ---- */
  if (strcmp(op, "skip") == 0) {
    int n = jint(args, "n", 0);
    sb_appendf(sb, "%s AS (SELECT * FROM %s OFFSET %d)", cte_name, prev, n);
    return 0;
  }

  /* ---- tail ---- */
  if (strcmp(op, "tail") == 0 || strcmp(op, "slice-tail") == 0) {
    int n = jint(args, "n", 10);
    sb_appendf(sb, "%s AS (SELECT * FROM (SELECT *, ROW_NUMBER() OVER () AS _rn, "
               "COUNT(*) OVER () AS _total FROM %s) WHERE _rn > _total - %d)",
               cte_name, prev, n);
    return 0;
  }

  /* ---- bounded top-k / bottom-k ---- */
  if (strcmp(op, "top") == 0 || strcmp(op, "top-k") == 0 ||
      strcmp(op, "bottom-k") == 0 || strcmp(op, "slice-min") == 0 ||
      strcmp(op, "slice-max") == 0) {
    int n = jint(args, "n", 10);
    const char *column = jstr(args, "column");
    int desc = jbool(args, "desc", 1);
    if (!column) { *error = strdup("top-k: missing 'column'"); return -1; }
    strbuf tmp;
    sb_init(&tmp);
    sql_quote_ident(&tmp, column);
    sb_appendf(sb, "%s AS (SELECT * FROM %s ORDER BY %s %s LIMIT %d)",
               cte_name, prev, tmp.data, desc ? "DESC" : "ASC", n);
    sb_free(&tmp);
    return 0;
  }

  /* ---- sample ---- */
  if (strcmp(op, "sample") == 0) {
    *error = strdup("sample: deterministic reservoir sampling cannot be lowered to SQL");
    return -1;
  }

  /* ---- grep ---- */
  if (strcmp(op, "grep") == 0) {
    const char *pattern = jstr(args, "pattern");
    const char *column = jstr(args, "column");
    int invert = jbool(args, "invert", 0);
    int regex = jbool(args, "regex", 0);
    if (!pattern) { *error = strdup("grep: missing 'pattern'"); return -1; }
    if (!column) column = "_line";
    strbuf pred;
    sb_init(&pred);
    if (append_grep_match_expr(&pred, column, pattern, regex) != TF_OK) {
      sb_free(&pred);
      *error = strdup("sql: out of memory");
      return -1;
    }
    sb_appendf(sb, "%s AS (SELECT * FROM %s WHERE %s%s)",
               cte_name, prev, invert ? "NOT " : "", pred.data);
    sb_free(&pred);
    return 0;
  }

  /* ---- cast ---- */
  if (strcmp(op, "cast") == 0) {
    cJSON *mapping = cJSON_GetObjectItemCaseSensitive(args, "mapping");
    if (!mapping) { *error = strdup("cast: missing 'mapping'"); return -1; }
    strbuf cols;
    sb_init(&cols);
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, mapping) {
      if (!cJSON_IsString(item)) continue;
      sb_append(&cols, ", CAST(");
      sql_quote_ident(&cols, item->string);
      /* Map tranfi types to SQL types */
      const char *tf_type = item->valuestring;
      const char *sql_type = "VARCHAR";
      if (strcmp(tf_type, "int") == 0 || strcmp(tf_type, "int64") == 0) sql_type = "BIGINT";
      else if (strcmp(tf_type, "float") == 0 || strcmp(tf_type, "float64") == 0) sql_type = "DOUBLE";
      else if (strcmp(tf_type, "bool") == 0) sql_type = "BOOLEAN";
      else if (strcmp(tf_type, "string") == 0) sql_type = "VARCHAR";
      else if (strcmp(tf_type, "date") == 0) sql_type = "DATE";
      else if (strcmp(tf_type, "timestamp") == 0) sql_type = "TIMESTAMP";
      sb_appendf(&cols, " AS %s) AS ", sql_type);
      sql_quote_ident(&cols, item->string);
    }
    /* Use COLUMNS(*) EXCLUDE + explicit casts — simpler: use REPLACE */
    /* DuckDB REPLACE: SELECT * REPLACE (CAST(col AS type) AS col) */
    strbuf rep;
    sb_init(&rep);
    int first = 1;
    cJSON_ArrayForEach(item, mapping) {
      if (!cJSON_IsString(item)) continue;
      if (!first) sb_append(&rep, ", ");
      first = 0;
      const char *tf_type = item->valuestring;
      const char *sql_type = "VARCHAR";
      if (strcmp(tf_type, "int") == 0 || strcmp(tf_type, "int64") == 0) sql_type = "BIGINT";
      else if (strcmp(tf_type, "float") == 0 || strcmp(tf_type, "float64") == 0) sql_type = "DOUBLE";
      else if (strcmp(tf_type, "bool") == 0) sql_type = "BOOLEAN";
      else if (strcmp(tf_type, "string") == 0) sql_type = "VARCHAR";
      else if (strcmp(tf_type, "date") == 0) sql_type = "DATE";
      else if (strcmp(tf_type, "timestamp") == 0) sql_type = "TIMESTAMP";
      sb_append(&rep, "CAST(");
      sql_quote_ident(&rep, item->string);
      sb_appendf(&rep, " AS %s) AS ", sql_type);
      sql_quote_ident(&rep, item->string);
    }
    sb_free(&cols);
    sb_appendf(sb, "%s AS (SELECT * REPLACE (%s) FROM %s)", cte_name, rep.data, prev);
    sb_free(&rep);
    return 0;
  }

  /* ---- clip ---- */
  if (strcmp(op, "clip") == 0) {
    const char *column = jstr(args, "column");
    if (!column) { *error = strdup("clip: missing 'column'"); return -1; }
    cJSON *min_v = cJSON_GetObjectItemCaseSensitive(args, "min");
    cJSON *max_v = cJSON_GetObjectItemCaseSensitive(args, "max");
    strbuf expr;
    sb_init(&expr);
    sql_quote_ident(&expr, column);
    if (min_v && max_v) {
      strbuf tmp;
      sb_init(&tmp);
      sql_quote_ident(&tmp, column);
      sb_init(&expr);
      sb_appendf(&expr, "GREATEST(" TF_FLOAT64_ROUNDTRIP_FORMAT ", LEAST(" TF_FLOAT64_ROUNDTRIP_FORMAT ", %s))", min_v->valuedouble, max_v->valuedouble, tmp.data);
      sb_free(&tmp);
    } else if (min_v) {
      strbuf tmp;
      sb_init(&tmp);
      sql_quote_ident(&tmp, column);
      sb_init(&expr);
      sb_appendf(&expr, "GREATEST(" TF_FLOAT64_ROUNDTRIP_FORMAT ", %s)", min_v->valuedouble, tmp.data);
      sb_free(&tmp);
    } else if (max_v) {
      strbuf tmp;
      sb_init(&tmp);
      sql_quote_ident(&tmp, column);
      sb_init(&expr);
      sb_appendf(&expr, "LEAST(" TF_FLOAT64_ROUNDTRIP_FORMAT ", %s)", max_v->valuedouble, tmp.data);
      sb_free(&tmp);
    }
    strbuf qcol;
    sb_init(&qcol);
    sql_quote_ident(&qcol, column);
    sb_appendf(sb, "%s AS (SELECT * REPLACE (%s AS %s) FROM %s)", cte_name, expr.data, qcol.data, prev);
    sb_free(&expr);
    sb_free(&qcol);
    return 0;
  }

  /* ---- replace ---- */
  if (strcmp(op, "replace") == 0) {
    const char *column = jstr(args, "column");
    const char *pattern = jstr(args, "pattern");
    const char *replacement = jstr(args, "replacement");
    int regex = jbool(args, "regex", 0);
    if (!column || !pattern || !replacement) { *error = strdup("replace: missing args"); return -1; }
    strbuf qcol;
    sb_init(&qcol);
    sql_quote_ident(&qcol, column);
    strbuf expr;
    sb_init(&expr);
    if (regex) {
      sb_append(&expr, "regexp_replace(");
      sb_append(&expr, qcol.data);
      sb_append(&expr, ", ");
      sql_quote_str(&expr, pattern);
      sb_append(&expr, ", ");
      sql_quote_str(&expr, replacement);
      sb_append(&expr, ", 'g')");
    } else {
      sb_append(&expr, "replace(");
      sb_append(&expr, qcol.data);
      sb_append(&expr, ", ");
      sql_quote_str(&expr, pattern);
      sb_append(&expr, ", ");
      sql_quote_str(&expr, replacement);
      sb_append(&expr, ")");
    }
    sb_appendf(sb, "%s AS (SELECT * REPLACE (%s AS %s) FROM %s)", cte_name, expr.data, qcol.data, prev);
    sb_free(&qcol);
    sb_free(&expr);
    return 0;
  }

  /* ---- trim ---- */
  if (strcmp(op, "trim") == 0) {
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cols || !cJSON_IsArray(cols) || cJSON_GetArraySize(cols) <= 0) {
      *error = strdup("trim: SQL lowering requires explicit columns");
      return -1;
    }
    strbuf rep;
    sb_init(&rep);
    int n = cJSON_GetArraySize(cols);
    for (int i = 0; i < n; i++) {
      cJSON *c = cJSON_GetArrayItem(cols, i);
      if (!cJSON_IsString(c) || !c->valuestring || c->valuestring[0] == '\0') {
        sb_free(&rep);
        *error = strdup("trim: invalid columns");
        return -1;
      }
      if (i > 0) sb_append(&rep, ", ");
      sb_append(&rep, "trim(");
      sql_quote_ident(&rep, c->valuestring);
      sb_append(&rep, ") AS ");
      sql_quote_ident(&rep, c->valuestring);
    }
    if (rep.failed) {
      sb_free(&rep);
      *error = strdup("sql: out of memory");
      return -1;
    }
    sb_appendf(sb, "%s AS (SELECT * REPLACE (%s) FROM %s)", cte_name, rep.data, prev);
    sb_free(&rep);
    return 0;
  }

  /* ---- fill-null ---- */
  if (strcmp(op, "fill-null") == 0) {
    cJSON *mapping = cJSON_GetObjectItemCaseSensitive(args, "mapping");
    if (!mapping) { *error = strdup("fill-null: missing 'mapping'"); return -1; }
    strbuf rep;
    sb_init(&rep);
    int first = 1;
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, mapping) {
      if (!cJSON_IsString(item)) continue;
      if (!first) sb_append(&rep, ", ");
      first = 0;
      sb_append(&rep, "COALESCE(");
      sql_quote_ident(&rep, item->string);
      sb_append(&rep, ", ");
      sql_quote_str(&rep, item->valuestring);
      sb_append(&rep, ") AS ");
      sql_quote_ident(&rep, item->string);
    }
    sb_appendf(sb, "%s AS (SELECT * REPLACE (%s) FROM %s)", cte_name, rep.data, prev);
    sb_free(&rep);
    return 0;
  }

  /* ---- group-agg ---- */
  if (strcmp(op, "group-agg") == 0) {
    cJSON *group_by = cJSON_GetObjectItemCaseSensitive(args, "group_by");
    cJSON *aggs = cJSON_GetObjectItemCaseSensitive(args, "aggs");
    if (!group_by || !aggs) { *error = strdup("group-agg: missing args"); return -1; }
    strbuf sel;
    sb_init(&sel);
    /* Group columns */
    int ng = cJSON_GetArraySize(group_by);
    for (int i = 0; i < ng; i++) {
      if (i > 0) sb_append(&sel, ", ");
      cJSON *c = cJSON_GetArrayItem(group_by, i);
      if (cJSON_IsString(c)) sql_quote_ident(&sel, c->valuestring);
    }
    /* Aggregate functions */
    int na = cJSON_GetArraySize(aggs);
    for (int i = 0; i < na; i++) {
      sb_append(&sel, ", ");
      cJSON *agg = cJSON_GetArrayItem(aggs, i);
      const char *col = jstr(agg, "column");
      const char *func = jstr(agg, "func");
      const char *result = jstr(agg, "name");
      if (!result) result = jstr(agg, "result");
      if (!col || !func) continue;
      /* Map agg function names */
      const char *sql_func = func;
      if (strcmp(func, "avg") == 0) sql_func = "AVG";
      else if (strcmp(func, "sum") == 0) sql_func = "SUM";
      else if (strcmp(func, "count") == 0) sql_func = "COUNT";
      else if (strcmp(func, "min") == 0) sql_func = "MIN";
      else if (strcmp(func, "max") == 0) sql_func = "MAX";
      else if (strcmp(func, "stddev") == 0) sql_func = "STDDEV_SAMP";
      else if (strcmp(func, "var") == 0) sql_func = "VAR_SAMP";
      else if (strcmp(func, "median") == 0) sql_func = "MEDIAN";
      sb_append(&sel, sql_func);
      sb_append(&sel, "(");
      if (strcmp(func, "count") == 0 && strcmp(col, "*") == 0) sb_append(&sel, "*");
      else sql_quote_ident(&sel, col);
      sb_append(&sel, ") AS ");
      if (result) sql_quote_ident(&sel, result);
      else {
        sql_quote_joined_ident(&sel, col, "_", func);
      }
    }
    /* GROUP BY clause */
    strbuf grp;
    sb_init(&grp);
    for (int i = 0; i < ng; i++) {
      if (i > 0) sb_append(&grp, ", ");
      cJSON *c = cJSON_GetArrayItem(group_by, i);
      if (cJSON_IsString(c)) sql_quote_ident(&grp, c->valuestring);
    }
    sb_appendf(sb, "%s AS (SELECT %s FROM %s GROUP BY %s)", cte_name, sel.data, prev, grp.data);
    sb_free(&sel);
    sb_free(&grp);
    return 0;
  }

  /* ---- frequency ---- */
  if (strcmp(op, "frequency") == 0) {
    cJSON *overflow = args ? cJSON_GetObjectItemCaseSensitive(args, "overflow") : NULL;
    if (cJSON_IsString(overflow) && strcmp(overflow->valuestring, "other") == 0) {
      *error = strdup("frequency: overflow=other is not supported by SQL lowering");
      return -1;
    }

    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cols || !cJSON_IsArray(cols) || cJSON_GetArraySize(cols) <= 0) {
      *error = strdup("frequency: SQL lowering requires explicit columns");
      return -1;
    }
    strbuf key;
    sb_init(&key);
    if (append_frequency_key_expr(&key, cols) != TF_OK) {
      sb_free(&key);
      *error = strdup("frequency: invalid columns");
      return -1;
    }
    sb_appendf(sb, "%s AS (SELECT \"value\", COUNT(*) AS \"count\" FROM "
                 "(SELECT %s AS \"value\" FROM %s) __tf_frequency "
                 "GROUP BY \"value\" ORDER BY \"count\" DESC, \"value\" ASC)",
               cte_name, key.data, prev);
    sb_free(&key);
    return 0;
  }

  /* ---- join ---- */
  if (strcmp(op, "join") == 0 || strcmp(op, "semi-join") == 0 || strcmp(op, "anti-join") == 0) {
    const char *file = jstr(args, "file");
    const char *on = jstr(args, "on");
    const char *how = jstr(args, "how");
    if (!file || !on) { *error = strdup("join: missing 'file' or 'on'"); return -1; }
    if (!how) {
      if (strcmp(op, "semi-join") == 0) how = "semi";
      else if (strcmp(op, "anti-join") == 0) how = "anti";
      else how = "inner";
    }
    const char *join_type = "INNER";
    if (strcmp(how, "left") == 0) join_type = "LEFT";
    else if (strcmp(how, "right") == 0) join_type = "RIGHT";
    else if (strcmp(how, "outer") == 0 || strcmp(how, "full") == 0) join_type = "FULL OUTER";

    /* Parse on: either "col" (same name both sides) or "left_col=right_col" */
    const char *eq = strchr(on, '=');
    strbuf cond;
    sb_init(&cond);
    if (eq) {
      char left_col[128], right_col[128];
      size_t llen = (size_t)(eq - on);
      if (llen >= sizeof(left_col)) llen = sizeof(left_col) - 1;
      memcpy(left_col, on, llen);
      left_col[llen] = '\0';
      strncpy(right_col, eq + 1, sizeof(right_col) - 1);
      right_col[sizeof(right_col) - 1] = '\0';
      /* Trim whitespace */
      while (llen > 0 && left_col[llen - 1] == ' ') left_col[--llen] = '\0';
      char *rp = right_col;
      while (*rp == ' ') rp++;
      sb_append(&cond, "a.");
      sql_quote_ident(&cond, left_col);
      sb_append(&cond, " = b.");
      sql_quote_ident(&cond, rp);
    } else {
      sb_append(&cond, "a.");
      sql_quote_ident(&cond, on);
      sb_append(&cond, " = b.");
      sql_quote_ident(&cond, on);
    }
    if (strcmp(how, "semi") == 0 || strcmp(how, "anti") == 0) {
      sb_appendf(sb, "%s AS (SELECT a.* FROM %s a WHERE %sEXISTS (SELECT 1 FROM read_csv_auto(",
                 cte_name, prev, strcmp(how, "anti") == 0 ? "NOT " : "");
      sql_quote_str(sb, file);
      sb_appendf(sb, ") b WHERE %s))", cond.data);
      sb_free(&cond);
      return 0;
    }

    sb_appendf(sb, "%s AS (SELECT a.* FROM %s a %s JOIN read_csv_auto(",
               cte_name, prev, join_type);
    sql_quote_str(sb, file);
    sb_appendf(sb, ") b ON %s)", cond.data);
    sb_free(&cond);
    return 0;
  }

  /* ---- set ops ---- */
  if (strcmp(op, "intersect") == 0 || strcmp(op, "setdiff") == 0 ||
      strcmp(op, "intersect-all") == 0 || strcmp(op, "setdiff-all") == 0 ||
      strcmp(op, "union") == 0 || strcmp(op, "union-all") == 0) {
    const char *file = jstr(args, "file");
    if (!file) { *error = strdup("set op: missing 'file'"); return -1; }
    cJSON *cols = args ? cJSON_GetObjectItemCaseSensitive(args, "columns") : NULL;
    if (cols && cJSON_IsArray(cols) && cJSON_GetArraySize(cols) > 0) {
      *error = strdup("set op SQL lowering only supports all-column set operations");
      return -1;
    }
    const char *sql_op = "INTERSECT";
    if (strcmp(op, "setdiff") == 0) sql_op = "EXCEPT";
    else if (strcmp(op, "intersect-all") == 0) sql_op = "INTERSECT ALL";
    else if (strcmp(op, "setdiff-all") == 0) sql_op = "EXCEPT ALL";
    else if (strcmp(op, "union") == 0) sql_op = "UNION";
    else if (strcmp(op, "union-all") == 0) sql_op = "UNION ALL";
    sb_appendf(sb, "%s AS (SELECT * FROM %s %s SELECT * FROM read_csv_auto(", cte_name, prev, sql_op);
    sql_quote_str(sb, file);
    sb_append(sb, "))");
    return 0;
  }

  /* ---- stack ---- */
  if (strcmp(op, "stack") == 0) {
    const char *file = jstr(args, "file");
    if (!file) { *error = strdup("stack: missing 'file'"); return -1; }
    sb_appendf(sb, "%s AS (SELECT * FROM %s UNION ALL SELECT * FROM read_csv_auto(", cte_name, prev);
    sql_quote_str(sb, file);
    sb_append(sb, "))");
    return 0;
  }

  /* ---- explode ---- */
  if (strcmp(op, "explode") == 0) {
    const char *column = jstr(args, "column");
    const char *delimiter = jstr(args, "delimiter");
    if (!column) { *error = strdup("explode: missing 'column'"); return -1; }
    if (!delimiter) delimiter = ",";
    strbuf qcol;
    sb_init(&qcol);
    sql_quote_ident(&qcol, column);
    sb_appendf(sb, "%s AS (SELECT * REPLACE (unnest(string_split(%s, ", cte_name, qcol.data);
    sql_quote_str(sb, delimiter);
    sb_appendf(sb, ")) AS %s) FROM %s)", qcol.data, prev);
    sb_free(&qcol);
    return 0;
  }

  /* ---- split ---- */
  if (strcmp(op, "split") == 0) {
    const char *column = jstr(args, "column");
    const char *delimiter = jstr(args, "delimiter");
    cJSON *names = cJSON_GetObjectItemCaseSensitive(args, "names");
    if (!column || !names) { *error = strdup("split: missing args"); return -1; }
    if (!delimiter) delimiter = " ";
    strbuf qcol;
    sb_init(&qcol);
    sql_quote_ident(&qcol, column);
    strbuf der;
    sb_init(&der);
    int n = cJSON_GetArraySize(names);
    for (int i = 0; i < n; i++) {
      cJSON *name = cJSON_GetArrayItem(names, i);
      if (!cJSON_IsString(name)) continue;
      sb_appendf(&der, ", string_split(%s, ", qcol.data);
      sql_quote_str(&der, delimiter);
      sb_appendf(&der, ")[%d] AS ", i + 1);
      sql_quote_ident(&der, name->valuestring);
    }
    sb_appendf(sb, "%s AS (SELECT *%s FROM %s)", cte_name, der.data, prev);
    sb_free(&qcol);
    sb_free(&der);
    return 0;
  }

  /* ---- unpivot ---- */
  if (strcmp(op, "unpivot") == 0) {
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cols || !cJSON_IsArray(cols)) { *error = strdup("unpivot: missing 'columns'"); return -1; }
    strbuf ucols;
    sb_init(&ucols);
    int n = cJSON_GetArraySize(cols);
    for (int i = 0; i < n; i++) {
      if (i > 0) sb_append(&ucols, ", ");
      cJSON *c = cJSON_GetArrayItem(cols, i);
      if (cJSON_IsString(c)) sql_quote_ident(&ucols, c->valuestring);
    }
    sb_appendf(sb, "%s AS (UNPIVOT %s ON %s INTO NAME \"variable\" VALUE \"value\")",
               cte_name, prev, ucols.data);
    sb_free(&ucols);
    return 0;
  }

  /* ---- pivot ---- */
  if (strcmp(op, "pivot") == 0) {
    const char *name_col = jstr(args, "name_column");
    const char *val_col = jstr(args, "value_column");
    const char *agg = jstr(args, "agg");
    if (!name_col || !val_col) { *error = strdup("pivot: missing args"); return -1; }
    if (!agg) agg = "first";
    const char *sql_agg = "FIRST";
    if (strcmp(agg, "sum") == 0) sql_agg = "SUM";
    else if (strcmp(agg, "avg") == 0) sql_agg = "AVG";
    else if (strcmp(agg, "count") == 0) sql_agg = "COUNT";
    else if (strcmp(agg, "min") == 0) sql_agg = "MIN";
    else if (strcmp(agg, "max") == 0) sql_agg = "MAX";
    strbuf qn, qv;
    sb_init(&qn); sb_init(&qv);
    sql_quote_ident(&qn, name_col);
    sql_quote_ident(&qv, val_col);
    sb_appendf(sb, "%s AS (PIVOT %s ON %s USING %s(%s))",
               cte_name, prev, qn.data, sql_agg, qv.data);
    sb_free(&qn); sb_free(&qv);
    return 0;
  }

  /* ---- bin ---- */
  if (strcmp(op, "bin") == 0) {
    const char *column = jstr(args, "column");
    cJSON *boundaries = cJSON_GetObjectItemCaseSensitive(args, "boundaries");
    if (!column || !boundaries) { *error = strdup("bin: missing args"); return -1; }
    strbuf qcol;
    sb_init(&qcol);
    sql_quote_ident(&qcol, column);
    strbuf expr;
    sb_init(&expr);
    sb_append(&expr, "CASE");
    int n = cJSON_GetArraySize(boundaries);
    for (int i = 0; i < n; i++) {
      cJSON *b = cJSON_GetArrayItem(boundaries, i);
      double val = b->valuedouble;
      if (i == 0) {
        sb_appendf(&expr, " WHEN %s < " TF_FLOAT64_ROUNDTRIP_FORMAT " THEN '<" TF_FLOAT64_ROUNDTRIP_FORMAT "'", qcol.data, val, val);
      }
      if (i > 0) {
        cJSON *prev_b = cJSON_GetArrayItem(boundaries, i - 1);
        sb_appendf(&expr, " WHEN %s >= " TF_FLOAT64_ROUNDTRIP_FORMAT " AND %s < " TF_FLOAT64_ROUNDTRIP_FORMAT " THEN '" TF_FLOAT64_ROUNDTRIP_FORMAT "-" TF_FLOAT64_ROUNDTRIP_FORMAT "'",
                   qcol.data, prev_b->valuedouble, qcol.data, val,
                   prev_b->valuedouble, val);
      }
    }
    if (n > 0) {
      cJSON *last = cJSON_GetArrayItem(boundaries, n - 1);
      sb_appendf(&expr, " WHEN %s >= " TF_FLOAT64_ROUNDTRIP_FORMAT " THEN '" TF_FLOAT64_ROUNDTRIP_FORMAT "+'", qcol.data, last->valuedouble, last->valuedouble);
    }
    sb_append(&expr, " END");
    strbuf bin_col;
    sb_init(&bin_col);
    sb_appendf(&bin_col, "%s_bin", column);
    strbuf qbin;
    sb_init(&qbin);
    sql_quote_ident(&qbin, bin_col.data);
    sb_appendf(sb, "%s AS (SELECT *, %s AS %s FROM %s)", cte_name, expr.data, qbin.data, prev);
    sb_free(&qcol); sb_free(&expr); sb_free(&bin_col); sb_free(&qbin);
    return 0;
  }

  /* ---- hash ---- */
  if (strcmp(op, "hash") == 0) {
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    strbuf expr;
    sb_init(&expr);
    if (cols && cJSON_IsArray(cols) && cJSON_GetArraySize(cols) > 0) {
      sb_append(&expr, "hash(");
      int n = cJSON_GetArraySize(cols);
      for (int i = 0; i < n; i++) {
        if (i > 0) sb_append(&expr, ", ");
        cJSON *c = cJSON_GetArrayItem(cols, i);
        if (cJSON_IsString(c)) sql_quote_ident(&expr, c->valuestring);
      }
      sb_append(&expr, ")");
    } else {
      sb_append(&expr, "hash(*)");
    }
    sb_appendf(sb, "%s AS (SELECT *, %s AS \"_hash\" FROM %s)", cte_name, expr.data, prev);
    sb_free(&expr);
    return 0;
  }

  /* ---- fill-down ---- */
  if (strcmp(op, "fill-down") == 0) {
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (cols && cJSON_IsArray(cols) && cJSON_GetArraySize(cols) > 0) {
      strbuf rep;
      sb_init(&rep);
      int n = cJSON_GetArraySize(cols);
      for (int i = 0; i < n; i++) {
        if (i > 0) sb_append(&rep, ", ");
        cJSON *c = cJSON_GetArrayItem(cols, i);
        if (!cJSON_IsString(c)) continue;
        strbuf qc;
        sb_init(&qc);
        sql_quote_ident(&qc, c->valuestring);
        sb_appendf(&rep, "LAST_VALUE(%s IGNORE NULLS) OVER (ORDER BY _rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS %s",
                   qc.data, qc.data);
        sb_free(&qc);
      }
      sb_appendf(sb, "%s AS (SELECT * REPLACE (%s) FROM %s)", cte_name, rep.data, prev);
      sb_free(&rep);
    } else {
      sb_appendf(sb, "%s AS (SELECT * FROM %s)", cte_name, prev);
    }
    return 0;
  }

  /* ---- window / rolling ---- */
  if (strcmp(op, "window") == 0 || strcmp(op, "rolling-sum") == 0 ||
      strcmp(op, "rolling-mean") == 0 || strcmp(op, "rolling-min") == 0 ||
      strcmp(op, "rolling-max") == 0 || strcmp(op, "rolling-any") == 0 ||
      strcmp(op, "rolling-all") == 0) {
    const char *column = jstr(args, "column");
    int size = jint(args, "size", 3);
    const char *func = jstr(args, "func");
    const char *result = jstr(args, "result");
    int bool_roll = 0;
    if (strcmp(op, "rolling-sum") == 0) func = "sum";
    else if (strcmp(op, "rolling-mean") == 0) func = "avg";
    else if (strcmp(op, "rolling-min") == 0) func = "min";
    else if (strcmp(op, "rolling-max") == 0) func = "max";
    else if (strcmp(op, "rolling-any") == 0) { func = "any"; bool_roll = 1; }
    else if (strcmp(op, "rolling-all") == 0) { func = "all"; bool_roll = 1; }
    if (!column || !func) { *error = strdup("window: missing args"); return -1; }

    strbuf qcol, qres;
    sb_init(&qcol); sb_init(&qres);
    sql_quote_ident(&qcol, column);
    if (result) sql_quote_ident(&qres, result);
    else sb_appendf(&qres, "\"%s_%s%d\"", column, func, size);

    int preceding = size > 0 ? size - 1 : 0;
    if (bool_roll) {
      const char *nulls = jstr(args, "nulls");
      if (!nulls) nulls = "ignore";
      if (strcmp(nulls, "ignore") != 0 && strcmp(nulls, "false") != 0 &&
          strcmp(nulls, "true") != 0 && strcmp(nulls, "propagate") != 0) {
        sb_free(&qcol); sb_free(&qres);
        *error = strdup("rolling-any/all: nulls must be ignore, false, true, or propagate");
        return -1;
      }
      const char *sql_func = strcmp(func, "any") == 0 ? "BOOL_OR" : "BOOL_AND";
      if (strcmp(nulls, "propagate") == 0) {
        sb_appendf(sb, "%s AS (SELECT *, CASE WHEN COUNT(*) OVER (ORDER BY _rn ROWS BETWEEN %d PRECEDING AND CURRENT ROW) > COUNT(%s) OVER (ORDER BY _rn ROWS BETWEEN %d PRECEDING AND CURRENT ROW) THEN NULL ELSE %s(%s) OVER (ORDER BY _rn ROWS BETWEEN %d PRECEDING AND CURRENT ROW) END AS %s FROM %s)",
                   cte_name, preceding, qcol.data, preceding, sql_func, qcol.data, preceding, qres.data, prev);
      } else if (strcmp(nulls, "false") == 0 || strcmp(nulls, "true") == 0) {
        const char *fill = strcmp(nulls, "true") == 0 ? "TRUE" : "FALSE";
        sb_appendf(sb, "%s AS (SELECT *, %s(COALESCE(%s, %s)) OVER (ORDER BY _rn ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS %s FROM %s)",
                   cte_name, sql_func, qcol.data, fill, preceding, qres.data, prev);
      } else {
        sb_appendf(sb, "%s AS (SELECT *, %s(%s) OVER (ORDER BY _rn ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS %s FROM %s)",
                   cte_name, sql_func, qcol.data, preceding, qres.data, prev);
      }
      sb_free(&qcol); sb_free(&qres);
      return 0;
    }

    const char *sql_func = "AVG";
    if (strcmp(func, "sum") == 0) sql_func = "SUM";
    else if (strcmp(func, "min") == 0) sql_func = "MIN";
    else if (strcmp(func, "max") == 0) sql_func = "MAX";
    else if (strcmp(func, "avg") == 0 || strcmp(func, "mean") == 0) sql_func = "AVG";
    sb_appendf(sb, "%s AS (SELECT *, %s(%s) OVER (ORDER BY _rn ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS %s FROM %s)",
               cte_name, sql_func, qcol.data, preceding, qres.data, prev);
    sb_free(&qcol); sb_free(&qres);
    return 0;
  }

  /* ---- step (running aggregate) ---- */
  if (strcmp(op, "step") == 0) {
    const char *column = jstr(args, "column");
    const char *func = jstr(args, "func");
    const char *result = jstr(args, "result");
    if (!column || !func) { *error = strdup("step: missing args"); return -1; }
    const char *sql_func = "SUM";
    if (strcmp(func, "cumsum") == 0 || strcmp(func, "running-sum") == 0) sql_func = "SUM";
    else if (strcmp(func, "cummax") == 0 || strcmp(func, "running-max") == 0) sql_func = "MAX";
    else if (strcmp(func, "cummin") == 0 || strcmp(func, "running-min") == 0) sql_func = "MIN";
    else if (strcmp(func, "cumavg") == 0 || strcmp(func, "running-avg") == 0) sql_func = "AVG";
    strbuf qcol, qres;
    sb_init(&qcol); sb_init(&qres);
    sql_quote_ident(&qcol, column);
    if (result) sql_quote_ident(&qres, result);
    else sb_appendf(&qres, "\"%s_%s\"", func, column);
    sb_appendf(sb, "%s AS (SELECT *, %s(%s) OVER (ORDER BY _rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS %s FROM %s)",
               cte_name, sql_func, qcol.data, qres.data, prev);
    sb_free(&qcol); sb_free(&qres);
    return 0;
  }

  /* ---- rowid ---- */
  if (strcmp(op, "rowid") == 0) {
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    const char *result = jstr(args, "result");
    if (!result) result = "_rowid";

    strbuf qres;
    sb_init(&qres);
    sql_quote_ident(&qres, result);

    strbuf partition;
    sb_init(&partition);
    if (cJSON_IsArray(cols) && cJSON_GetArraySize(cols) > 0) {
      int n_cols = cJSON_GetArraySize(cols);
      for (int i = 0; i < n_cols; i++) {
        cJSON *item = cJSON_GetArrayItem(cols, i);
        if (!cJSON_IsString(item)) {
          sb_free(&qres);
          sb_free(&partition);
          *error = strdup("rowid: columns must be strings");
          return -1;
        }
        strbuf qcol;
        sb_init(&qcol);
        sql_quote_ident(&qcol, item->valuestring);
        if (i > 0) sb_append(&partition, ", ");
        sb_append(&partition, qcol.data);
        sb_free(&qcol);
      }
    }

    if (partition.len > 0) {
      sb_appendf(sb, "%s AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY _rn) AS %s FROM %s)",
                 cte_name, partition.data, qres.data, prev);
    } else {
      sb_appendf(sb, "%s AS (SELECT *, ROW_NUMBER() OVER (ORDER BY _rn) AS %s FROM %s)",
                 cte_name, qres.data, prev);
    }
    sb_free(&qres);
    sb_free(&partition);
    return 0;
  }

  /* ---- rleid ---- */
  if (strcmp(op, "rleid") == 0) {
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    const char *result = jstr(args, "result");
    if (!result) result = "_rleid";
    if (!cJSON_IsArray(cols) || cJSON_GetArraySize(cols) <= 0) {
      *error = strdup("rleid: missing 'columns'");
      return -1;
    }

    strbuf qres;
    sb_init(&qres);
    sql_quote_ident(&qres, result);

    strbuf change;
    sb_init(&change);
    int n_cols = cJSON_GetArraySize(cols);
    for (int i = 0; i < n_cols; i++) {
      cJSON *item = cJSON_GetArrayItem(cols, i);
      if (!cJSON_IsString(item)) {
        sb_free(&qres);
        sb_free(&change);
        *error = strdup("rleid: columns must be strings");
        return -1;
      }
      strbuf qcol;
      sb_init(&qcol);
      sql_quote_ident(&qcol, item->valuestring);
      if (i > 0) sb_append(&change, " OR ");
      sb_appendf(&change, "%s IS DISTINCT FROM LAG(%s) OVER (ORDER BY _rn)",
                 qcol.data, qcol.data);
      sb_free(&qcol);
    }

    sb_appendf(sb, "%s AS (SELECT * EXCLUDE (__tf_rleid_changed, __tf_rleid_ord), "
                   "SUM(__tf_rleid_changed) OVER (ORDER BY __tf_rleid_ord ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS %s "
                   "FROM (SELECT *, _rn AS __tf_rleid_ord, "
                   "CASE WHEN ROW_NUMBER() OVER (ORDER BY _rn) = 1 OR %s THEN 1 ELSE 0 END AS __tf_rleid_changed "
                   "FROM %s) __tf_rleid_src)",
               cte_name, qres.data, change.data, prev);
    sb_free(&qres);
    sb_free(&change);
    return 0;
  }

  /* ---- lead / lag / shift ---- */
  if (strcmp(op, "lead") == 0 || strcmp(op, "lag") == 0 || strcmp(op, "shift") == 0) {
    const char *column = jstr(args, "column");
    int offset = jint(args, "offset", 1);
    const char *result = jstr(args, "result");
    const char *type = jstr(args, "type");
    const char *fn = "LAG";
    const char *suffix = "lag";
    if (strcmp(op, "lead") == 0 || (strcmp(op, "shift") == 0 && type && strcmp(type, "lead") == 0)) {
      fn = "LEAD";
      suffix = "lead";
    } else if (strcmp(op, "shift") == 0) {
      suffix = "shift";
    }
    if (!column) { *error = strdup("shift: missing 'column'"); return -1; }
    if (offset <= 0) offset = 1;
    strbuf qcol, qres;
    sb_init(&qcol); sb_init(&qres);
    sql_quote_ident(&qcol, column);
    if (result) sql_quote_ident(&qres, result);
    else sb_appendf(&qres, "\"%s_%s\"", column, suffix);
    sb_appendf(sb, "%s AS (SELECT *, %s(%s, %d) OVER (ORDER BY _rn) AS %s FROM %s)",
               cte_name, fn, qcol.data, offset, qres.data, prev);
    sb_free(&qcol); sb_free(&qres);
    return 0;
  }

  /* ---- datetime ---- */
  if (strcmp(op, "datetime") == 0) {
    const char *column = jstr(args, "column");
    cJSON *extract = cJSON_GetObjectItemCaseSensitive(args, "extract");
    if (!column) { *error = strdup("datetime: missing 'column'"); return -1; }
    strbuf qcol;
    sb_init(&qcol);
    sql_quote_ident(&qcol, column);
    strbuf der;
    sb_init(&der);
    if (extract && cJSON_IsArray(extract)) {
      int n = cJSON_GetArraySize(extract);
      for (int i = 0; i < n; i++) {
        cJSON *part = cJSON_GetArrayItem(extract, i);
        if (!cJSON_IsString(part)) continue;
        const char *p = part->valuestring;
        sb_appendf(&der, ", EXTRACT(%s FROM %s::TIMESTAMP) AS ", p, qcol.data);
        sql_quote_joined_ident(&der, column, "_", p);
      }
    }
    sb_appendf(sb, "%s AS (SELECT *%s FROM %s)", cte_name, der.data, prev);
    sb_free(&qcol); sb_free(&der);
    return 0;
  }

  /* ---- date-trunc ---- */
  if (strcmp(op, "date-trunc") == 0) {
    const char *column = jstr(args, "column");
    const char *trunc = jstr(args, "trunc");
    const char *result = jstr(args, "result");
    if (!column || !trunc) { *error = strdup("date-trunc: missing args"); return -1; }
    strbuf qcol;
    sb_init(&qcol);
    sql_quote_ident(&qcol, column);
    strbuf qres;
    sb_init(&qres);
    int in_place = result == NULL;
    sql_quote_ident(&qres, result ? result : column);
    if (in_place) {
      sb_appendf(sb, "%s AS (SELECT * REPLACE (date_trunc('%s', %s::TIMESTAMP) AS %s) FROM %s)",
                 cte_name, trunc, qcol.data, qres.data, prev);
    } else {
      sb_appendf(sb, "%s AS (SELECT *, date_trunc('%s', %s::TIMESTAMP) AS %s FROM %s)",
                 cte_name, trunc, qcol.data, qres.data, prev);
    }
    sb_free(&qcol); sb_free(&qres);
    return 0;
  }

  /* ---- stats ---- */
  if (strcmp(op, "stats") == 0) {
    *error = strdup("stats: native report shape cannot be lowered to SQL");
    return -1;
  }

  /* ---- flatten ---- */
  if (strcmp(op, "flatten") == 0) {
    sb_appendf(sb, "%s AS (SELECT * FROM %s)", cte_name, prev);
    return 0;
  }

  /* Unknown op */
  char buf[256];
  snprintf(buf, sizeof(buf), "unsupported op for SQL: '%s'", op);
  *error = strdup(buf);
  return -1;
}

/* ---- Main transpiler ---- */

char *tf_ir_to_sql(const tf_ir_plan *plan, char **error) {
  if (!plan || plan->n_nodes == 0) {
    if (error) *error = strdup("empty plan");
    return NULL;
  }

  if (error) *error = NULL;

  strbuf sb;
  sb_init(&sb);

  /* Find decoder (first node) and encoder (last node) */
  size_t first_transform = 0;
  size_t last_transform = plan->n_nodes;
  const char *input_source = "input_data";

  /* Check first node for decoder — extract as metadata */
  const tf_ir_node *first = &plan->nodes[0];
  if (strncmp(first->op, "codec.", 6) == 0 &&
      strstr(first->op, ".decode") != NULL) {
    first_transform = 1;
  }

  /* Check last node for encoder — skip it */
  if (plan->n_nodes > 1) {
    const tf_ir_node *last = &plan->nodes[plan->n_nodes - 1];
    if (strncmp(last->op, "codec.", 6) == 0 &&
        strstr(last->op, ".encode") != NULL) {
      last_transform = plan->n_nodes - 1;
    }
  }

  /* No transforms — just SELECT * */
  if (first_transform >= last_transform) {
    sb_appendf(&sb, "SELECT * FROM %s", input_source);
    char *sql = sb_detach(&sb);
    if (!sql && error && !*error) *error = strdup("sql: out of memory");
    return sql;
  }

  /* Check if any operator needs row ordering (_rn column) */
  int needs_rn = 0;
  for (size_t i = first_transform; i < last_transform; i++) {
    const char *op = plan->nodes[i].op;
    if (strcmp(op, "window") == 0 || strcmp(op, "rolling-sum") == 0 ||
        strcmp(op, "rolling-mean") == 0 || strcmp(op, "rolling-min") == 0 ||
        strcmp(op, "rolling-max") == 0 || strcmp(op, "rolling-any") == 0 ||
        strcmp(op, "rolling-all") == 0 || strcmp(op, "step") == 0 ||
        strcmp(op, "lead") == 0 || strcmp(op, "lag") == 0 ||
        strcmp(op, "shift") == 0 || strcmp(op, "rowid") == 0 ||
        strcmp(op, "rleid") == 0 || strcmp(op, "fill-down") == 0) {
      needs_rn = 1;
      break;
    }
  }

  /* Build CTE chain — use two alternating name buffers so prev != current */
  int n_ctes = 0;
  char name_bufs[2][32];
  const char *prev = input_source;
  char *err = NULL;

  sb_append(&sb, "WITH\n");

  /* If row ordering is needed, inject a _rn column from the source */
  if (needs_rn) {
    sb_appendf(&sb, "  _numbered AS (SELECT *, ROW_NUMBER() OVER () AS _rn FROM %s)", input_source);
    prev = "_numbered";
    n_ctes++;
  }

  for (size_t i = first_transform; i < last_transform; i++) {
    const tf_ir_node *node = &plan->nodes[i];
    char *cte_name = name_bufs[n_ctes % 2];
    snprintf(cte_name, 32, "step_%zu", i);

    if (n_ctes > 0) sb_append(&sb, ",\n");
    sb_append(&sb, "  ");

    if (emit_cte(&sb, cte_name, prev, node->op, node->args, &err) != 0) {
      sb_free(&sb);
      if (error) *error = err;
      else free(err);
      return NULL;
    }
    if (sb.failed) {
      sb_free(&sb);
      if (error) *error = strdup("sql: out of memory");
      return NULL;
    }

    prev = cte_name;
    n_ctes++;
  }

  /* Final SELECT from last CTE — exclude internal _rn column */
  if (needs_rn) {
    sb_appendf(&sb, "\nSELECT * EXCLUDE (_rn) FROM %s", prev);
  } else {
    sb_appendf(&sb, "\nSELECT * FROM %s", prev);
  }

  char *sql = sb_detach(&sb);
  if (!sql && error && !*error) *error = strdup("sql: out of memory");
  return sql;
}
