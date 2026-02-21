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
} strbuf;

static void sb_init(strbuf *sb) {
  sb->data = malloc(256);
  sb->len = 0;
  sb->cap = 256;
  if (sb->data) sb->data[0] = '\0';
}

static void sb_ensure(strbuf *sb, size_t extra) {
  if (sb->len + extra + 1 > sb->cap) {
    size_t newcap = sb->cap * 2;
    if (newcap < sb->len + extra + 1) newcap = sb->len + extra + 1;
    char *nd = realloc(sb->data, newcap);
    if (nd) { sb->data = nd; sb->cap = newcap; }
  }
}

static void sb_append(strbuf *sb, const char *s) {
  size_t n = strlen(s);
  sb_ensure(sb, n);
  memcpy(sb->data + sb->len, s, n);
  sb->len += n;
  sb->data[sb->len] = '\0';
}

static void sb_appendn(strbuf *sb, const char *s, size_t n) {
  sb_ensure(sb, n);
  memcpy(sb->data + sb->len, s, n);
  sb->len += n;
  sb->data[sb->len] = '\0';
}

static void sb_appendf(strbuf *sb, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

static void sb_appendf(strbuf *sb, const char *fmt, ...) {
  char buf[512];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  if (n > 0) sb_appendn(sb, buf, (size_t)n);
}

static char *sb_detach(strbuf *sb) {
  char *s = sb->data;
  sb->data = NULL;
  sb->len = sb->cap = 0;
  return s;
}

static void sb_free(strbuf *sb) {
  free(sb->data);
  sb->data = NULL;
  sb->len = sb->cap = 0;
}

/* ---- Expression AST to SQL ---- */

/* Append a SQL-quoted identifier: "name" */
static void sql_quote_ident(strbuf *sb, const char *name) {
  sb_append(sb, "\"");
  for (const char *p = name; *p; p++) {
    if (*p == '"') sb_append(sb, "\"\"");
    else sb_appendn(sb, p, 1);
  }
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
      sb_appendf(sb, "%g", e->lit_float);
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
  if (rc != 0) { sb_free(&sb); return NULL; }
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

  /* ---- select / reorder ---- */
  if (strcmp(op, "select") == 0 || strcmp(op, "reorder") == 0) {
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cols || !cJSON_IsArray(cols)) { *error = strdup("select: missing 'columns'"); return -1; }
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
  if (strcmp(op, "head") == 0) {
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
  if (strcmp(op, "tail") == 0) {
    int n = jint(args, "n", 10);
    sb_appendf(sb, "%s AS (SELECT * FROM (SELECT *, ROW_NUMBER() OVER () AS _rn, "
               "COUNT(*) OVER () AS _total FROM %s) WHERE _rn > _total - %d)",
               cte_name, prev, n);
    return 0;
  }

  /* ---- top ---- */
  if (strcmp(op, "top") == 0) {
    int n = jint(args, "n", 10);
    const char *column = jstr(args, "column");
    int desc = jbool(args, "desc", 1);
    if (!column) { *error = strdup("top: missing 'column'"); return -1; }
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
    int n = jint(args, "n", 100);
    sb_appendf(sb, "%s AS (SELECT * FROM %s USING SAMPLE %d)", cte_name, prev, n);
    return 0;
  }

  /* ---- grep ---- */
  if (strcmp(op, "grep") == 0) {
    const char *pattern = jstr(args, "pattern");
    const char *column = jstr(args, "column");
    int invert = jbool(args, "invert", 0);
    int regex = jbool(args, "regex", 0);
    if (!pattern) { *error = strdup("grep: missing 'pattern'"); return -1; }
    if (!column) column = "_line";
    strbuf tmp;
    sb_init(&tmp);
    sql_quote_ident(&tmp, column);
    if (regex) {
      sb_appendf(sb, "%s AS (SELECT * FROM %s WHERE %sregexp_matches(%s, ",
                 cte_name, prev, invert ? "NOT " : "", tmp.data);
      sql_quote_str(sb, pattern);
      sb_append(sb, "))");
    } else {
      sb_appendf(sb, "%s AS (SELECT * FROM %s WHERE ", cte_name, prev);
      if (invert) sb_append(sb, "NOT ");
      sb_append(sb, tmp.data);
      sb_append(sb, " LIKE '%");
      /* Escape pattern for LIKE */
      for (const char *p = pattern; *p; p++) {
        if (*p == '%' || *p == '_' || *p == '\\') sb_appendn(sb, "\\", 1);
        if (*p == '\'') sb_append(sb, "''");
        else sb_appendn(sb, p, 1);
      }
      sb_append(sb, "%')");
    }
    sb_free(&tmp);
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
      sb_appendf(&expr, "GREATEST(%g, LEAST(%g, %s))", min_v->valuedouble, max_v->valuedouble, tmp.data);
      sb_free(&tmp);
    } else if (min_v) {
      strbuf tmp;
      sb_init(&tmp);
      sql_quote_ident(&tmp, column);
      sb_init(&expr);
      sb_appendf(&expr, "GREATEST(%g, %s)", min_v->valuedouble, tmp.data);
      sb_free(&tmp);
    } else if (max_v) {
      strbuf tmp;
      sb_init(&tmp);
      sql_quote_ident(&tmp, column);
      sb_init(&expr);
      sb_appendf(&expr, "LEAST(%g, %s)", max_v->valuedouble, tmp.data);
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
    if (cols && cJSON_IsArray(cols) && cJSON_GetArraySize(cols) > 0) {
      strbuf rep;
      sb_init(&rep);
      int n = cJSON_GetArraySize(cols);
      for (int i = 0; i < n; i++) {
        if (i > 0) sb_append(&rep, ", ");
        cJSON *c = cJSON_GetArrayItem(cols, i);
        if (!cJSON_IsString(c)) continue;
        sb_append(&rep, "trim(");
        sql_quote_ident(&rep, c->valuestring);
        sb_append(&rep, ") AS ");
        sql_quote_ident(&rep, c->valuestring);
      }
      sb_appendf(sb, "%s AS (SELECT * REPLACE (%s) FROM %s)", cte_name, rep.data, prev);
      sb_free(&rep);
    } else {
      /* Trim all string columns — without schema we just pass through.
       * DuckDB doesn't have a "trim all" so we'd need column names.
       * Fallback: SELECT * (no trim) with a comment. */
      sb_appendf(sb, "%s AS (SELECT * FROM %s)", cte_name, prev);
    }
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
      sql_quote_ident(&sel, col);
      sb_append(&sel, ") AS ");
      if (result) sql_quote_ident(&sel, result);
      else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_%s", func, col);
        sql_quote_ident(&sel, buf);
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
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (cols && cJSON_IsArray(cols) && cJSON_GetArraySize(cols) > 0) {
      strbuf sel;
      sb_init(&sel);
      strbuf grp;
      sb_init(&grp);
      int n = cJSON_GetArraySize(cols);
      for (int i = 0; i < n; i++) {
        if (i > 0) { sb_append(&sel, ", "); sb_append(&grp, ", "); }
        cJSON *c = cJSON_GetArrayItem(cols, i);
        if (cJSON_IsString(c)) {
          sql_quote_ident(&sel, c->valuestring);
          sql_quote_ident(&grp, c->valuestring);
        }
      }
      sb_appendf(sb, "%s AS (SELECT %s, COUNT(*) AS \"count\" FROM %s GROUP BY %s ORDER BY \"count\" DESC)",
                 cte_name, sel.data, prev, grp.data);
      sb_free(&sel);
      sb_free(&grp);
    } else {
      /* No columns specified — frequency of all columns not meaningful,
       * fall back to counting rows */
      sb_appendf(sb, "%s AS (SELECT COUNT(*) AS \"count\" FROM %s)", cte_name, prev);
    }
    return 0;
  }

  /* ---- join ---- */
  if (strcmp(op, "join") == 0) {
    const char *file = jstr(args, "file");
    const char *on = jstr(args, "on");
    const char *how = jstr(args, "how");
    if (!file || !on) { *error = strdup("join: missing 'file' or 'on'"); return -1; }
    if (!how) how = "inner";
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
    sb_appendf(sb, "%s AS (SELECT a.* FROM %s a %s JOIN read_csv_auto(",
               cte_name, prev, join_type);
    sql_quote_str(sb, file);
    sb_appendf(sb, ") b ON %s)", cond.data);
    sb_free(&cond);
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
        sb_appendf(&expr, " WHEN %s < %g THEN '<%g'", qcol.data, val, val);
      }
      if (i > 0) {
        cJSON *prev_b = cJSON_GetArrayItem(boundaries, i - 1);
        sb_appendf(&expr, " WHEN %s >= %g AND %s < %g THEN '%g-%g'",
                   qcol.data, prev_b->valuedouble, qcol.data, val,
                   prev_b->valuedouble, val);
      }
    }
    if (n > 0) {
      cJSON *last = cJSON_GetArrayItem(boundaries, n - 1);
      sb_appendf(&expr, " WHEN %s >= %g THEN '>=%g'", qcol.data, last->valuedouble, last->valuedouble);
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
        sb_appendf(&rep, "LAST_VALUE(%s IGNORE NULLS) OVER (ORDER BY rowid() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS %s",
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

  /* ---- window ---- */
  if (strcmp(op, "window") == 0) {
    const char *column = jstr(args, "column");
    int size = jint(args, "size", 3);
    const char *func = jstr(args, "func");
    const char *result = jstr(args, "result");
    if (!column || !func) { *error = strdup("window: missing args"); return -1; }
    const char *sql_func = "AVG";
    if (strcmp(func, "sum") == 0) sql_func = "SUM";
    else if (strcmp(func, "min") == 0) sql_func = "MIN";
    else if (strcmp(func, "max") == 0) sql_func = "MAX";
    else if (strcmp(func, "avg") == 0) sql_func = "AVG";
    strbuf qcol, qres;
    sb_init(&qcol); sb_init(&qres);
    sql_quote_ident(&qcol, column);
    if (result) sql_quote_ident(&qres, result);
    else sb_appendf(&qres, "\"%s_%s_%d\"", func, column, size);
    sb_appendf(sb, "%s AS (SELECT *, %s(%s) OVER (ORDER BY rowid() ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS %s FROM %s)",
               cte_name, sql_func, qcol.data, size - 1, qres.data, prev);
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
    sb_appendf(sb, "%s AS (SELECT *, %s(%s) OVER (ORDER BY rowid() ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS %s FROM %s)",
               cte_name, sql_func, qcol.data, qres.data, prev);
    sb_free(&qcol); sb_free(&qres);
    return 0;
  }

  /* ---- lead ---- */
  if (strcmp(op, "lead") == 0) {
    const char *column = jstr(args, "column");
    int offset = jint(args, "offset", 1);
    const char *result = jstr(args, "result");
    if (!column) { *error = strdup("lead: missing 'column'"); return -1; }
    strbuf qcol, qres;
    sb_init(&qcol); sb_init(&qres);
    sql_quote_ident(&qcol, column);
    if (result) sql_quote_ident(&qres, result);
    else sb_appendf(&qres, "\"%s_lead_%d\"", column, offset);
    sb_appendf(sb, "%s AS (SELECT *, LEAD(%s, %d) OVER (ORDER BY rowid()) AS %s FROM %s)",
               cte_name, qcol.data, offset, qres.data, prev);
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
        char result_name[128];
        snprintf(result_name, sizeof(result_name), "%s_%s", column, p);
        sb_appendf(&der, ", EXTRACT(%s FROM %s::TIMESTAMP) AS ", p, qcol.data);
        sql_quote_ident(&der, result_name);
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
    if (result) sql_quote_ident(&qres, result);
    else {
      char buf[128];
      snprintf(buf, sizeof(buf), "%s_%s", column, trunc);
      sql_quote_ident(&qres, buf);
    }
    sb_appendf(sb, "%s AS (SELECT *, date_trunc('%s', %s::TIMESTAMP) AS %s FROM %s)",
               cte_name, trunc, qcol.data, qres.data, prev);
    sb_free(&qcol); sb_free(&qres);
    return 0;
  }

  /* ---- stats ---- */
  if (strcmp(op, "stats") == 0) {
    /* Stats uses DuckDB's SUMMARIZE — works without knowing column names */
    sb_appendf(sb, "%s AS (SELECT * FROM (SUMMARIZE SELECT * FROM %s))", cte_name, prev);
    return 0;
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
    return sb_detach(&sb);
  }

  /* Build CTE chain — use two alternating name buffers so prev != current */
  int n_ctes = 0;
  char name_bufs[2][32];
  const char *prev = input_source;
  char *err = NULL;

  sb_append(&sb, "WITH\n");

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

    prev = cte_name;
    n_ctes++;
  }

  /* Final SELECT from last CTE */
  sb_appendf(&sb, "\nSELECT * FROM %s", prev);

  return sb_detach(&sb);
}
