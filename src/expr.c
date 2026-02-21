/*
 * expr.c — Expression parser and evaluator for filter and derive operations.
 *
 * Grammar:
 *   expr     = or_expr
 *   or_expr  = and_expr ('or' and_expr)*
 *   and_expr = not_expr ('and' not_expr)*
 *   not_expr = 'not' not_expr | cmp_expr
 *   cmp_expr = add_expr (cmp_op add_expr)?
 *   cmp_op   = '>' | '>=' | '<' | '<=' | '==' | '!='
 *   add_expr = mul_expr (('+' | '-') mul_expr)*
 *   mul_expr = unary (('*' | '/') unary)*
 *   unary    = '-' unary | atom
 *   atom     = NUMBER | STRING | col_ref | '(' expr ')'
 *   col_ref  = 'col(' STRING ')'
 *   NUMBER   = [0-9]+('.'[0-9]+)?
 *   STRING   = '\'' [^']* '\'' | '"' [^"]* '"'
 */

#include "internal.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <math.h>

/* ---- AST node types ---- */

typedef enum {
    EXPR_LIT_INT,
    EXPR_LIT_FLOAT,
    EXPR_LIT_STR,
    EXPR_COL_REF,
    EXPR_CMP,
    EXPR_AND,
    EXPR_OR,
    EXPR_NOT,
    EXPR_ADD,
    EXPR_SUB,
    EXPR_MUL,
    EXPR_DIV,
    EXPR_NEG,
    EXPR_FUNC_CALL,
} expr_kind;

typedef enum {
    CMP_GT, CMP_GE, CMP_LT, CMP_LE, CMP_EQ, CMP_NE,
} cmp_op;

struct tf_expr {
    expr_kind kind;
    union {
        int64_t lit_int;
        double  lit_float;
        char   *lit_str;
        char   *col_name;    /* for EXPR_COL_REF */
        struct {              /* for EXPR_CMP */
            struct tf_expr *left;
            struct tf_expr *right;
            cmp_op op;
        } cmp;
        struct {              /* for EXPR_AND, EXPR_OR, EXPR_ADD, EXPR_SUB, EXPR_MUL, EXPR_DIV */
            struct tf_expr *left;
            struct tf_expr *right;
        } binary;
        struct tf_expr *child; /* for EXPR_NOT, EXPR_NEG */
        struct {              /* for EXPR_FUNC_CALL */
            char *name;
            struct tf_expr **args;
            int n_args;
        } func;
    };
};

/* ---- Lexer ---- */

typedef struct {
    const char *src;
    size_t      pos;
    size_t      len;
} parser_state;

static void skip_ws(parser_state *p) {
    while (p->pos < p->len && isspace((unsigned char)p->src[p->pos]))
        p->pos++;
}

static int match_char(parser_state *p, char c) {
    skip_ws(p);
    if (p->pos < p->len && p->src[p->pos] == c) {
        p->pos++;
        return 1;
    }
    return 0;
}

static int match_keyword(parser_state *p, const char *kw) {
    skip_ws(p);
    size_t kwlen = strlen(kw);
    if (p->pos + kwlen <= p->len &&
        strncmp(p->src + p->pos, kw, kwlen) == 0 &&
        (p->pos + kwlen == p->len || !isalnum((unsigned char)p->src[p->pos + kwlen]))) {
        p->pos += kwlen;
        return 1;
    }
    return 0;
}

static int peek_cmp_op(parser_state *p, cmp_op *op) {
    skip_ws(p);
    size_t pos = p->pos;
    if (pos >= p->len) return 0;

    if (pos + 1 < p->len) {
        if (p->src[pos] == '>' && p->src[pos + 1] == '=') { *op = CMP_GE; return 1; }
        if (p->src[pos] == '<' && p->src[pos + 1] == '=') { *op = CMP_LE; return 1; }
        if (p->src[pos] == '=' && p->src[pos + 1] == '=') { *op = CMP_EQ; return 1; }
        if (p->src[pos] == '!' && p->src[pos + 1] == '=') { *op = CMP_NE; return 1; }
    }
    if (p->src[pos] == '>') { *op = CMP_GT; return 1; }
    if (p->src[pos] == '<') { *op = CMP_LT; return 1; }
    return 0;
}

static void consume_cmp_op(parser_state *p, cmp_op op) {
    skip_ws(p);
    switch (op) {
        case CMP_GE: case CMP_LE: case CMP_EQ: case CMP_NE:
            p->pos += 2; break;
        case CMP_GT: case CMP_LT:
            p->pos += 1; break;
    }
}

/* ---- Recursive descent parser ---- */

static tf_expr *parse_expr(parser_state *p);

static tf_expr *make_expr(expr_kind kind) {
    tf_expr *e = calloc(1, sizeof(tf_expr));
    if (e) e->kind = kind;
    return e;
}

static tf_expr *parse_string_literal(parser_state *p) {
    skip_ws(p);
    char quote = p->src[p->pos];
    if (quote != '\'' && quote != '"') return NULL;
    p->pos++; /* skip opening quote */

    size_t start = p->pos;
    while (p->pos < p->len && p->src[p->pos] != quote)
        p->pos++;
    if (p->pos >= p->len) return NULL; /* unterminated */

    size_t slen = p->pos - start;
    p->pos++; /* skip closing quote */

    tf_expr *e = make_expr(EXPR_LIT_STR);
    if (!e) return NULL;
    e->lit_str = malloc(slen + 1);
    memcpy(e->lit_str, p->src + start, slen);
    e->lit_str[slen] = '\0';
    return e;
}

/* Parse a comma-separated argument list inside parentheses.
 * Assumes '(' has already been consumed. Consumes the closing ')'.
 * Returns allocated array of expressions, sets *n_args. */
static tf_expr **parse_arg_list(parser_state *p, int *n_args) {
    *n_args = 0;
    int cap = 4;
    tf_expr **args = malloc(cap * sizeof(tf_expr *));
    if (!args) return NULL;

    skip_ws(p);
    /* Empty arg list: func() */
    if (p->pos < p->len && p->src[p->pos] == ')') {
        p->pos++;
        return args;
    }

    for (;;) {
        tf_expr *arg = parse_expr(p);
        if (!arg) goto fail;
        if (*n_args >= cap) {
            cap *= 2;
            tf_expr **na = realloc(args, cap * sizeof(tf_expr *));
            if (!na) { tf_expr_free(arg); goto fail; }
            args = na;
        }
        args[(*n_args)++] = arg;
        skip_ws(p);
        if (p->pos < p->len && p->src[p->pos] == ',') {
            p->pos++;
            continue;
        }
        break;
    }
    if (!match_char(p, ')')) goto fail;
    return args;

fail:
    for (int i = 0; i < *n_args; i++) tf_expr_free(args[i]);
    free(args);
    *n_args = 0;
    return NULL;
}

static tf_expr *parse_atom(parser_state *p) {
    skip_ws(p);
    if (p->pos >= p->len) return NULL;

    /* Parenthesized expression */
    if (p->src[p->pos] == '(') {
        p->pos++;
        tf_expr *e = parse_expr(p);
        if (!e) return NULL;
        if (!match_char(p, ')')) { tf_expr_free(e); return NULL; }
        return e;
    }

    /* String literal */
    if (p->src[p->pos] == '\'' || p->src[p->pos] == '"') {
        return parse_string_literal(p);
    }

    /* Number (but NOT negative — that's handled in parse_unary) */
    if (isdigit((unsigned char)p->src[p->pos])) {
        size_t start = p->pos;
        while (p->pos < p->len && isdigit((unsigned char)p->src[p->pos])) p->pos++;
        int is_float = 0;
        if (p->pos < p->len && p->src[p->pos] == '.') {
            is_float = 1;
            p->pos++;
            while (p->pos < p->len && isdigit((unsigned char)p->src[p->pos])) p->pos++;
        }
        /* Also handle e/E notation */
        if (p->pos < p->len && (p->src[p->pos] == 'e' || p->src[p->pos] == 'E')) {
            is_float = 1;
            p->pos++;
            if (p->pos < p->len && (p->src[p->pos] == '+' || p->src[p->pos] == '-')) p->pos++;
            while (p->pos < p->len && isdigit((unsigned char)p->src[p->pos])) p->pos++;
        }

        size_t numlen = p->pos - start;
        char *numstr = malloc(numlen + 1);
        memcpy(numstr, p->src + start, numlen);
        numstr[numlen] = '\0';

        tf_expr *e;
        if (is_float) {
            e = make_expr(EXPR_LIT_FLOAT);
            if (e) e->lit_float = strtod(numstr, NULL);
        } else {
            e = make_expr(EXPR_LIT_INT);
            if (e) e->lit_int = strtoll(numstr, NULL, 10);
        }
        free(numstr);
        return e;
    }

    /* Identifier: could be col(), function call, or keyword */
    if (isalpha((unsigned char)p->src[p->pos]) || p->src[p->pos] == '_') {
        size_t start = p->pos;
        while (p->pos < p->len &&
               (isalnum((unsigned char)p->src[p->pos]) || p->src[p->pos] == '_'))
            p->pos++;
        size_t id_len = p->pos - start;

        /* Check for keywords — don't consume them here, let caller handle */
        /* "and", "or", "not" are handled at higher precedence levels */
        if ((id_len == 3 && strncmp(p->src + start, "and", 3) == 0) ||
            (id_len == 2 && strncmp(p->src + start, "or", 2) == 0) ||
            (id_len == 3 && strncmp(p->src + start, "not", 3) == 0)) {
            p->pos = start; /* rewind */
            return NULL;
        }

        char *name = malloc(id_len + 1);
        if (!name) return NULL;
        memcpy(name, p->src + start, id_len);
        name[id_len] = '\0';

        skip_ws(p);
        if (p->pos < p->len && p->src[p->pos] == '(') {
            p->pos++; /* consume '(' */

            /* Special case: col() — column reference */
            if (strcmp(name, "col") == 0) {
                free(name);
                skip_ws(p);
                if (p->pos >= p->len) return NULL;

                char *col_name = NULL;
                if (p->src[p->pos] == '\'' || p->src[p->pos] == '"') {
                    tf_expr *str = parse_string_literal(p);
                    if (!str) return NULL;
                    col_name = str->lit_str;
                    str->lit_str = NULL;
                    tf_expr_free(str);
                } else {
                    size_t cstart = p->pos;
                    while (p->pos < p->len &&
                           (isalnum((unsigned char)p->src[p->pos]) || p->src[p->pos] == '_'))
                        p->pos++;
                    if (p->pos == cstart) return NULL;
                    col_name = malloc(p->pos - cstart + 1);
                    if (!col_name) return NULL;
                    memcpy(col_name, p->src + cstart, p->pos - cstart);
                    col_name[p->pos - cstart] = '\0';
                }

                if (!match_char(p, ')')) { free(col_name); return NULL; }
                tf_expr *e = make_expr(EXPR_COL_REF);
                if (!e) { free(col_name); return NULL; }
                e->col_name = col_name;
                return e;
            }

            /* General function call: name(arg1, arg2, ...) */
            int n_args = 0;
            tf_expr **args = parse_arg_list(p, &n_args);
            if (!args) { free(name); return NULL; }

            tf_expr *e = make_expr(EXPR_FUNC_CALL);
            if (!e) {
                for (int i = 0; i < n_args; i++) tf_expr_free(args[i]);
                free(args);
                free(name);
                return NULL;
            }
            e->func.name = name;
            e->func.args = args;
            e->func.n_args = n_args;
            return e;
        }

        /* Bare identifier without parens — not valid in our grammar */
        free(name);
        p->pos = start; /* rewind */
        return NULL;
    }

    return NULL; /* parse error */
}

/* Unary minus: -expr */
static tf_expr *parse_unary(parser_state *p) {
    skip_ws(p);
    if (p->pos < p->len && p->src[p->pos] == '-') {
        /* Check it's not a comparison op like != */
        p->pos++;
        tf_expr *child = parse_unary(p);
        if (!child) return NULL;
        /* Optimize: fold literal negation */
        if (child->kind == EXPR_LIT_INT) {
            child->lit_int = -child->lit_int;
            return child;
        }
        if (child->kind == EXPR_LIT_FLOAT) {
            child->lit_float = -child->lit_float;
            return child;
        }
        tf_expr *e = make_expr(EXPR_NEG);
        if (!e) { tf_expr_free(child); return NULL; }
        e->child = child;
        return e;
    }
    return parse_atom(p);
}

/* Multiplicative: unary (('*' | '/') unary)* */
static tf_expr *parse_mul(parser_state *p) {
    tf_expr *left = parse_unary(p);
    if (!left) return NULL;

    for (;;) {
        skip_ws(p);
        if (p->pos >= p->len) break;
        char op_char = p->src[p->pos];
        if (op_char != '*' && op_char != '/') break;
        p->pos++;

        tf_expr *right = parse_unary(p);
        if (!right) { tf_expr_free(left); return NULL; }

        expr_kind kind = (op_char == '*') ? EXPR_MUL : EXPR_DIV;
        tf_expr *e = make_expr(kind);
        if (!e) { tf_expr_free(left); tf_expr_free(right); return NULL; }
        e->binary.left = left;
        e->binary.right = right;
        left = e;
    }
    return left;
}

/* Additive: mul_expr (('+' | '-') mul_expr)* */
static tf_expr *parse_add(parser_state *p) {
    tf_expr *left = parse_mul(p);
    if (!left) return NULL;

    for (;;) {
        skip_ws(p);
        if (p->pos >= p->len) break;
        char op_char = p->src[p->pos];
        if (op_char != '+' && op_char != '-') break;
        /* Disambiguate: '-' followed by digit could be subtraction.
         * But we need to avoid consuming '-' that's part of a comparison like '<=' etc.
         * Since we're at the add level, '+' and '-' are always arithmetic. */
        p->pos++;

        tf_expr *right = parse_mul(p);
        if (!right) { tf_expr_free(left); return NULL; }

        expr_kind kind = (op_char == '+') ? EXPR_ADD : EXPR_SUB;
        tf_expr *e = make_expr(kind);
        if (!e) { tf_expr_free(left); tf_expr_free(right); return NULL; }
        e->binary.left = left;
        e->binary.right = right;
        left = e;
    }
    return left;
}

static tf_expr *parse_cmp(parser_state *p) {
    tf_expr *left = parse_add(p);
    if (!left) return NULL;

    cmp_op op;
    if (peek_cmp_op(p, &op)) {
        consume_cmp_op(p, op);
        tf_expr *right = parse_add(p);
        if (!right) { tf_expr_free(left); return NULL; }

        tf_expr *e = make_expr(EXPR_CMP);
        if (!e) { tf_expr_free(left); tf_expr_free(right); return NULL; }
        e->cmp.left = left;
        e->cmp.right = right;
        e->cmp.op = op;
        return e;
    }

    return left;
}

static tf_expr *parse_not(parser_state *p) {
    if (match_keyword(p, "not")) {
        tf_expr *child = parse_not(p);
        if (!child) return NULL;
        tf_expr *e = make_expr(EXPR_NOT);
        if (!e) { tf_expr_free(child); return NULL; }
        e->child = child;
        return e;
    }
    return parse_cmp(p);
}

static tf_expr *parse_and(parser_state *p) {
    tf_expr *left = parse_not(p);
    if (!left) return NULL;
    while (match_keyword(p, "and")) {
        tf_expr *right = parse_not(p);
        if (!right) { tf_expr_free(left); return NULL; }
        tf_expr *e = make_expr(EXPR_AND);
        if (!e) { tf_expr_free(left); tf_expr_free(right); return NULL; }
        e->binary.left = left;
        e->binary.right = right;
        left = e;
    }
    return left;
}

static tf_expr *parse_or(parser_state *p) {
    tf_expr *left = parse_and(p);
    if (!left) return NULL;
    while (match_keyword(p, "or")) {
        tf_expr *right = parse_and(p);
        if (!right) { tf_expr_free(left); return NULL; }
        tf_expr *e = make_expr(EXPR_OR);
        if (!e) { tf_expr_free(left); tf_expr_free(right); return NULL; }
        e->binary.left = left;
        e->binary.right = right;
        left = e;
    }
    return left;
}

static tf_expr *parse_expr(parser_state *p) {
    return parse_or(p);
}

tf_expr *tf_expr_parse(const char *text) {
    if (!text) return NULL;
    parser_state p = { .src = text, .pos = 0, .len = strlen(text) };
    tf_expr *e = parse_expr(&p);
    if (!e) return NULL;
    /* Ensure we consumed all input */
    skip_ws(&p);
    if (p.pos < p.len) {
        tf_expr_free(e);
        return NULL;
    }
    return e;
}

void tf_expr_free(tf_expr *e) {
    if (!e) return;
    switch (e->kind) {
        case EXPR_LIT_STR: free(e->lit_str); break;
        case EXPR_COL_REF: free(e->col_name); break;
        case EXPR_CMP:
            tf_expr_free(e->cmp.left);
            tf_expr_free(e->cmp.right);
            break;
        case EXPR_AND: case EXPR_OR:
        case EXPR_ADD: case EXPR_SUB: case EXPR_MUL: case EXPR_DIV:
            tf_expr_free(e->binary.left);
            tf_expr_free(e->binary.right);
            break;
        case EXPR_NOT: case EXPR_NEG:
            tf_expr_free(e->child);
            break;
        case EXPR_FUNC_CALL:
            free(e->func.name);
            for (int i = 0; i < e->func.n_args; i++)
                tf_expr_free(e->func.args[i]);
            free(e->func.args);
            break;
        default: break;
    }
    free(e);
}

/* ---- Evaluator ---- */

/* A runtime value for expression evaluation.
 * String values either point to batch/literal memory (owned=0)
 * or to the scratch buffer (owned=0, in scratch). */
typedef struct {
    enum { VAL_NULL, VAL_INT, VAL_FLOAT, VAL_STR, VAL_BOOL, VAL_DATE, VAL_TIMESTAMP } tag;
    union {
        int64_t i;
        double  f;
        const char *s;
        int     b;
        int32_t date;  /* days since epoch */
        int64_t ts;    /* microseconds since epoch */
    };
} eval_val;

/* Scratch buffer for string function results.
 * Uses a ring buffer approach: each call gets a slot, wrapping around. */
#define SCRATCH_SLOTS 8
#define SCRATCH_SLOT_SIZE 4096
static char scratch_buf[SCRATCH_SLOTS][SCRATCH_SLOT_SIZE];
static int scratch_idx = 0;

static char *scratch_alloc(void) {
    char *slot = scratch_buf[scratch_idx % SCRATCH_SLOTS];
    scratch_idx++;
    return slot;
}

static eval_val eval_node(const tf_expr *e, const tf_batch *batch, size_t row);

static eval_val val_null(void) { return (eval_val){ .tag = VAL_NULL }; }
static eval_val val_int(int64_t v) { return (eval_val){ .tag = VAL_INT, .i = v }; }
static eval_val val_float(double v) { return (eval_val){ .tag = VAL_FLOAT, .f = v }; }
static eval_val val_str(const char *v) { return (eval_val){ .tag = VAL_STR, .s = v }; }
static eval_val val_bool(int v) { return (eval_val){ .tag = VAL_BOOL, .b = v }; }
static eval_val val_date(int32_t v) { return (eval_val){ .tag = VAL_DATE, .date = v }; }
static eval_val val_timestamp(int64_t v) { return (eval_val){ .tag = VAL_TIMESTAMP, .ts = v }; }

/* Convert to double for numeric comparison */
static double to_double(eval_val v) {
    if (v.tag == VAL_INT) return (double)v.i;
    if (v.tag == VAL_FLOAT) return v.f;
    return 0.0;
}

static int is_numeric(eval_val v) {
    return v.tag == VAL_INT || v.tag == VAL_FLOAT;
}

static eval_val eval_cmp(eval_val lv, eval_val rv, cmp_op op) {
    /* Null comparisons: null != anything is true, null == null is true */
    if (lv.tag == VAL_NULL || rv.tag == VAL_NULL) {
        int both_null = (lv.tag == VAL_NULL && rv.tag == VAL_NULL);
        switch (op) {
            case CMP_EQ: return val_bool(both_null);
            case CMP_NE: return val_bool(!both_null);
            default:     return val_bool(0);
        }
    }

    /* String comparison */
    if (lv.tag == VAL_STR && rv.tag == VAL_STR) {
        int cmp = strcmp(lv.s, rv.s);
        switch (op) {
            case CMP_GT: return val_bool(cmp > 0);
            case CMP_GE: return val_bool(cmp >= 0);
            case CMP_LT: return val_bool(cmp < 0);
            case CMP_LE: return val_bool(cmp <= 0);
            case CMP_EQ: return val_bool(cmp == 0);
            case CMP_NE: return val_bool(cmp != 0);
        }
    }

    /* Numeric comparison */
    if (is_numeric(lv) && is_numeric(rv)) {
        double l = to_double(lv);
        double r = to_double(rv);
        switch (op) {
            case CMP_GT: return val_bool(l > r);
            case CMP_GE: return val_bool(l >= r);
            case CMP_LT: return val_bool(l < r);
            case CMP_LE: return val_bool(l <= r);
            case CMP_EQ: return val_bool(l == r);
            case CMP_NE: return val_bool(l != r);
        }
    }

    /* Date/timestamp comparison — promote date to timestamp (midnight),
     * and try parsing string literals as dates/timestamps */
    if (lv.tag == VAL_DATE || lv.tag == VAL_TIMESTAMP ||
        rv.tag == VAL_DATE || rv.tag == VAL_TIMESTAMP) {
        /* Promote VAL_STR to date/timestamp if parseable */
        for (int side = 0; side < 2; side++) {
            eval_val *v = (side == 0) ? &lv : &rv;
            if (v->tag == VAL_STR && v->s) {
                size_t slen = strlen(v->s);
                int y, m, d;
                if (slen == 10 && sscanf(v->s, "%d-%d-%d", &y, &m, &d) == 3) {
                    *v = val_date(tf_date_from_ymd(y, m, d));
                } else if (slen >= 19) {
                    int h = 0, mi2 = 0, s = 0;
                    if (sscanf(v->s, "%d-%d-%dT%d:%d:%d", &y, &m, &d, &h, &mi2, &s) >= 6 ||
                        sscanf(v->s, "%d-%d-%d %d:%d:%d", &y, &m, &d, &h, &mi2, &s) >= 6)
                        *v = val_timestamp(tf_timestamp_from_parts(y, m, d, h, mi2, s, 0));
                }
            }
        }
        int64_t l, r;
        if (lv.tag == VAL_DATE) l = (int64_t)lv.date * 86400LL * 1000000LL;
        else if (lv.tag == VAL_TIMESTAMP) l = lv.ts;
        else { if (op == CMP_EQ) return val_bool(0); if (op == CMP_NE) return val_bool(1); return val_bool(0); }
        if (rv.tag == VAL_DATE) r = (int64_t)rv.date * 86400LL * 1000000LL;
        else if (rv.tag == VAL_TIMESTAMP) r = rv.ts;
        else { if (op == CMP_EQ) return val_bool(0); if (op == CMP_NE) return val_bool(1); return val_bool(0); }
        switch (op) {
            case CMP_GT: return val_bool(l > r);
            case CMP_GE: return val_bool(l >= r);
            case CMP_LT: return val_bool(l < r);
            case CMP_LE: return val_bool(l <= r);
            case CMP_EQ: return val_bool(l == r);
            case CMP_NE: return val_bool(l != r);
        }
    }

    /* Mixed types: convert to string and compare */
    /* For simplicity, treat as not equal for non-EQ/NE ops */
    if (op == CMP_EQ) return val_bool(0);
    if (op == CMP_NE) return val_bool(1);
    return val_bool(0);
}

static eval_val eval_arith(eval_val lv, eval_val rv, expr_kind kind) {
    /* If either is null, result is null */
    if (lv.tag == VAL_NULL || rv.tag == VAL_NULL) return val_null();

    /* Date/timestamp arithmetic */
    if (lv.tag == VAL_DATE || lv.tag == VAL_TIMESTAMP ||
        rv.tag == VAL_DATE || rv.tag == VAL_TIMESTAMP) {
        /* date - date → int (days) */
        if (lv.tag == VAL_DATE && rv.tag == VAL_DATE && kind == EXPR_SUB)
            return val_int((int64_t)lv.date - (int64_t)rv.date);
        /* timestamp - timestamp → int (microseconds) */
        if (lv.tag == VAL_TIMESTAMP && rv.tag == VAL_TIMESTAMP && kind == EXPR_SUB)
            return val_int(lv.ts - rv.ts);
        /* date ± int → date */
        if (lv.tag == VAL_DATE && rv.tag == VAL_INT) {
            if (kind == EXPR_ADD) return val_date(lv.date + (int32_t)rv.i);
            if (kind == EXPR_SUB) return val_date(lv.date - (int32_t)rv.i);
        }
        /* timestamp ± int → timestamp */
        if (lv.tag == VAL_TIMESTAMP && rv.tag == VAL_INT) {
            if (kind == EXPR_ADD) return val_timestamp(lv.ts + rv.i);
            if (kind == EXPR_SUB) return val_timestamp(lv.ts - rv.i);
        }
        /* int + date → date */
        if (lv.tag == VAL_INT && rv.tag == VAL_DATE && kind == EXPR_ADD)
            return val_date((int32_t)lv.i + rv.date);
        /* int + timestamp → timestamp */
        if (lv.tag == VAL_INT && rv.tag == VAL_TIMESTAMP && kind == EXPR_ADD)
            return val_timestamp(lv.i + rv.ts);
        return val_null();
    }

    /* Both must be numeric */
    if (!is_numeric(lv) || !is_numeric(rv)) return val_null();

    /* If both are int and not dividing, stay in int domain */
    if (lv.tag == VAL_INT && rv.tag == VAL_INT && kind != EXPR_DIV) {
        int64_t l = lv.i, r = rv.i;
        switch (kind) {
            case EXPR_ADD: return val_int(l + r);
            case EXPR_SUB: return val_int(l - r);
            case EXPR_MUL: return val_int(l * r);
            default: break;
        }
    }

    /* Float domain */
    double l = to_double(lv);
    double r = to_double(rv);
    switch (kind) {
        case EXPR_ADD: return val_float(l + r);
        case EXPR_SUB: return val_float(l - r);
        case EXPR_MUL: return val_float(l * r);
        case EXPR_DIV:
            if (r == 0.0) return val_null();
            return val_float(l / r);
        default: return val_null();
    }
}

/* Get string representation of a value for string functions */
static const char *val_to_str(eval_val v, char *buf, size_t buf_sz) {
    switch (v.tag) {
        case VAL_STR:  return v.s;
        case VAL_INT:  snprintf(buf, buf_sz, "%lld", (long long)v.i); return buf;
        case VAL_FLOAT: snprintf(buf, buf_sz, "%g", v.f); return buf;
        case VAL_BOOL: return v.b ? "true" : "false";
        case VAL_DATE: tf_date_format(v.date, buf, buf_sz); return buf;
        case VAL_TIMESTAMP: tf_timestamp_format(v.ts, buf, buf_sz); return buf;
        default: return "";
    }
}

static eval_val eval_func(const char *name, eval_val *args, int n_args) {
    /* ---- String functions ---- */

    /* upper(s) */
    if (strcmp(name, "upper") == 0 && n_args == 1) {
        if (args[0].tag == VAL_NULL) return val_null();
        char tmp[64];
        const char *s = val_to_str(args[0], tmp, sizeof(tmp));
        char *out = scratch_alloc();
        size_t i;
        for (i = 0; s[i] && i < SCRATCH_SLOT_SIZE - 1; i++)
            out[i] = (char)toupper((unsigned char)s[i]);
        out[i] = '\0';
        return val_str(out);
    }

    /* lower(s) */
    if (strcmp(name, "lower") == 0 && n_args == 1) {
        if (args[0].tag == VAL_NULL) return val_null();
        char tmp[64];
        const char *s = val_to_str(args[0], tmp, sizeof(tmp));
        char *out = scratch_alloc();
        size_t i;
        for (i = 0; s[i] && i < SCRATCH_SLOT_SIZE - 1; i++)
            out[i] = (char)tolower((unsigned char)s[i]);
        out[i] = '\0';
        return val_str(out);
    }

    /* len(s) — returns string length as int */
    if (strcmp(name, "len") == 0 && n_args == 1) {
        if (args[0].tag == VAL_NULL) return val_null();
        if (args[0].tag == VAL_STR) return val_int((int64_t)strlen(args[0].s));
        return val_null();
    }

    /* trim(s) */
    if (strcmp(name, "trim") == 0 && n_args == 1) {
        if (args[0].tag == VAL_NULL) return val_null();
        if (args[0].tag != VAL_STR) return val_null();
        const char *s = args[0].s;
        while (*s && isspace((unsigned char)*s)) s++;
        size_t len = strlen(s);
        while (len > 0 && isspace((unsigned char)s[len - 1])) len--;
        char *out = scratch_alloc();
        if (len >= SCRATCH_SLOT_SIZE) len = SCRATCH_SLOT_SIZE - 1;
        memcpy(out, s, len);
        out[len] = '\0';
        return val_str(out);
    }

    /* starts_with(s, prefix) */
    if (strcmp(name, "starts_with") == 0 && n_args == 2) {
        if (args[0].tag == VAL_NULL || args[1].tag == VAL_NULL) return val_null();
        if (args[0].tag != VAL_STR || args[1].tag != VAL_STR) return val_bool(0);
        size_t plen = strlen(args[1].s);
        return val_bool(strncmp(args[0].s, args[1].s, plen) == 0);
    }

    /* ends_with(s, suffix) */
    if (strcmp(name, "ends_with") == 0 && n_args == 2) {
        if (args[0].tag == VAL_NULL || args[1].tag == VAL_NULL) return val_null();
        if (args[0].tag != VAL_STR || args[1].tag != VAL_STR) return val_bool(0);
        size_t slen = strlen(args[0].s);
        size_t xlen = strlen(args[1].s);
        if (xlen > slen) return val_bool(0);
        return val_bool(strcmp(args[0].s + slen - xlen, args[1].s) == 0);
    }

    /* contains(s, substr) */
    if (strcmp(name, "contains") == 0 && n_args == 2) {
        if (args[0].tag == VAL_NULL || args[1].tag == VAL_NULL) return val_null();
        if (args[0].tag != VAL_STR || args[1].tag != VAL_STR) return val_bool(0);
        return val_bool(strstr(args[0].s, args[1].s) != NULL);
    }

    /* slice(s, start, len) — substring */
    if (strcmp(name, "slice") == 0 && n_args >= 2) {
        if (args[0].tag == VAL_NULL) return val_null();
        if (args[0].tag != VAL_STR) return val_null();
        int64_t start = 0, slice_len = -1;
        if (args[1].tag == VAL_INT) start = args[1].i;
        else if (args[1].tag == VAL_FLOAT) start = (int64_t)args[1].f;
        if (n_args >= 3) {
            if (args[2].tag == VAL_INT) slice_len = args[2].i;
            else if (args[2].tag == VAL_FLOAT) slice_len = (int64_t)args[2].f;
        }
        size_t slen = strlen(args[0].s);
        if (start < 0) start = (int64_t)slen + start;
        if (start < 0) start = 0;
        if ((size_t)start >= slen) return val_str("");
        size_t avail = slen - (size_t)start;
        size_t take = (slice_len < 0) ? avail : ((size_t)slice_len < avail ? (size_t)slice_len : avail);
        char *out = scratch_alloc();
        if (take >= SCRATCH_SLOT_SIZE) take = SCRATCH_SLOT_SIZE - 1;
        memcpy(out, args[0].s + start, take);
        out[take] = '\0';
        return val_str(out);
    }

    /* concat(a, b, ...) — concatenate strings */
    if (strcmp(name, "concat") == 0 && n_args >= 1) {
        char *out = scratch_alloc();
        size_t pos = 0;
        char tmp[64];
        for (int i = 0; i < n_args && pos < SCRATCH_SLOT_SIZE - 1; i++) {
            if (args[i].tag == VAL_NULL) continue;
            const char *s = val_to_str(args[i], tmp, sizeof(tmp));
            size_t sl = strlen(s);
            if (pos + sl >= SCRATCH_SLOT_SIZE) sl = SCRATCH_SLOT_SIZE - 1 - pos;
            memcpy(out + pos, s, sl);
            pos += sl;
        }
        out[pos] = '\0';
        return val_str(out);
    }

    /* pad_left(s, width, char) */
    if (strcmp(name, "pad_left") == 0 && n_args >= 2) {
        if (args[0].tag == VAL_NULL) return val_null();
        char tmp[64];
        const char *s = val_to_str(args[0], tmp, sizeof(tmp));
        int64_t width = 0;
        if (args[1].tag == VAL_INT) width = args[1].i;
        else if (args[1].tag == VAL_FLOAT) width = (int64_t)args[1].f;
        char pad_ch = ' ';
        if (n_args >= 3 && args[2].tag == VAL_STR && args[2].s[0])
            pad_ch = args[2].s[0];
        char *out = scratch_alloc();
        size_t slen = strlen(s);
        if (width < 0) width = 0;
        if ((size_t)width >= SCRATCH_SLOT_SIZE) width = SCRATCH_SLOT_SIZE - 1;
        if (slen >= (size_t)width) {
            memcpy(out, s, slen < SCRATCH_SLOT_SIZE - 1 ? slen : SCRATCH_SLOT_SIZE - 1);
            out[slen < SCRATCH_SLOT_SIZE - 1 ? slen : SCRATCH_SLOT_SIZE - 1] = '\0';
        } else {
            size_t pad_n = (size_t)width - slen;
            memset(out, pad_ch, pad_n);
            memcpy(out + pad_n, s, slen);
            out[(size_t)width] = '\0';
        }
        return val_str(out);
    }

    /* pad_right(s, width, char) */
    if (strcmp(name, "pad_right") == 0 && n_args >= 2) {
        if (args[0].tag == VAL_NULL) return val_null();
        char tmp[64];
        const char *s = val_to_str(args[0], tmp, sizeof(tmp));
        int64_t width = 0;
        if (args[1].tag == VAL_INT) width = args[1].i;
        else if (args[1].tag == VAL_FLOAT) width = (int64_t)args[1].f;
        char pad_ch = ' ';
        if (n_args >= 3 && args[2].tag == VAL_STR && args[2].s[0])
            pad_ch = args[2].s[0];
        char *out = scratch_alloc();
        size_t slen = strlen(s);
        if (width < 0) width = 0;
        if ((size_t)width >= SCRATCH_SLOT_SIZE) width = SCRATCH_SLOT_SIZE - 1;
        if (slen >= (size_t)width) {
            memcpy(out, s, slen < SCRATCH_SLOT_SIZE - 1 ? slen : SCRATCH_SLOT_SIZE - 1);
            out[slen < SCRATCH_SLOT_SIZE - 1 ? slen : SCRATCH_SLOT_SIZE - 1] = '\0';
        } else {
            memcpy(out, s, slen);
            memset(out + slen, pad_ch, (size_t)width - slen);
            out[(size_t)width] = '\0';
        }
        return val_str(out);
    }

    /* ---- Conditional functions ---- */

    /* if(cond, then, else) */
    if (strcmp(name, "if") == 0 && n_args == 3) {
        if (args[0].tag == VAL_BOOL)
            return args[0].b ? args[1] : args[2];
        /* Truthy: non-null, non-zero */
        int truthy = (args[0].tag != VAL_NULL);
        if (args[0].tag == VAL_INT) truthy = (args[0].i != 0);
        if (args[0].tag == VAL_FLOAT) truthy = (args[0].f != 0.0);
        return truthy ? args[1] : args[2];
    }

    /* coalesce(a, b, ...) — first non-null */
    if (strcmp(name, "coalesce") == 0) {
        for (int i = 0; i < n_args; i++) {
            if (args[i].tag != VAL_NULL) return args[i];
        }
        return val_null();
    }

    /* ---- Math functions ---- */

    /* abs(x) */
    if (strcmp(name, "abs") == 0 && n_args == 1) {
        if (args[0].tag == VAL_INT) return val_int(args[0].i < 0 ? -args[0].i : args[0].i);
        if (args[0].tag == VAL_FLOAT) return val_float(fabs(args[0].f));
        return val_null();
    }

    /* round(x) */
    if (strcmp(name, "round") == 0 && n_args == 1) {
        if (args[0].tag == VAL_INT) return args[0];
        if (args[0].tag == VAL_FLOAT) return val_int((int64_t)round(args[0].f));
        return val_null();
    }

    /* floor(x) */
    if (strcmp(name, "floor") == 0 && n_args == 1) {
        if (args[0].tag == VAL_INT) return args[0];
        if (args[0].tag == VAL_FLOAT) return val_int((int64_t)floor(args[0].f));
        return val_null();
    }

    /* ceil(x) */
    if (strcmp(name, "ceil") == 0 && n_args == 1) {
        if (args[0].tag == VAL_INT) return args[0];
        if (args[0].tag == VAL_FLOAT) return val_int((int64_t)ceil(args[0].f));
        return val_null();
    }

    /* min(a, b, ...) / least(a, b, ...) — variadic minimum */
    if ((strcmp(name, "min") == 0 || strcmp(name, "least") == 0) && n_args >= 2) {
        int all_int = 1;
        for (int i = 0; i < n_args; i++) {
            if (!is_numeric(args[i])) return val_null();
            if (args[i].tag != VAL_INT) all_int = 0;
        }
        if (all_int) {
            int64_t result = args[0].i;
            for (int i = 1; i < n_args; i++)
                if (args[i].i < result) result = args[i].i;
            return val_int(result);
        }
        double result = to_double(args[0]);
        for (int i = 1; i < n_args; i++) {
            double v = to_double(args[i]);
            if (v < result) result = v;
        }
        return val_float(result);
    }

    /* max(a, b, ...) / greatest(a, b, ...) — variadic maximum */
    if ((strcmp(name, "max") == 0 || strcmp(name, "greatest") == 0) && n_args >= 2) {
        int all_int = 1;
        for (int i = 0; i < n_args; i++) {
            if (!is_numeric(args[i])) return val_null();
            if (args[i].tag != VAL_INT) all_int = 0;
        }
        if (all_int) {
            int64_t result = args[0].i;
            for (int i = 1; i < n_args; i++)
                if (args[i].i > result) result = args[i].i;
            return val_int(result);
        }
        double result = to_double(args[0]);
        for (int i = 1; i < n_args; i++) {
            double v = to_double(args[i]);
            if (v > result) result = v;
        }
        return val_float(result);
    }

    /* sign(x) — returns -1, 0, or 1 */
    if (strcmp(name, "sign") == 0 && n_args == 1) {
        if (args[0].tag == VAL_INT)
            return val_int(args[0].i > 0 ? 1 : (args[0].i < 0 ? -1 : 0));
        if (args[0].tag == VAL_FLOAT)
            return val_int(args[0].f > 0 ? 1 : (args[0].f < 0 ? -1 : 0));
        return val_null();
    }

    /* nullif(a, b) — returns NULL if a == b, else a */
    if (strcmp(name, "nullif") == 0 && n_args == 2) {
        if (args[0].tag == VAL_NULL && args[1].tag == VAL_NULL) return val_null();
        if (args[0].tag == VAL_INT && args[1].tag == VAL_INT)
            return args[0].i == args[1].i ? val_null() : args[0];
        if (is_numeric(args[0]) && is_numeric(args[1]))
            return to_double(args[0]) == to_double(args[1]) ? val_null() : args[0];
        if (args[0].tag == VAL_STR && args[1].tag == VAL_STR)
            return strcmp(args[0].s, args[1].s) == 0 ? val_null() : args[0];
        return args[0];
    }

    /* initcap(s) — title case */
    if (strcmp(name, "initcap") == 0 && n_args == 1) {
        if (args[0].tag == VAL_NULL) return val_null();
        char tmp[64];
        const char *s = val_to_str(args[0], tmp, sizeof(tmp));
        char *out = scratch_alloc();
        int word_start = 1;
        size_t i;
        for (i = 0; s[i] && i < SCRATCH_SLOT_SIZE - 1; i++) {
            if (isspace((unsigned char)s[i]) || s[i] == '_' || s[i] == '-') {
                out[i] = s[i];
                word_start = 1;
            } else if (word_start) {
                out[i] = (char)toupper((unsigned char)s[i]);
                word_start = 0;
            } else {
                out[i] = (char)tolower((unsigned char)s[i]);
            }
        }
        out[i] = '\0';
        return val_str(out);
    }

    /* left(s, n) — first n characters */
    if (strcmp(name, "left") == 0 && n_args == 2) {
        if (args[0].tag == VAL_NULL) return val_null();
        if (args[0].tag != VAL_STR) return val_null();
        int64_t n = args[1].tag == VAL_INT ? args[1].i : (int64_t)args[1].f;
        if (n < 0) n = 0;
        size_t slen = strlen(args[0].s);
        size_t take = (size_t)n < slen ? (size_t)n : slen;
        char *out = scratch_alloc();
        if (take >= SCRATCH_SLOT_SIZE) take = SCRATCH_SLOT_SIZE - 1;
        memcpy(out, args[0].s, take);
        out[take] = '\0';
        return val_str(out);
    }

    /* right(s, n) — last n characters */
    if (strcmp(name, "right") == 0 && n_args == 2) {
        if (args[0].tag == VAL_NULL) return val_null();
        if (args[0].tag != VAL_STR) return val_null();
        int64_t n = args[1].tag == VAL_INT ? args[1].i : (int64_t)args[1].f;
        if (n < 0) n = 0;
        size_t slen = strlen(args[0].s);
        size_t take = (size_t)n < slen ? (size_t)n : slen;
        char *out = scratch_alloc();
        if (take >= SCRATCH_SLOT_SIZE) take = SCRATCH_SLOT_SIZE - 1;
        memcpy(out, args[0].s + slen - take, take);
        out[take] = '\0';
        return val_str(out);
    }

    /* replace(s, old, new) — string replace */
    if (strcmp(name, "replace") == 0 && n_args == 3) {
        if (args[0].tag == VAL_NULL) return val_null();
        if (args[0].tag != VAL_STR || args[1].tag != VAL_STR || args[2].tag != VAL_STR)
            return val_null();
        const char *s = args[0].s;
        const char *old_s = args[1].s;
        const char *new_s = args[2].s;
        size_t old_len = strlen(old_s);
        size_t new_len = strlen(new_s);
        if (old_len == 0) return args[0];
        char *out = scratch_alloc();
        size_t pos = 0;
        while (*s && pos < SCRATCH_SLOT_SIZE - 1) {
            const char *found = strstr(s, old_s);
            if (!found) {
                size_t rest = strlen(s);
                if (pos + rest >= SCRATCH_SLOT_SIZE) rest = SCRATCH_SLOT_SIZE - 1 - pos;
                memcpy(out + pos, s, rest);
                pos += rest;
                break;
            }
            size_t before = (size_t)(found - s);
            if (pos + before >= SCRATCH_SLOT_SIZE) before = SCRATCH_SLOT_SIZE - 1 - pos;
            memcpy(out + pos, s, before);
            pos += before;
            if (pos + new_len >= SCRATCH_SLOT_SIZE) break;
            memcpy(out + pos, new_s, new_len);
            pos += new_len;
            s = found + old_len;
        }
        out[pos] = '\0';
        return val_str(out);
    }

    /* pow(x, y) */
    if (strcmp(name, "pow") == 0 && n_args == 2) {
        if (!is_numeric(args[0]) || !is_numeric(args[1])) return val_null();
        return val_float(pow(to_double(args[0]), to_double(args[1])));
    }

    /* sqrt(x) */
    if (strcmp(name, "sqrt") == 0 && n_args == 1) {
        if (!is_numeric(args[0])) return val_null();
        double v = to_double(args[0]);
        if (v < 0) return val_null();
        return val_float(sqrt(v));
    }

    /* log(x) — natural log */
    if (strcmp(name, "log") == 0 && n_args == 1) {
        if (!is_numeric(args[0])) return val_null();
        double v = to_double(args[0]);
        if (v <= 0) return val_null();
        return val_float(log(v));
    }

    /* exp(x) */
    if (strcmp(name, "exp") == 0 && n_args == 1) {
        if (!is_numeric(args[0])) return val_null();
        return val_float(exp(to_double(args[0])));
    }

    /* mod(a, b) — modulo */
    if (strcmp(name, "mod") == 0 && n_args == 2) {
        if (!is_numeric(args[0]) || !is_numeric(args[1])) return val_null();
        if (args[0].tag == VAL_INT && args[1].tag == VAL_INT) {
            if (args[1].i == 0) return val_null();
            return val_int(args[0].i % args[1].i);
        }
        double b = to_double(args[1]);
        if (b == 0.0) return val_null();
        return val_float(fmod(to_double(args[0]), b));
    }

    /* ---- Aliases ---- */

    /* substr → slice */
    if (strcmp(name, "substr") == 0 && n_args >= 2)
        return eval_func("slice", args, n_args);

    /* length → len */
    if (strcmp(name, "length") == 0 && n_args == 1)
        return eval_func("len", args, n_args);

    /* lpad → pad_left */
    if (strcmp(name, "lpad") == 0 && n_args >= 2)
        return eval_func("pad_left", args, n_args);

    /* rpad → pad_right */
    if (strcmp(name, "rpad") == 0 && n_args >= 2)
        return eval_func("pad_right", args, n_args);

    return val_null(); /* unknown function */
}

static eval_val eval_node(const tf_expr *e, const tf_batch *batch, size_t row) {
    switch (e->kind) {
        case EXPR_LIT_INT:
            return val_int(e->lit_int);
        case EXPR_LIT_FLOAT:
            return val_float(e->lit_float);
        case EXPR_LIT_STR:
            return val_str(e->lit_str);
        case EXPR_COL_REF: {
            int ci = tf_batch_col_index(batch, e->col_name);
            if (ci < 0) return val_null();
            if (tf_batch_is_null(batch, row, ci)) return val_null();
            switch (batch->col_types[ci]) {
                case TF_TYPE_BOOL:      return val_bool(tf_batch_get_bool(batch, row, ci));
                case TF_TYPE_INT64:     return val_int(tf_batch_get_int64(batch, row, ci));
                case TF_TYPE_FLOAT64:   return val_float(tf_batch_get_float64(batch, row, ci));
                case TF_TYPE_STRING:    return val_str(tf_batch_get_string(batch, row, ci));
                case TF_TYPE_DATE:      return val_date(tf_batch_get_date(batch, row, ci));
                case TF_TYPE_TIMESTAMP: return val_timestamp(tf_batch_get_timestamp(batch, row, ci));
                default:                return val_null();
            }
        }
        case EXPR_CMP: {
            eval_val lv = eval_node(e->cmp.left, batch, row);
            eval_val rv = eval_node(e->cmp.right, batch, row);
            return eval_cmp(lv, rv, e->cmp.op);
        }
        case EXPR_AND: {
            eval_val lv = eval_node(e->binary.left, batch, row);
            if (lv.tag == VAL_BOOL && !lv.b) return val_bool(0);
            eval_val rv = eval_node(e->binary.right, batch, row);
            return val_bool(lv.tag == VAL_BOOL && lv.b &&
                            rv.tag == VAL_BOOL && rv.b);
        }
        case EXPR_OR: {
            eval_val lv = eval_node(e->binary.left, batch, row);
            if (lv.tag == VAL_BOOL && lv.b) return val_bool(1);
            eval_val rv = eval_node(e->binary.right, batch, row);
            return val_bool((lv.tag == VAL_BOOL && lv.b) ||
                            (rv.tag == VAL_BOOL && rv.b));
        }
        case EXPR_NOT: {
            eval_val cv = eval_node(e->child, batch, row);
            return val_bool(!(cv.tag == VAL_BOOL && cv.b));
        }
        case EXPR_NEG: {
            eval_val cv = eval_node(e->child, batch, row);
            if (cv.tag == VAL_INT) return val_int(-cv.i);
            if (cv.tag == VAL_FLOAT) return val_float(-cv.f);
            return val_null();
        }
        case EXPR_ADD: case EXPR_SUB: case EXPR_MUL: case EXPR_DIV: {
            eval_val lv = eval_node(e->binary.left, batch, row);
            eval_val rv = eval_node(e->binary.right, batch, row);
            return eval_arith(lv, rv, e->kind);
        }
        case EXPR_FUNC_CALL: {
            /* Evaluate all arguments */
            eval_val argv[16]; /* max 16 args */
            int nargs = e->func.n_args;
            if (nargs > 16) nargs = 16;
            for (int i = 0; i < nargs; i++)
                argv[i] = eval_node(e->func.args[i], batch, row);
            return eval_func(e->func.name, argv, nargs);
        }
    }
    return val_null();
}

int tf_expr_eval(const tf_expr *e, const tf_batch *batch, size_t row, bool *result) {
    eval_val v = eval_node(e, batch, row);
    if (v.tag == VAL_BOOL) {
        *result = v.b != 0;
        return TF_OK;
    }
    /* Non-boolean result: treat truthy (non-null, non-zero) */
    *result = (v.tag != VAL_NULL);
    return TF_OK;
}

int tf_expr_eval_val(const tf_expr *e, const tf_batch *batch, size_t row,
                     tf_eval_result *result) {
    eval_val v = eval_node(e, batch, row);
    switch (v.tag) {
        case VAL_NULL:
            result->type = TF_TYPE_NULL;
            break;
        case VAL_INT:
            result->type = TF_TYPE_INT64;
            result->i = v.i;
            break;
        case VAL_FLOAT:
            result->type = TF_TYPE_FLOAT64;
            result->f = v.f;
            break;
        case VAL_STR:
            result->type = TF_TYPE_STRING;
            result->s = v.s;
            break;
        case VAL_BOOL:
            result->type = TF_TYPE_BOOL;
            result->b = v.b != 0;
            break;
        case VAL_DATE:
            result->type = TF_TYPE_DATE;
            result->date = v.date;
            break;
        case VAL_TIMESTAMP:
            result->type = TF_TYPE_TIMESTAMP;
            result->i = v.ts;
            break;
    }
    return TF_OK;
}
