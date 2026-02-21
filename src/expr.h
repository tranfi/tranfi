/*
 * expr.h — Expression AST types (shared between expr.c and ir_sql.c).
 */

#ifndef TF_EXPR_H
#define TF_EXPR_H

#include <stdint.h>

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

#endif /* TF_EXPR_H */
