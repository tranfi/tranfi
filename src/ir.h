/*
 * ir.h — L2 Intermediate Representation types for the Tranfi compilation pipeline.
 *
 * Defines the op registry, schema, IR node, and IR plan types.
 * The IR is the contract between authoring (L3) and execution (L1).
 */

#ifndef TF_IR_H
#define TF_IR_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/* Forward declarations */
typedef struct cJSON cJSON;
typedef struct tf_ir_node tf_ir_node;
typedef struct tf_schema tf_schema;

/* ---- Capability flags ---- */

#define TF_CAP_STREAMING      (1 << 0)  /* processes batches without buffering all data */
#define TF_CAP_BOUNDED_MEMORY (1 << 1)  /* memory usage independent of input size */
#define TF_CAP_BROWSER_SAFE   (1 << 2)  /* no filesystem/network access */
#define TF_CAP_DETERMINISTIC  (1 << 3)  /* same input always produces same output */
#define TF_CAP_FS             (1 << 4)  /* requires filesystem access */
#define TF_CAP_NET            (1 << 5)  /* requires network access */

/* ---- Op tier ---- */

typedef enum {
    TF_TIER_CORE,       /* built into the C library */
    TF_TIER_ECOSYSTEM,  /* provided by host-side plugins */
} tf_op_tier;

/* ---- Op kind ---- */

typedef enum {
    TF_OP_DECODER,
    TF_OP_ENCODER,
    TF_OP_TRANSFORM,
} tf_op_kind;

/* ---- Argument descriptor ---- */

typedef struct {
    const char *name;        /* "delimiter", "expr", "columns", etc. */
    const char *type;        /* "string", "int", "bool", "string[]", "map" */
    bool        required;
    const char *default_val; /* JSON-encoded default, or NULL */
} tf_arg_desc;

/* ---- Value types (shared with batch system) ---- */

typedef enum tf_type {
    TF_TYPE_NULL = 0,
    TF_TYPE_BOOL,
    TF_TYPE_INT64,
    TF_TYPE_FLOAT64,
    TF_TYPE_STRING,
    TF_TYPE_DATE,       /* int32_t: days since 1970-01-01 */
    TF_TYPE_TIMESTAMP,  /* int64_t: microseconds since 1970-01-01T00:00:00Z */
} tf_type;

/* ---- Schema ---- */

struct tf_schema {
    char    **col_names;
    tf_type  *col_types;
    size_t    n_cols;
    bool      known;  /* false if schema can't be determined until runtime */
};

void tf_schema_free(tf_schema *s);
void tf_schema_copy(tf_schema *dst, const tf_schema *src);

/* ---- Op registry entry ---- */

typedef struct {
    const char    *name;        /* "codec.csv.decode", "filter", etc. */
    tf_op_kind     kind;
    tf_op_tier     tier;
    uint32_t       caps;        /* TF_CAP_* bitfield */
    tf_arg_desc   *args;
    size_t         n_args;
    /* Schema transform: given input schema, compute output schema.
     * Returns TF_OK or TF_ERROR. */
    int (*infer_schema)(const tf_ir_node *node,
                        const tf_schema *in, tf_schema *out);
    /* Native target constructor (NULL for ecosystem ops).
     * Returns an opaque pointer (tf_decoder*, tf_step*, or tf_encoder*). */
    void *(*create_native)(const cJSON *args);
} tf_op_entry;

/* ---- Op registry API ---- */

const tf_op_entry *tf_op_registry_find(const char *name);
size_t             tf_op_registry_count(void);
const tf_op_entry *tf_op_registry_get(size_t index);

/* ---- IR node ---- */

struct tf_ir_node {
    const char     *op;           /* op name (owned, freed with node) */
    cJSON          *args;         /* argument values (owned, freed with node) */
    tf_schema       input_schema;
    tf_schema       output_schema;
    uint32_t        caps;         /* resolved capability flags from registry */
    size_t          index;        /* position in plan */
};

/* ---- IR plan ---- */

typedef struct tf_ir_plan {
    tf_ir_node    *nodes;
    size_t         n_nodes;
    size_t         capacity;      /* allocated node slots */
    tf_schema      final_schema;  /* schema after last transform, before encoder */
    uint32_t       plan_caps;     /* intersection of all node caps */
    char          *error;         /* validation error, if any */
    bool           validated;
    bool           schema_inferred;
} tf_ir_plan;

/* ---- IR plan API ---- */

tf_ir_plan *tf_ir_plan_create(void);
int         tf_ir_plan_add_node(tf_ir_plan *plan, const char *op, cJSON *args);
tf_ir_plan *tf_ir_plan_clone(const tf_ir_plan *plan);
void        tf_ir_plan_free(tf_ir_plan *plan);

/* ---- IR serialization ---- */

tf_ir_plan *tf_ir_from_json(const char *json, size_t len, char **error);
char       *tf_ir_to_json(const tf_ir_plan *plan);

/* ---- IR passes ---- */

int tf_ir_validate(tf_ir_plan *plan);
int tf_ir_infer_schema(tf_ir_plan *plan);

/* ---- Expression eval result ---- */

typedef struct tf_eval_result {
    tf_type type;
    union {
        int64_t     i;     /* INT64 or TIMESTAMP (microseconds since epoch) */
        double      f;
        const char *s;
        bool        b;
        int32_t     date;  /* DATE: days since epoch */
    };
} tf_eval_result;

/* ---- Compiler ---- */

/* Forward declarations of internal types */
typedef struct tf_decoder tf_decoder;
typedef struct tf_step tf_step;
typedef struct tf_encoder tf_encoder;

int tf_compile_native(const tf_ir_plan *plan,
                      tf_decoder **decoder, tf_step ***steps,
                      size_t *n_steps, tf_encoder **encoder, char **error);

#endif /* TF_IR_H */
