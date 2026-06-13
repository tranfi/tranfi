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

/* ---- Streaming contract metadata ---- */

typedef enum {
    TF_MEM_ROW_LOCAL,       /* O(batch_rows * columns) */
    TF_MEM_BOUNDED_STATE,   /* O(parameter), e.g. window K or tail N */
    TF_MEM_KEY_STATE,       /* O(distinct keys/categories) */
    TF_MEM_BLOCKING,        /* needs full input in native mode */
    TF_MEM_EXTERNAL,        /* uses spill/SQL/external bounded storage */
} tf_memory_class;

typedef enum {
    TF_EMIT_PER_BATCH,      /* can emit during push() */
    TF_EMIT_ON_FLUSH,       /* emits only at finish(), e.g. tail/top/stats */
    TF_EMIT_SIDE_ONLY,      /* writes side channels, not main output */
    TF_EMIT_MIXED,          /* emits both during push() and flush/side output */
} tf_emit_class;

typedef enum {
    TF_SCHEMA_STABLE,       /* output columns known from input schema/args */
    TF_SCHEMA_PARAMETRIC,   /* output schema depends on explicit parameters */
    TF_SCHEMA_DATA_DEPENDENT, /* output schema depends on observed data */
} tf_schema_class;

/* ---- Argument descriptor ---- */

typedef struct {
    const char *name;        /* "delimiter", "expr", "columns", etc. */
    const char *type;        /* "string", "int", "bool", "string[]", "map" */
    bool        required;
    const char *default_val; /* JSON-encoded default, or NULL */
} tf_arg_desc;

/* ---- Value types (shared with batch system) ---- */

#ifndef TF_TYPE_DEFINED
#define TF_TYPE_DEFINED
typedef enum tf_type {
    TF_TYPE_NULL = 0,
    TF_TYPE_BOOL,
    TF_TYPE_INT64,
    TF_TYPE_FLOAT64,
    TF_TYPE_STRING,
    TF_TYPE_DATE,       /* int32_t: days since 1970-01-01 */
    TF_TYPE_TIMESTAMP,  /* int64_t: microseconds since 1970-01-01T00:00:00Z */
} tf_type;
#endif

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
    tf_memory_class memory_class;
    tf_emit_class   emit_class;
    tf_schema_class schema_class;
    const char    *state_estimate; /* Big-O state retained by the op */
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
const char        *tf_memory_class_name(tf_memory_class cls);
const char        *tf_emit_class_name(tf_emit_class cls);
const char        *tf_schema_class_name(tf_schema_class cls);

/* ---- IR node ---- */

struct tf_ir_node {
    const char     *op;           /* op name (owned, freed with node) */
    cJSON          *args;         /* argument values (owned, freed with node) */
    tf_schema       input_schema;
    tf_schema       output_schema;
    uint32_t        caps;         /* resolved capability flags from registry */
    tf_memory_class memory_class;
    tf_emit_class   emit_class;
    tf_schema_class schema_class;
    const char     *state_estimate; /* Big-O state estimate, static string */
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
char       *tf_ir_to_sql(const tf_ir_plan *plan, char **error);

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
