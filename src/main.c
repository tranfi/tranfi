/*
 * main.c — Tranfi CLI.
 *
 * Usage:
 *   tranfi 'csv | filter "col(age) > 25" | select name,age | csv'  < in.csv
 *   tranfi -f pipeline.tf < in.csv > out.csv
 *   tranfi -j 'csv | head 5 | csv'   # compile only, output JSON
 *   tranfi -i input.csv -o output.csv 'csv | filter "col(age) > 25" | csv'
 *
 * Channels:
 *   stdout  — main output (encoded data)
 *   stderr  — stats and errors
 */

#include "tranfi.h"
#include "internal.h"
#include "ir.h"
#include "dsl.h"
#include "recipes.h"
#include "report.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define READ_BUF_SIZE (64 * 1024)
#define PULL_BUF_SIZE (64 * 1024)

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s [OPTIONS] PIPELINE\n"
        "       %s [OPTIONS] -f FILE\n"
        "\n"
        "Streaming ETL with a pipe-style DSL.\n"
        "\n"
        "Examples:\n"
        "  %s 'csv | csv'                                    # passthrough\n"
        "  %s 'csv | filter \"col(age) > 25\" | csv'          # filter rows\n"
        "  %s 'csv | select name,age | csv'                  # select columns\n"
        "  %s 'csv | rename name=full_name | csv'            # rename columns\n"
        "  %s 'csv | head 10 | csv'                          # first N rows\n"
        "  %s 'csv | skip 5 | csv'                           # skip first 5 rows\n"
        "  %s 'csv | derive total=col(price)*col(qty) | csv' # computed columns\n"
        "  %s 'csv | sort age | csv'                         # sort by column\n"
        "  %s 'csv | unique name | csv'                      # deduplicate\n"
        "  %s 'csv | stats | csv'                            # aggregate stats\n"
        "  %s 'jsonl | filter \"col(x) > 0\" | jsonl'          # JSONL variant\n"
        "\n"
        "Options:\n"
        "  -f FILE   Read pipeline from file instead of argument\n"
        "  -i FILE   Read input from file instead of stdin\n"
        "  -o FILE   Write output to file instead of stdout\n"
        "  -j        Output plan as JSON (compile only, don't execute)\n"
        "  -p, --progress  Show progress on stderr\n"
        "  -q        Quiet mode (suppress stats on stderr)\n"
        "  --raw     Force raw CSV stats output (disable report formatting)\n"
        "  -v        Show version\n"
        "  -R, --recipes  List built-in recipes\n"
        "  -h        Show this help\n"
        "\n"
        "Recipes (use by name, e.g. %s profile):\n"
        "  profile, preview, schema, summary, count, cardinality,\n"
        "  distro, freq, dedup, clean, sample, head, tail, csv2json,\n"
        "  json2csv, tsv2csv, csv2tsv, histogram, hash, samples\n",
        prog, prog, prog, prog, prog, prog, prog, prog,
        prog, prog, prog, prog, prog, prog);
}

static char *read_file(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;

    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (size <= 0) { fclose(f); return NULL; }

    char *buf = malloc(size + 1);
    if (!buf) { fclose(f); return NULL; }

    size_t nread = fread(buf, 1, size, f);
    fclose(f);
    buf[nread] = '\0';
    return buf;
}

static const char *format_bytes(size_t bytes, char *buf, size_t buf_size) {
    if (bytes < 1024) {
        snprintf(buf, buf_size, "%zuB", bytes);
    } else if (bytes < 1024 * 1024) {
        snprintf(buf, buf_size, "%.1fKB", (double)bytes / 1024);
    } else if (bytes < 1024 * 1024 * 1024) {
        snprintf(buf, buf_size, "%.1fMB", (double)bytes / (1024 * 1024));
    } else {
        snprintf(buf, buf_size, "%.1fGB", (double)bytes / (1024 * 1024 * 1024));
    }
    return buf;
}

int main(int argc, char **argv) {
    const char *pipeline_file = NULL;
    const char *pipeline_text = NULL;
    const char *input_file = NULL;
    const char *output_file = NULL;
    int json_mode = 0;
    int quiet = 0;
    int progress = 0;
    int raw_stats = 0;

    /* Parse options */
    int argi = 1;
    while (argi < argc && argv[argi][0] == '-') {
        const char *opt = argv[argi];
        if (strcmp(opt, "-h") == 0 || strcmp(opt, "--help") == 0) {
            usage(argv[0]);
            return 0;
        } else if (strcmp(opt, "-v") == 0 || strcmp(opt, "--version") == 0) {
            printf("tranfi %s\n", tf_version());
            return 0;
        } else if (strcmp(opt, "-R") == 0 || strcmp(opt, "--recipes") == 0) {
            size_t n = tf_recipe_count();
            printf("Built-in recipes (%zu):\n\n", n);
            for (size_t i = 0; i < n; i++) {
                printf("  %-12s %s\n", tf_recipe_name(i), tf_recipe_description(i));
                printf("  %-12s %s\n", "", tf_recipe_dsl(i));
                printf("\n");
            }
            return 0;
        } else if (strcmp(opt, "-j") == 0) {
            json_mode = 1;
        } else if (strcmp(opt, "-q") == 0) {
            quiet = 1;
        } else if (strcmp(opt, "-p") == 0 || strcmp(opt, "--progress") == 0) {
            progress = 1;
        } else if (strcmp(opt, "--raw") == 0) {
            raw_stats = 1;
        } else if (strcmp(opt, "-f") == 0) {
            argi++;
            if (argi >= argc) {
                fprintf(stderr, "error: -f requires a file argument\n");
                return 1;
            }
            pipeline_file = argv[argi];
        } else if (strcmp(opt, "-i") == 0) {
            argi++;
            if (argi >= argc) {
                fprintf(stderr, "error: -i requires a file argument\n");
                return 1;
            }
            input_file = argv[argi];
        } else if (strcmp(opt, "-o") == 0) {
            argi++;
            if (argi >= argc) {
                fprintf(stderr, "error: -o requires a file argument\n");
                return 1;
            }
            output_file = argv[argi];
        } else {
            fprintf(stderr, "error: unknown option '%s'\n", opt);
            return 1;
        }
        argi++;
    }

    /* Get pipeline text */
    char *file_content = NULL;
    if (pipeline_file) {
        file_content = read_file(pipeline_file);
        if (!file_content) {
            fprintf(stderr, "error: cannot read file '%s'\n", pipeline_file);
            return 1;
        }
        pipeline_text = file_content;
    } else if (argi < argc) {
        pipeline_text = argv[argi];
    } else {
        fprintf(stderr, "error: no pipeline specified\n\n");
        usage(argv[0]);
        free(file_content);
        return 1;
    }

    /* Parse pipeline: recipe name → JSON recipe → DSL */
    char *error = NULL;
    tf_ir_plan *ir = NULL;
    size_t pt_len = strlen(pipeline_text);
    /* Skip leading whitespace for detection */
    const char *pt = pipeline_text;
    while (*pt == ' ' || *pt == '\t' || *pt == '\n' || *pt == '\r') pt++;

    if (*pt == '{') {
        /* JSON recipe */
        ir = tf_ir_from_json(pipeline_text, pt_len, &error);
    } else if (!strchr(pt, '|') && !strchr(pt, ' ')) {
        /* Single word — try built-in recipe */
        const char *recipe_dsl = tf_recipe_find_dsl(pt);
        if (recipe_dsl) {
            ir = tf_dsl_parse(recipe_dsl, strlen(recipe_dsl), &error);
        } else {
            ir = tf_dsl_parse(pipeline_text, pt_len, &error);
        }
    } else {
        ir = tf_dsl_parse(pipeline_text, pt_len, &error);
    }
    free(file_content);

    if (!ir) {
        fprintf(stderr, "error: %s\n", error ? error : "failed to parse pipeline");
        free(error);
        return 1;
    }

    /* Validate */
    if (tf_ir_validate(ir) != TF_OK) {
        fprintf(stderr, "error: %s\n", ir->error ? ir->error : "validation failed");
        tf_ir_plan_free(ir);
        return 1;
    }

    /* Schema inference (best-effort) */
    tf_ir_infer_schema(ir);

    /* JSON mode: print IR and exit */
    if (json_mode) {
        char *json = tf_ir_to_json(ir);
        if (json) {
            printf("%s\n", json);
            free(json);
        }
        tf_ir_plan_free(ir);
        return 0;
    }

    /* Compile to native pipeline */
    tf_pipeline *p = tf_pipeline_create_from_ir(ir);
    tf_ir_plan_free(ir);

    if (!p) {
        fprintf(stderr, "error: %s\n",
                tf_last_error() ? tf_last_error() : "failed to create pipeline");
        return 1;
    }

    /* Open I/O files */
    FILE *fin = stdin;
    FILE *fout = stdout;

    if (input_file) {
        fin = fopen(input_file, "rb");
        if (!fin) {
            fprintf(stderr, "error: cannot open input file '%s'\n", input_file);
            tf_pipeline_free(p);
            return 1;
        }
    }

    if (output_file) {
        fout = fopen(output_file, "wb");
        if (!fout) {
            fprintf(stderr, "error: cannot open output file '%s'\n", output_file);
            if (fin != stdin) fclose(fin);
            tf_pipeline_free(p);
            return 1;
        }
    }

    /* Decide whether to buffer output for report formatting.
     * When stdout is a TTY and --raw is not set, buffer main output
     * and try to render it as a rich report. Falls back to raw CSV
     * if the output doesn't look like a stats table. */
    int try_report = !raw_stats && !output_file && isatty(STDOUT_FILENO);

    /* Stream input → pipeline → output */
    uint8_t read_buf[READ_BUF_SIZE];
    size_t nread;
    size_t total_bytes = 0;

    /* Output buffer (used when try_report is true) */
    size_t out_cap = PULL_BUF_SIZE;
    size_t out_len = 0;
    char *out_buf = try_report ? malloc(out_cap) : NULL;

    while ((nread = fread(read_buf, 1, sizeof(read_buf), fin)) > 0) {
        if (tf_pipeline_push(p, read_buf, nread) != TF_OK) {
            fprintf(stderr, "error: %s\n",
                    tf_pipeline_error(p) ? tf_pipeline_error(p) : "push failed");
            free(out_buf);
            if (fin != stdin) fclose(fin);
            if (fout != stdout) fclose(fout);
            tf_pipeline_free(p);
            return 1;
        }

        total_bytes += nread;

        /* Pull any available output immediately (streaming) */
        uint8_t pull_buf[PULL_BUF_SIZE];
        size_t n;
        while ((n = tf_pipeline_pull(p, TF_CHAN_MAIN, pull_buf, sizeof(pull_buf))) > 0) {
            if (try_report && out_buf) {
                while (out_len + n > out_cap) {
                    out_cap *= 2;
                    char *tmp = realloc(out_buf, out_cap);
                    if (!tmp) { free(out_buf); out_buf = NULL; break; }
                    out_buf = tmp;
                }
                if (out_buf) {
                    memcpy(out_buf + out_len, pull_buf, n);
                    out_len += n;
                }
            } else {
                fwrite(pull_buf, 1, n, fout);
            }
        }

        /* Show progress */
        if (progress) {
            char bytes_str[32];
            format_bytes(total_bytes, bytes_str, sizeof(bytes_str));
            fprintf(stderr, "\r%s processed", bytes_str);
        }
    }

    /* Finish */
    if (tf_pipeline_finish(p) != TF_OK) {
        fprintf(stderr, "error: %s\n",
                tf_pipeline_error(p) ? tf_pipeline_error(p) : "finish failed");
        free(out_buf);
        if (fin != stdin) fclose(fin);
        if (fout != stdout) fclose(fout);
        tf_pipeline_free(p);
        return 1;
    }

    /* Pull remaining output */
    uint8_t pull_buf[PULL_BUF_SIZE];
    size_t n;
    while ((n = tf_pipeline_pull(p, TF_CHAN_MAIN, pull_buf, sizeof(pull_buf))) > 0) {
        if (try_report && out_buf) {
            while (out_len + n > out_cap) {
                out_cap *= 2;
                char *tmp = realloc(out_buf, out_cap);
                if (!tmp) { free(out_buf); out_buf = NULL; break; }
                out_buf = tmp;
            }
            if (out_buf) {
                memcpy(out_buf + out_len, pull_buf, n);
                out_len += n;
            }
        } else {
            fwrite(pull_buf, 1, n, fout);
        }
    }

    /* Try report formatting, fall back to raw */
    if (try_report && out_buf && out_len > 0) {
        char *report = tf_report_format(out_buf, out_len, 1);
        if (report) {
            fwrite(report, 1, strlen(report), fout);
            free(report);
        } else {
            fwrite(out_buf, 1, out_len, fout);
        }
    }
    free(out_buf);
    fflush(fout);

    if (progress) {
        char bytes_str[32];
        format_bytes(total_bytes, bytes_str, sizeof(bytes_str));
        fprintf(stderr, "\r%s processed (done)\n", bytes_str);
    }

    /* Pull errors to stderr */
    while ((n = tf_pipeline_pull(p, TF_CHAN_ERRORS, pull_buf, sizeof(pull_buf))) > 0) {
        fwrite(pull_buf, 1, n, stderr);
    }

    /* Pull stats to stderr (unless quiet) */
    if (!quiet) {
        while ((n = tf_pipeline_pull(p, TF_CHAN_STATS, pull_buf, sizeof(pull_buf))) > 0) {
            fwrite(pull_buf, 1, n, stderr);
        }
    }

    /* Cleanup */
    if (fin != stdin) fclose(fin);
    if (fout != stdout) fclose(fout);
    tf_pipeline_free(p);
    return 0;
}
