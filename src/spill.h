/*
 * spill.h -- internal secure spill-file helper.
 *
 * Spill users create one private session directory under a host-approved root,
 * then create individual run files inside it with exclusive openat().
 */

#ifndef TF_SPILL_H
#define TF_SPILL_H

#include "tranfi.h"
#include <stdio.h>

typedef struct tf_spill_session tf_spill_session;

int   tf_spill_session_create(const char *root, tf_spill_session **out);
int   tf_spill_open_run(tf_spill_session *s, const char *label, int *fd, char **path_out);
FILE *tf_spill_open_run_file(tf_spill_session *s, const char *label, char **path_out);
int   tf_spill_cleanup(tf_spill_session *s);

#endif
