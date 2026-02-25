/*
 * report.h — Rich ANSI terminal report for stats output.
 */

#ifndef TF_REPORT_H
#define TF_REPORT_H

#include <stddef.h>

/*
 * Format stats CSV into a rich terminal report with ANSI colors and
 * Unicode sparkline histograms.
 *
 * stats_csv: the CSV output from the stats channel (column,count,avg,...)
 * stats_len: byte length
 * use_color: 1 = ANSI escape codes, 0 = plain text
 *
 * Returns a malloc'd string (caller frees), or NULL on error.
 */
char *tf_report_format(const char *stats_csv, size_t stats_len, int use_color);

#endif
