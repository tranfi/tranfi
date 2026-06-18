/*
 * config.h -- Tranfi C feature-test configuration.
 *
 * Build files also define these macros at compile-command level so they are
 * visible before any system header in every translation unit. This header
 * documents the source contract and protects internal/public headers included
 * first.
 */

#ifndef TF_CONFIG_H
#define TF_CONFIG_H

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif

#endif /* TF_CONFIG_H */
