/*
 * config.h -- Tranfi C feature-test configuration.
 *
 * Build files also define this macro at compile-command level so it is visible
 * before any system header in every translation unit. This header documents the
 * source contract and protects internal/public headers included first.
 */

#ifndef TF_CONFIG_H
#define TF_CONFIG_H

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#endif /* TF_CONFIG_H */
