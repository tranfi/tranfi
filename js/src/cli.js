#!/usr/bin/env node

/**
 * cli.js — Tranfi CLI for Node.js.
 *
 * Usage:
 *   tranfi 'csv | filter "age > 25" | csv'  < in.csv
 *   tranfi -f pipeline.tf < in.csv > out.csv
 *   tranfi profile < data.csv
 */

import { pipeline, compileDsl, recipes } from './index.js'
import { readFileSync, writeFileSync } from 'fs'
import nativeBinding from './native.js'

function usage() {
  process.stderr.write(`Usage: tranfi [OPTIONS] PIPELINE
       tranfi [OPTIONS] -f FILE

Streaming ETL with a pipe-style DSL.

Examples:
  tranfi 'csv | csv'                           # passthrough
  tranfi 'csv | filter "age > 25" | csv'       # filter rows
  tranfi 'csv | select name,age | csv'         # select columns
  tranfi 'csv | head 10 | csv'                 # first N rows
  tranfi 'csv | sort age | csv'                # sort by column
  tranfi 'csv | stats | jsonl'                 # aggregate stats
  tranfi profile                               # built-in recipe

Options:
  -f FILE   Read pipeline from file
  -i FILE   Read input from file instead of stdin
  -o FILE   Write output to file instead of stdout
  -j        Compile only, output JSON plan
  -q        Quiet mode (suppress stats)
  -v        Show version
  -R        List built-in recipes
  -h        Show this help
`)
}

async function main() {
  const argv = process.argv.slice(2)
  let pipelineFile = null
  let pipelineText = null
  let inputFile = null
  let outputFile = null
  let jsonMode = false
  let quiet = false

  // Parse args
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '-h' || arg === '--help') { usage(); process.exit(0) }
    else if (arg === '-v' || arg === '--version') {
      const v = nativeBinding ? nativeBinding.version() : 'unknown'
      process.stdout.write(`tranfi ${v}\n`)
      process.exit(0)
    }
    else if (arg === '-R' || arg === '--recipes') {
      const list = await recipes()
      process.stdout.write(`Built-in recipes (${list.length}):\n\n`)
      for (const r of list) {
        process.stdout.write(`  ${r.name.padEnd(12)} ${r.description}\n`)
        process.stdout.write(`  ${''.padEnd(12)} ${r.dsl}\n\n`)
      }
      process.exit(0)
    }
    else if (arg === '-j') jsonMode = true
    else if (arg === '-q') quiet = true
    else if (arg === '-f') { pipelineFile = argv[++i] }
    else if (arg === '-i') { inputFile = argv[++i] }
    else if (arg === '-o') { outputFile = argv[++i] }
    else if (!arg.startsWith('-')) { pipelineText = arg }
    else {
      process.stderr.write(`error: unknown option '${arg}'\n`)
      process.exit(1)
    }
  }

  // Get pipeline text
  if (pipelineFile) {
    pipelineText = readFileSync(pipelineFile, 'utf-8')
  }
  if (!pipelineText) {
    process.stderr.write('error: no pipeline specified\n\n')
    usage()
    process.exit(1)
  }

  // JSON mode
  if (jsonMode) {
    const json = await compileDsl(pipelineText)
    process.stdout.write(json + '\n')
    process.exit(0)
  }

  // Create and run pipeline
  const p = pipeline(pipelineText)
  const input = inputFile ? undefined : await readStdin()
  const result = await p.run({
    inputFile: inputFile || undefined,
    input
  })

  // Write output
  if (outputFile) {
    writeFileSync(outputFile, result.output)
  } else {
    process.stdout.write(result.output)
  }

  // Errors to stderr
  if (result.errors.length > 0) {
    process.stderr.write(result.errors)
  }

  // Stats to stderr (unless quiet)
  if (!quiet && result.stats.length > 0) {
    process.stderr.write(result.stats)
  }
}

function readStdin() {
  return new Promise((resolve) => {
    const chunks = []
    process.stdin.on('data', chunk => chunks.push(chunk))
    process.stdin.on('end', () => resolve(Buffer.concat(chunks)))
  })
}

main().catch(err => {
  process.stderr.write(`error: ${err.message}\n`)
  process.exit(1)
})
