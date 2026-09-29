#!/usr/bin/env node

/**
 * cli.js — Tranfi CLI for Node.js.
 *
 * Usage:
 *   tranfi 'csv | filter "age > 25" | csv'  < in.csv
 *   tranfi -f pipeline.tf < in.csv > out.csv
 *   tranfi profile < data.csv
 */

const { pipeline, compileDsl, compileToSql, recipes } = require('./index.js')
const { readFileSync, writeFileSync, existsSync, openSync, closeSync } = require('fs')
const { resolve, dirname } = require('path')
const nativeBinding = require('./native.js')

function usage() {
  process.stderr.write(`Usage: tranfi [OPTIONS] PIPELINE
       tranfi [OPTIONS] -f FILE

Streaming ETL with a pipe-style DSL.

Examples:
  tranfi 'csv | csv'                           # passthrough
  tranfi 'csv | filter "age > 25" | csv'       # filter rows
  tranfi 'csv | select name,age | csv'         # select columns
  tranfi 'csv | head 10 | csv'                 # first N rows
  tranfi 'csv | top-k 10 age | csv'            # bounded top rows by column
  tranfi 'csv | stats | jsonl'                 # aggregate stats
  tranfi profile                               # built-in recipe

Options:
  -f FILE   Read pipeline from file
  -i FILE   Read input from file instead of stdin
  -o FILE   Write output to file instead of stdout
  -j        Compile only, output JSON plan
  --target native|json|sql  Execute or compile a pipeline
  --dialect NAME           SQL dialect (duckdb)
  --allow-blocking         Permit full-input operations on known-small data
  --fail-on-blocking       Reject blocking operations (default)
  --memory SIZE            Native memory policy, e.g. max:64MB
  --spill-dir DIR          Existing private spill directory
  --stats-json FILE        Write stats NDJSON; - means stderr
  -q        Quiet mode (suppress stats)
  -v        Show version
  -R        List built-in recipes
  -h        Show this help
`)
}

function findAppDir () {
  // Try known locations relative to this file
  const candidates = [
    resolve(__dirname, '../../app/dist'),          // dev: js/src/ -> ../../app/dist
    resolve(__dirname, '../app'),                 // npm package: src/ -> ../app/
  ]
  for (const dir of candidates) {
    if (existsSync(resolve(dir, 'index.html'))) return dir
  }
  return null
}

async function serveCommand (argv) {
  const { startServer } = require('./server.js')
  let dataDir = '.'
  let appDir = null
  let port = 3000

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '-d' || arg === '--data') dataDir = argv[++i]
    else if (arg === '-a' || arg === '--app') appDir = argv[++i]
    else if (arg === '-p' || arg === '--port') port = parseInt(argv[++i], 10)
    else if (arg === '-h' || arg === '--help') {
      process.stderr.write(`Usage: tranfi serve [OPTIONS]

Serve the tranfi app with a native backend.

Options:
  -d, --data DIR   Data directory (default: .)
  -a, --app DIR    App dist directory (auto-detect)
  -p, --port PORT  Port (default: 3000)
  -h, --help       Show this help
`)
      process.exit(0)
    }
  }

  if (!appDir) appDir = findAppDir()
  if (!appDir) {
    process.stderr.write('error: app directory not found. Use --app to specify it.\n')
    process.exit(1)
  }

  startServer({ port, dataDir: resolve(dataDir), appDir: resolve(appDir) })
}

async function main() {
  const argv = process.argv.slice(2)

  // Check for serve subcommand
  if (argv[0] === 'serve') {
    return serveCommand(argv.slice(1))
  }

  let pipelineFile = null
  let pipelineText = null
  let inputFile = null
  let outputFile = null
  let jsonMode = false
  let quiet = false
  let allowBlocking = false
  let failOnBlocking = false
  let memory, spillDir, statsFile
  let target = 'native'
  let dialect = 'duckdb'
  function valueAt(i, inline, flag) {
    const value = inline === undefined ? argv[i + 1] : inline
    if (!value || value.startsWith('--')) throw new Error(`${flag} requires a value`)
    return value
  }

  // Parse args
  for (let i = 0; i < argv.length; i++) {
    const [arg, ...assigned] = argv[i].split('=')
    const inline = assigned.length ? assigned.join('=') : undefined
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
    else if (arg === '-j' || arg === '--json') jsonMode = true
    else if (arg === '-q' || arg === '--quiet') quiet = true
    else if (arg === '--allow-blocking') allowBlocking = true
    else if (arg === '--fail-on-blocking') failOnBlocking = true
    else if (['-f', '--file', '-i', '--input', '-o', '--output', '--memory', '--spill-dir', '--stats-json', '--target', '--dialect'].includes(arg)) {
      const value = valueAt(i, inline, arg)
      if (inline === undefined) i++
      if (arg === '-f' || arg === '--file') pipelineFile = value
      else if (arg === '-i' || arg === '--input') inputFile = value
      else if (arg === '-o' || arg === '--output') outputFile = value
      else if (arg === '--memory') memory = value
      else if (arg === '--spill-dir') spillDir = value
      else if (arg === '--stats-json') statsFile = value
      else if (arg === '--target') target = value
      else if (arg === '--dialect') dialect = value
    }
    else if (!argv[i].startsWith('-')) {
      if (pipelineText !== null) throw new Error('only one pipeline argument is allowed')
      pipelineText = argv[i]
    }
    else {
      process.stderr.write(`error: unknown option '${arg}'\n`)
      process.exit(1)
    }
  }

  if (allowBlocking && failOnBlocking) throw new Error('--allow-blocking conflicts with --fail-on-blocking')
  if (!['native', 'json', 'sql'].includes(target)) throw new Error('unknown --target; expected native, json or sql')

  // Get pipeline text
  if (pipelineFile) {
    pipelineText = readFileSync(pipelineFile, 'utf-8')
  }
  if (!pipelineText) {
    process.stderr.write('error: no pipeline specified\n\n')
    usage()
    process.exit(1)
  }

  const named = (await recipes()).find(recipe => recipe.name === pipelineText.trim())
  if (named) pipelineText = named.dsl
  if (target === 'sql') {
    process.stdout.write(await compileToSql(pipelineText, { dialect }) + '\n')
    return
  }

  if (jsonMode || target === 'json') {
    const json = await compileDsl(pipelineText)
    process.stdout.write(json + '\n')
    process.exit(0)
  }

  const p = pipeline(pipelineText)
  const options = {
    inputFile: inputFile || undefined,
    inputStream: inputFile ? undefined : process.stdin,
    allowBlocking, memory, spillDir, allowFs: true, allowRulesFile: true
  }
  let result
  if (outputFile) {
    let fd
    try {
      result = await p.run({
        ...options,
        collectOutput: false,
        onOutput(chunk) {
          // Open only after plan validation; a rejected plan must not truncate output.
          if (fd === undefined) fd = openSync(outputFile, 'w')
          writeFileSync(fd, chunk)
        }
      })
      if (fd === undefined) fd = openSync(outputFile, 'w')
    } finally {
      if (fd !== undefined) closeSync(fd)
    }
  } else {
    result = await p.writeTo(process.stdout, { ...options, end: false })
  }

  // Errors to stderr
  if (result.errors.length > 0) {
    process.stderr.write(result.errors)
  }

  if (statsFile && statsFile !== '-') writeFileSync(statsFile, result.stats)
  else if ((statsFile || !quiet) && result.stats.length > 0) process.stderr.write(result.stats)
}

main().catch(err => {
  process.stderr.write(`error: ${err.message}\n`)
  process.exit(1)
})
