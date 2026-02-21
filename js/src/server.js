/**
 * server.js — HTTP server for tranfi serve mode.
 *
 * Serves the app frontend and provides API endpoints backed by
 * the native C core (N-API). No external dependencies — uses
 * built-in http, fs, path, url modules only.
 */

import { createServer } from 'http'
import { readFileSync, readdirSync, statSync, readFile as readFileCb, existsSync } from 'fs'
import { join, extname, resolve, basename } from 'path'
import { URL } from 'url'
import { pipeline, recipes } from './index.js'
import nativeBinding from './native.js'

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.mjs': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.wasm': 'application/wasm',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
  '.eot': 'application/vnd.ms-fontobject',
  '.map': 'application/json'
}

const DATA_EXTENSIONS = new Set(['.csv', '.jsonl', '.tsv', '.txt', '.json'])

function esc (s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')
}

function csvToHtml (csv, label) {
  if (!csv || !csv.trim()) return ''
  const lines = csv.trim().split('\n')
  const header = lines[0].split(',')
  const body = lines.slice(1)
  const maxRows = 500
  const truncated = body.length > maxRows
  const displayBody = truncated ? body.slice(0, maxRows) : body

  let h = `<div style="font-size:12px;color:#888;margin-bottom:6px">`
  h += `${esc(label)} (${body.length} rows${truncated ? ', showing first ' + maxRows : ''})</div>`
  h += '<div style="overflow-x:auto;max-height:500px;overflow-y:auto">'
  h += '<table style="width:100%;border-collapse:collapse;font-size:13px;font-family:monospace">'
  h += '<thead><tr>'
  for (const col of header) {
    h += '<th style="padding:6px 12px;text-align:left;border-bottom:2px solid #ddd;'
    h += 'background:#f8f8f8;font-weight:600;position:sticky;top:0">'
    h += esc(col.trim()) + '</th>'
  }
  h += '</tr></thead><tbody>'
  for (const row of displayBody) {
    h += '<tr>'
    for (const cell of row.split(',')) {
      h += '<td style="padding:4px 12px;text-align:left;border-bottom:1px solid #eee">'
      h += esc(cell.trim()) + '</td>'
    }
    h += '</tr>'
  }
  h += '</tbody></table></div>'
  return h
}

function jsonBody (req) {
  return new Promise((resolve, reject) => {
    const chunks = []
    req.on('data', c => chunks.push(c))
    req.on('end', () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString()))
      } catch (e) {
        reject(new Error('Invalid JSON body'))
      }
    })
    req.on('error', reject)
  })
}

function sendJson (res, data, status = 200) {
  const body = JSON.stringify(data)
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*'
  })
  res.end(body)
}

function sendError (res, msg, status = 400) {
  sendJson(res, { error: msg }, status)
}

function isSafeName (name) {
  return name && !name.includes('..') && !name.includes('/') && !name.includes('\\')
}

export function startServer ({ port = 3000, dataDir, appDir }) {
  dataDir = resolve(dataDir)
  appDir = resolve(appDir)

  if (!existsSync(appDir)) {
    throw new Error(`App directory not found: ${appDir}`)
  }
  if (!existsSync(dataDir)) {
    throw new Error(`Data directory not found: ${dataDir}`)
  }

  // Read and patch index.html once
  const indexPath = join(appDir, 'index.html')
  let indexHtml = ''
  if (existsSync(indexPath)) {
    indexHtml = readFileSync(indexPath, 'utf-8')
    const config = `<script>window.__TRANFI_SERVER__={api:'/api'}</script>`
    indexHtml = indexHtml.replace('</head>', config + '</head>')
  }

  const server = createServer(async (req, res) => {
    const url = new URL(req.url, `http://${req.headers.host}`)
    const pathname = url.pathname

    // CORS preflight
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
      })
      res.end()
      return
    }

    try {
      // API routes
      if (pathname === '/api/files' && req.method === 'GET') {
        return handleFiles(res, dataDir)
      }
      if (pathname === '/api/file' && req.method === 'GET') {
        const name = url.searchParams.get('name')
        const head = parseInt(url.searchParams.get('head') || '20', 10)
        return handleFilePreview(res, dataDir, name, head)
      }
      if (pathname === '/api/run' && req.method === 'POST') {
        const body = await jsonBody(req)
        return await handleRun(res, dataDir, body)
      }
      if (pathname === '/api/recipes' && req.method === 'GET') {
        return await handleRecipes(res)
      }
      if (pathname === '/api/version' && req.method === 'GET') {
        const v = nativeBinding ? nativeBinding.version() : 'unknown'
        return sendJson(res, { version: v })
      }

      // Static files
      serveStatic(res, appDir, pathname, indexHtml)
    } catch (err) {
      sendError(res, err.message, 500)
    }
  })

  server.listen(port, () => {
    const v = nativeBinding ? nativeBinding.version() : 'unknown'
    process.stderr.write(`tranfi ${v} server\n`)
    process.stderr.write(`  App:  ${appDir}\n`)
    process.stderr.write(`  Data: ${dataDir}\n`)
    process.stderr.write(`  http://localhost:${port}\n`)
  })

  return server
}

function handleFiles (res, dataDir) {
  const entries = readdirSync(dataDir)
  const files = []
  for (const name of entries) {
    const ext = extname(name).toLowerCase()
    if (!DATA_EXTENSIONS.has(ext)) continue
    try {
      const st = statSync(join(dataDir, name))
      if (st.isFile()) {
        files.push({ name, size: st.size })
      }
    } catch {}
  }
  files.sort((a, b) => a.name.localeCompare(b.name))
  sendJson(res, { files })
}

function handleFilePreview (res, dataDir, name, head) {
  if (!isSafeName(name)) return sendError(res, 'Invalid file name')
  const filePath = join(dataDir, name)
  if (!existsSync(filePath)) return sendError(res, 'File not found', 404)

  const content = readFileSync(filePath, 'utf-8')
  const lines = content.split('\n')
  const preview = lines.slice(0, head).join('\n')
  sendJson(res, { preview, lines: lines.length })
}

async function handleRun (res, dataDir, body) {
  const { file, dsl } = body
  if (!dsl) return sendError(res, 'Missing dsl field')
  if (!file) return sendError(res, 'Missing file field')
  if (!isSafeName(file)) return sendError(res, 'Invalid file name')

  const filePath = join(dataDir, file)
  if (!existsSync(filePath)) return sendError(res, `File not found: ${file}`, 404)

  const p = pipeline(dsl)
  const result = await p.run({ inputFile: filePath })

  const output = result.outputText
  const stats = result.statsText

  sendJson(res, {
    output: csvToHtml(output, 'Output'),
    stats: csvToHtml(stats, 'Stats')
  })
}

async function handleRecipes (res) {
  const list = await recipes()
  sendJson(res, { recipes: list })
}

function serveStatic (res, appDir, pathname, indexHtml) {
  // Normalize path
  let filePath = join(appDir, pathname === '/' ? 'index.html' : pathname)

  // Security: ensure resolved path is within appDir
  const resolved = resolve(filePath)
  if (!resolved.startsWith(appDir)) {
    res.writeHead(403)
    res.end('Forbidden')
    return
  }

  // SPA fallback: if file doesn't exist, serve index.html
  if (!existsSync(resolved) || statSync(resolved).isDirectory()) {
    if (indexHtml) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' })
      res.end(indexHtml)
    } else {
      res.writeHead(404)
      res.end('Not found')
    }
    return
  }

  // Serve index.html with injected config
  if (basename(resolved) === 'index.html' && indexHtml) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' })
    res.end(indexHtml)
    return
  }

  // Serve regular file
  const ext = extname(resolved).toLowerCase()
  const contentType = MIME_TYPES[ext] || 'application/octet-stream'

  readFileCb(resolved, (err, data) => {
    if (err) {
      res.writeHead(500)
      res.end('Internal error')
      return
    }
    res.writeHead(200, { 'Content-Type': contentType })
    res.end(data)
  })
}
