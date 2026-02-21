#!/usr/bin/env node
/**
 * build-site.js — Render README files to a static site.
 *
 * Usage: node scripts/build-site.js [--out _site] [--base /]
 *
 * Renders:
 *   README.md    → _site/index.html
 *   js/README.md → _site/js/index.html
 *   py/README.md → _site/py/index.html
 *
 * If app/dist/ exists, copies it to _site/app/
 */

import { readFileSync, writeFileSync, mkdirSync, cpSync, existsSync } from 'fs'
import { dirname, resolve } from 'path'
import { Marked } from 'marked'

const args = process.argv.slice(2)
let outDir = '_site'
let base = '/'

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--out') outDir = args[++i]
  if (args[i] === '--base') base = args[++i]
}

const root = resolve(dirname(new URL(import.meta.url).pathname), '..')
const out = resolve(root, outDir)

const marked = new Marked()

const pages = [
  { src: 'README.md', dest: 'index.html', title: 'tranfi' },
  { src: 'js/README.md', dest: 'js/index.html', title: 'tranfi — Node.js / WASM' },
  { src: 'py/README.md', dest: 'py/index.html', title: 'tranfi — Python' }
]

function template (title, body, currentPath) {
  const nav = [
    { href: `${base}`, label: 'Home', path: '/' },
    { href: `${base}js/`, label: 'Node.js', path: '/js/' },
    { href: `${base}py/`, label: 'Python', path: '/py/' },
    { href: `${base}app/`, label: 'App', path: '/app/' }
  ]

  const navHtml = nav.map(n => {
    const active = currentPath === n.path ? ' class="active"' : ''
    return `<a href="${n.href}"${active}>${n.label}</a>`
  }).join('\n      ')

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${title}</title>
  <style>
    :root { --fg: #1f2328; --bg: #fff; --muted: #656d76; --border: #d0d7de; --link: #0969da; --code-bg: #f6f8fa; --nav-bg: #f6f8fa; }
    @media (prefers-color-scheme: dark) {
      :root { --fg: #e6edf3; --bg: #0d1117; --muted: #8b949e; --border: #30363d; --link: #58a6ff; --code-bg: #161b22; --nav-bg: #161b22; }
    }
    * { box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Noto Sans, Helvetica, Arial, sans-serif; color: var(--fg); background: var(--bg); margin: 0; line-height: 1.6; }
    nav { background: var(--nav-bg); border-bottom: 1px solid var(--border); padding: 12px 24px; display: flex; gap: 20px; align-items: center; }
    nav a { color: var(--muted); text-decoration: none; font-size: 14px; font-weight: 500; }
    nav a:hover, nav a.active { color: var(--fg); }
    .content { max-width: 960px; margin: 0 auto; padding: 32px 24px; }
    h1 { font-size: 2em; border-bottom: 1px solid var(--border); padding-bottom: 0.3em; }
    h2 { font-size: 1.5em; border-bottom: 1px solid var(--border); padding-bottom: 0.3em; margin-top: 1.5em; }
    h3 { font-size: 1.25em; margin-top: 1.3em; }
    a { color: var(--link); text-decoration: none; }
    a:hover { text-decoration: underline; }
    code { font-family: ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace; font-size: 85%; background: var(--code-bg); padding: 0.2em 0.4em; border-radius: 6px; }
    pre { background: var(--code-bg); border-radius: 6px; padding: 16px; overflow-x: auto; }
    pre code { background: none; padding: 0; font-size: 85%; }
    table { border-collapse: collapse; width: 100%; margin: 16px 0; }
    th, td { padding: 8px 16px; border: 1px solid var(--border); text-align: left; }
    th { background: var(--code-bg); font-weight: 600; }
    blockquote { border-left: 4px solid var(--border); margin: 0; padding: 0 16px; color: var(--muted); }
    img { max-width: 100%; }
    .mermaid { margin: 16px 0; }
  </style>
</head>
<body>
  <nav>
    <strong style="margin-right: 8px">tranfi</strong>
    ${navHtml}
  </nav>
  <div class="content">
    ${body}
  </div>
  <script type="module">
    import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs'
    mermaid.initialize({ startOnLoad: true, theme: window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'default' })
  </script>
</body>
</html>`
}

// Render pages
for (const page of pages) {
  const srcPath = resolve(root, page.src)
  if (!existsSync(srcPath)) {
    console.warn(`  skip: ${page.src} (not found)`)
    continue
  }

  let md = readFileSync(srcPath, 'utf-8')

  // Convert mermaid code blocks to <pre class="mermaid"> for client-side rendering
  md = md.replace(/```mermaid\n([\s\S]*?)```/g, (_, code) => {
    return `<pre class="mermaid">\n${code}</pre>`
  })

  // Fix relative links: (py/) → (base + py/), (js/) → (base + js/)
  if (base !== '/') {
    md = md.replace(/\]\((?!http|#|\/)(.*?)\)/g, `](${base}$1)`)
  }

  const body = marked.parse(md)
  const currentPath = page.dest === 'index.html' ? '/' : `/${page.dest.replace('/index.html', '/')}`
  const html = template(page.title, body, currentPath)

  const destPath = resolve(out, page.dest)
  mkdirSync(dirname(destPath), { recursive: true })
  writeFileSync(destPath, html)
  console.log(`  ${page.src} → ${page.dest}`)
}

// Copy app/dist if it exists
const appDist = resolve(root, 'app/dist')
if (existsSync(appDist)) {
  const appOut = resolve(out, 'app')
  cpSync(appDist, appOut, { recursive: true })
  console.log('  app/dist → app/')
}

console.log(`Site built → ${outDir}/`)
