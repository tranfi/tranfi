'use strict'

const fs = require('fs')
const path = require('path')

const pkgDir = path.resolve(__dirname, '..')
const repoDir = path.resolve(pkgDir, '..')
const srcDir = path.join(repoDir, 'src')
const outDir = path.join(pkgDir, 'csrc')

if (!fs.existsSync(srcDir)) {
  console.log('sync-csrc: src/ not found (published package), skipping')
  process.exit(0)
}

fs.rmSync(outDir, { recursive: true, force: true })
fs.mkdirSync(outDir, { recursive: true })

for (const name of fs.readdirSync(srcDir)) {
  if (name.endsWith('.c') || name.endsWith('.h')) {
    fs.copyFileSync(path.join(srcDir, name), path.join(outDir, name))
  }
}
