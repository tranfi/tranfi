import { test, expect } from '@playwright/test'
import { fileURLToPath } from 'url'
import path from 'path'
import { execSync } from 'child_process'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const dataPath = path.join(__dirname, 'data.csv')

// Helper: insert a block between existing blocks using the "+" menu
// Clicks the first "+" insert point (between first and second block)
async function insertBlock(page, blockName) {
  const insertBtn = page.locator('.add-block-here').first()
  await insertBtn.click()
  const menuItem = page.getByText(blockName, { exact: true }).last()
  await menuItem.click()
}

// Helper: upload file via JSEE's file input and click JSEE's run button
async function uploadAndRun(page, expectedCells) {
  // JSEE renders its own file input
  const fileInput = page.locator('#jsee-output input[type="file"]')
  await fileInput.setInputFiles(dataPath)

  // Click JSEE's run button
  const runBtn = page.locator('#jsee-output').getByRole('button', { name: /run/i })
  await runBtn.click()

  // Wait for output table to appear
  const output = page.locator('#jsee-output table').first()
  await expect(output).toBeVisible({ timeout: 30000 })

  // Verify expected content
  const text = await output.textContent()
  for (const cell of expectedCells) {
    expect(text).toContain(cell)
  }

  return text
}

test.describe('tranfi app (dev server)', () => {
  test.beforeAll(async () => {
    execSync('npm run build', { cwd: path.join(__dirname, '..'), timeout: 30000 })
  })

  test('csv passthrough with block editor', async ({ page }) => {
    await page.goto('/')
    await page.waitForFunction(() => window.JSEE, { timeout: 10000 })

    // Default blocks: CSV Input + CSV Output — should pass through all rows
    await uploadAndRun(page, ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'])
  })

  test('insert head block and run', async ({ page }) => {
    await page.goto('/')
    await page.waitForFunction(() => window.JSEE, { timeout: 10000 })

    // Insert Head block between CSV Input and CSV Output via "+" menu
    await insertBlock(page, 'Head')

    // Set head n = 2
    const nInput = page.locator('.sidebar input[type="number"]').first()
    await nInput.fill('2')

    // Verify DSL is correct order: csv | head 2 | csv
    const dslDisplay = page.locator('.dsl-display code')
    await expect(dslDisplay).toContainText('csv | head 2 | csv')

    const text = await uploadAndRun(page, ['Alice', 'Bob'])
    expect(text).not.toContain('Eve')
  })

  test('insert filter block and run', async ({ page }) => {
    await page.goto('/')
    await page.waitForFunction(() => window.JSEE, { timeout: 10000 })

    // Insert Filter block between CSV Input and CSV Output via "+" menu
    await insertBlock(page, 'Filter')

    // Set expression in the Filter block's Expression input
    const exprInput = page.getByLabel('Expression')
    await exprInput.fill('col(age) > 30')

    const text = await uploadAndRun(page, ['Charlie', 'Eve'])
    expect(text).not.toContain('Bob')
  })

  test('export standalone HTML', async ({ page }) => {
    await page.goto('/')
    await page.waitForFunction(() => window.JSEE, { timeout: 10000 })

    // Insert Head block between CSV Input and CSV Output
    const insertBtn = page.locator('.add-block-here').first()
    await insertBtn.click()
    await page.getByText('Head', { exact: true }).last().click()

    // Click Export HTML button
    const [download] = await Promise.all([
      page.waitForEvent('download'),
      page.getByRole('button', { name: /export html/i }).click()
    ])

    expect(download.suggestedFilename()).toBe('tranfi-pipeline.html')

    // Read the downloaded file and verify it's valid standalone HTML
    const downloadPath = await download.path()
    const fs = await import('fs')
    const html = fs.readFileSync(downloadPath, 'utf-8')

    // Must be a complete HTML document
    expect(html).toContain('<!DOCTYPE html>')
    expect(html).toContain('<title>Tranfi')

    // Must have the JSEE runtime inlined
    expect(html).toContain('JSEE')

    // Must have model code in hidden storage
    expect(html).toContain('id="hidden-storage"')
    expect(html).toContain('data-src="/wasm/tranfi-runner.js"')
    expect(html).toContain('data-src="/wasm/tranfi_core.js"')

    // Must have the current DSL baked in
    expect(html).toContain('head 10')

    // Must have the schema
    expect(html).toContain('"type": "async-function"')

    // Save it so we can test it opens
    fs.writeFileSync(path.join(__dirname, 'exported-from-ui.html'), html)
  })

  test('exported UI HTML runs pipeline', async ({ page }) => {
    const exportedPath = path.join(__dirname, 'exported-from-ui.html')
    await page.goto(`file://${exportedPath}`)

    // Wait for JSEE to initialize
    await page.waitForFunction(() => window.env && window.env.data, { timeout: 30000 })

    // Upload file and run
    const fileInput = page.locator('input[type="file"]')
    await fileInput.setInputFiles(dataPath)
    const runBtn = page.getByRole('button', { name: /run/i })
    await runBtn.click()

    // Wait for output table
    const output = page.locator('table').first()
    await expect(output).toBeVisible({ timeout: 30000 })

    // Head 10 should pass all 5 rows (data has only 5)
    const text = await output.textContent()
    expect(text).toContain('Alice')
    expect(text).toContain('Eve')
  })

  test('dsl display updates when blocks change', async ({ page }) => {
    await page.goto('/')
    await page.waitForFunction(() => window.JSEE, { timeout: 10000 })

    // Default: csv | csv
    const dslDisplay = page.locator('.dsl-display code')
    await expect(dslDisplay).toContainText('csv | csv')

    // Insert Head block
    await insertBlock(page, 'Head')
    await expect(dslDisplay).toContainText('head')
  })
})

test.describe('exported standalone HTML', () => {
  const exportedPath = path.join(__dirname, 'exported.html')

  test('runs pipeline from exported single-file HTML', async ({ page }) => {
    await page.goto(`file://${exportedPath}`)

    // Wait for JSEE to initialize
    await page.waitForFunction(() => window.env && window.env.data, { timeout: 30000 })

    // Find the file input and upload
    const fileInput = page.locator('input[type="file"]')
    await fileInput.setInputFiles(dataPath)

    // The DSL should already be set to default "csv | head 3 | csv"
    // Click Run button (JSEE renders its own run button)
    const runBtn = page.getByRole('button', { name: /run/i })
    await runBtn.click()

    // Wait for output table
    const output = page.locator('table').first()
    await expect(output).toBeVisible({ timeout: 30000 })

    // Should have first 3 rows
    const text = await output.textContent()
    expect(text).toContain('Alice')
    expect(text).toContain('Bob')
    expect(text).toContain('Charlie')
    // Should NOT have rows beyond head 3
    expect(text).not.toContain('Diana')
  })
})
