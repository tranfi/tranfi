import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './test',
  testMatch: '**/*.e2e.js',
  timeout: 30000,
  retries: 0,
  use: {
    baseURL: 'http://localhost:5199',
    headless: true
  },
  projects: [
    { name: 'chromium', use: { browserName: 'chromium' } }
  ],
  webServer: {
    command: 'npx vite --port 5199',
    port: 5199,
    reuseExistingServer: true,
    timeout: 15000
  }
})
