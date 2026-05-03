import { chromium } from 'playwright';

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage();
const errors = [];
page.on('console', msg => { if (msg.type() === 'error') errors.push(msg.text()); });
page.on('pageerror', err => errors.push(err.message));

await page.goto('http://localhost:5173/broker-recommend', { waitUntil: 'networkidle', timeout: 15000 });
await page.waitForTimeout(2000);

// Expand first broker
const firstBroker = page.locator('button:has(span:has-text("只"))').first();
await firstBroker.click();
await page.waitForTimeout(500);

// Click 累计收益 in broker expanded view to test sort
const sortBtns = page.locator('th button:has-text("累计收益")');
console.log('Sort buttons found:', await sortBtns.count());

// Click first 累计收益 button → desc
await sortBtns.nth(0).click();
await page.waitForTimeout(300);
let html = await page.content();
console.log('Broker view 累计收益 desc:', html.includes('↓'));

// Switch to stock view, verify sort works there too
await page.locator('button:has-text("涉及股票")').click();
await page.waitForTimeout(500);

// Click 推荐数 in stock view
await page.locator('th button:has-text("推荐数")').click();
await page.waitForTimeout(300);
html = await page.content();
console.log('Stock view 推荐数 desc:', html.includes('↓'));

console.log('Errors:', errors.length > 0 ? errors.join(' | ') : 'none');
await browser.close();
