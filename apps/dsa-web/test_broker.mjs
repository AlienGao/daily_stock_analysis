import { chromium } from 'playwright';

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage();
const errors = [];
page.on('console', msg => { if (msg.type() === 'error') errors.push(msg.text()); });
page.on('pageerror', err => errors.push(err.message));

await page.goto('http://localhost:5173/broker-recommend', { waitUntil: 'networkidle', timeout: 15000 });
await page.waitForTimeout(2000);
let html = await page.content();

// 1. Basic render
console.log('1. Page load:', html.includes('券商金股明细') ? 'PASS' : 'FAIL');
console.log('2. Overview:', html.includes('推荐总数') ? 'PASS' : 'FAIL');
console.log('3. Broker groups:', html.includes('只</span>') ? 'PASS' : 'FAIL');

// 2. Expand broker
const firstBroker = page.locator('button:has(span:has-text("只"))').first();
await firstBroker.click();
await page.waitForTimeout(500);
html = await page.content();
const tableCount = (html.match(/<table/g) || []).length;
console.log('4. Expand broker (tables):', tableCount > 0 ? 'PASS' : 'FAIL');

// 3. Sort in broker view
await page.locator('th button:has-text("累计收益")').first().click();
await page.waitForTimeout(300);
html = await page.content();
console.log('5. Broker sort desc:', html.includes('↓') ? 'PASS' : 'FAIL');

await page.locator('th button:has-text("累计收益")').first().click();
await page.waitForTimeout(300);
html = await page.content();
console.log('6. Broker sort asc:', html.includes('↑') ? 'PASS' : 'FAIL');

// 4. Stock view
await page.locator('button:has-text("涉及股票")').click();
await page.waitForTimeout(500);
html = await page.content();
console.log('7. Stock view:', html.includes('全部金股明细') ? 'PASS' : 'FAIL');

// 5. Stock sort 3-state
await page.locator('th button:has-text("推荐数")').click();
await page.waitForTimeout(300);
html = await page.content();
console.log('8. Stock sort desc:', html.includes('↓') ? 'PASS' : 'FAIL');

await page.locator('th button:has-text("推荐数")').click();
await page.waitForTimeout(300);
html = await page.content();
console.log('9. Stock sort asc:', html.includes('↑') ? 'PASS' : 'FAIL');

await page.locator('th button:has-text("推荐数")').click();
await page.waitForTimeout(300);
html = await page.content();
const hasIndicator = html.match(/推荐数<span[^>]*>[^<]*[↑↓]/);
console.log('10. Sort default:', !hasIndicator ? 'PASS' : 'FAIL');

// 6. Back to broker
await page.locator('button:has-text("券商数量")').click();
await page.waitForTimeout(500);
html = await page.content();
console.log('11. Back to broker:', html.includes('券商金股明细') ? 'PASS' : 'FAIL');

// 7. Chart renders (SVG)
console.log('12. Chart SVG:', html.includes('<svg') ? 'PASS' : 'FAIL');

// 8. Fetch button exists, no backtest button
const controls = html.substring(html.indexOf('获取当月数据'), html.indexOf('获取当月数据') + 200);
const hasBacktestBtn = controls.includes('>回测</button>');
console.log('13. Fetch button:', controls.includes('获取当月数据') ? 'PASS' : 'FAIL');
console.log('14. No backtest button:', !hasBacktestBtn ? 'PASS' : 'FAIL');

// 9. Colors: red for positive
const hasRed400 = html.includes('text-red-400');
console.log('15. Red for positive:', hasRed400 ? 'PASS' : 'FAIL');

// 10. Date range shows same-month
console.log('16. Date range:', html.includes('回测区间') ? 'PASS' : 'FAIL');

console.log('Errors:', errors.length > 0 ? errors.join(' | ') : 'none');
await browser.close();
