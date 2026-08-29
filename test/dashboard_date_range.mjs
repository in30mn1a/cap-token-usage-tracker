import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { chromium } from 'playwright-core';

const [htmlPath, chromePath, scenario = 'exclusive'] = process.argv.slice(2);
if (!htmlPath || !chromePath) {
  throw new Error('usage: node test/dashboard_date_range.mjs <dashboard-html-path> <google-chrome-path> [exclusive|end-time|end-time-reset|quick-preset|reverse|los-angeles-dst|all-range-trend|all-range-cold|granularity-persists|share-metric-persists|refresh-animation]');
}
if (!['exclusive', 'end-time', 'end-time-reset', 'quick-preset', 'reverse', 'los-angeles-dst', 'all-range-trend', 'all-range-cold', 'granularity-persists', 'share-metric-persists', 'refresh-animation'].includes(scenario)) {
  throw new Error(`unknown dashboard date-range browser scenario: ${scenario}`);
}

const dashboardHTML = await readFile(htmlPath);
const resourceBase = '/v0/resource/plugins/calendar-browser-test';
const timezoneId = scenario === 'los-angeles-dst' ? 'America/Los_Angeles' : 'UTC';
const initialRange = scenario === 'los-angeles-dst'
  ? { start: '2026-08-23T07:00:00.000Z', end: '2026-08-24T07:00:00.000Z' }
  : { start: '2026-08-23T00:00:00.000Z', end: '2026-08-24T00:00:00.000Z' };
const emptyInitial = {
  generated_at: '2026-08-23T00:00:00.000Z',
  last_used: '0001-01-01T00:00:00.000Z',
  models: [],
  sources: [],
  bucket_seconds: 86400,
};
// The "All" preset has to resolve to the retained window rather than a fixed
// far-past date, so this scenario serves a store that started three hours ago
// and holds a single bucket of traffic.
const persistenceScenario = scenario === 'granularity-persists' || scenario === 'share-metric-persists';
const populatedScenario = scenario === 'all-range-trend' || scenario === 'all-range-cold' || persistenceScenario || scenario === 'refresh-animation';
let savedMetric = '';
// Saved preferences live in the stub for the lifetime of the scenario so a
// reload can prove the granularity survived the round trip.
let savedGranularity = '';
const retainedSince = '2026-08-23T09:00:00.000Z';
const trendPointHour = '2026-08-23T11:00:00.000Z';
const trendCounters = { requests: 3, input_tokens: 200, output_tokens: 100, reasoning_tokens: 0, cached_tokens: 0, cache_read_tokens: 0, cache_creation_tokens: 0, total_tokens: 300, failed_requests: 0 };
const populatedInitial = {
  schema_version: 2,
  generated_at: '2026-08-23T12:00:00.000Z',
  retained_since: retainedSince,
  last_used: trendPointHour,
  bucket_seconds: 300,
  summary: trendCounters,
  models: [Object.assign({ model: 'upstream-model', alias: 'client-alias' }, trendCounters)],
  series: [Object.assign({ hour: trendPointHour }, trendCounters)],
  sources: ['cli'],
};
const populatedTrend = {
  schema_version: 2,
  bucket_seconds: 300,
  model_series: [Object.assign({ hour: trendPointHour, model: 'upstream-model', alias: 'client-alias' }, trendCounters)],
};

async function setTimePickerValue(page, boundary, values) {
  for (const [part, value] of Object.entries(values)) {
    await page.locator(`#${boundary}TimePicker [data-time-part="${part}"]`).evaluate((field, nextValue) => {
      field.value = nextValue;
      field.dispatchEvent(new Event('input', { bubbles: true }));
    }, value);
  }
}

const server = createServer((request, response) => {
  const url = new URL(request.url, 'http://127.0.0.1');
  const sendJSON = (value) => {
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify(value));
  };

  if (url.pathname === `${resourceBase}/dashboard`) {
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    response.end(dashboardHTML);
    return;
  }
  if (url.pathname === `${resourceBase}/preferences`) {
    if (persistenceScenario) {
      if (url.searchParams.get('save') === '1') {
        savedGranularity = url.searchParams.get('granularity') || '';
        savedMetric = url.searchParams.get('model_share_metric') || '';
      }
      sendJSON({ time_range_mode: 'all', granularity: savedGranularity, model_share_metric: savedMetric });
      return;
    }
    sendJSON(scenario === 'all-range-cold'
      ? { time_range_mode: 'all', granularity: 'hour' }
      : {
        time_range_mode: 'custom',
        time_range_start: initialRange.start,
        time_range_end: initialRange.end,
      });
    return;
  }
  if (url.pathname === `${resourceBase}/stats/initial`) {
    sendJSON(populatedScenario ? populatedInitial : emptyInitial);
    return;
  }
  if (url.pathname === `${resourceBase}/stats/trends`) {
    sendJSON(populatedScenario ? populatedTrend : { model_series: [], bucket_seconds: 86400 });
    return;
  }
  if (url.pathname === `${resourceBase}/stats/groups` || url.pathname === `${resourceBase}/requests`) {
    sendJSON({ items: [], total: 0 });
    return;
  }
  if (url.pathname === `${resourceBase}/costs`) {
    sendJSON({ summary: { requests: 0, priced_requests: 0, unpriced_requests: 0 }, models: [], price_book_revision: 0 });
    return;
  }
  if (url.pathname === `${resourceBase}/prices`) {
    sendJSON({ prices: {}, revision: 0 });
    return;
  }
  sendJSON({});
});

await new Promise((resolve, reject) => {
  server.once('error', reject);
  server.listen(0, '127.0.0.1', resolve);
});

const address = server.address();
const dashboardURL = `http://127.0.0.1:${address.port}${resourceBase}/dashboard`;
const browser = await chromium.launch({ executablePath: chromePath, headless: true });

try {
  const context = await browser.newContext({ timezoneId });
  const page = await context.newPage();
  const pageErrors = [];
  page.on('pageerror', (error) => pageErrors.push(error));

  await page.clock.install({ time: new Date('2026-08-23T12:00:00.000Z') });
  await page.goto(dashboardURL, { waitUntil: 'domcontentloaded' });
  await page.waitForResponse((response) => new URL(response.url()).pathname === `${resourceBase}/stats/initial`);

  if (scenario === 'refresh-animation') {
    await page.waitForTimeout(1200);
    // One refresh renders three times as the stats, price book and cost
    // payloads land. Replaying the entry animation on an unchanged number
    // makes the summary cards twitch, so count the replays across a refresh
    // that returns identical data.
    const counted = await page.evaluate(() => new Promise((resolve) => {
      const ids = ['totalTokens', 'totalCost', 'requests', 'topModel'];
      const replays = {};
      const observers = ids.map((id) => {
        replays[id] = 0;
        const node = document.getElementById(id);
        if (!node) return null;
        const observer = new MutationObserver((records) => {
          records.forEach((record) => {
            if (record.attributeName === 'class' && node.classList.contains('animate')) replays[id] += 1;
          });
        });
        observer.observe(node, { attributes: true, attributeFilter: ['class'] });
        return observer;
      });
      document.getElementById('refreshButton').click();
      setTimeout(() => {
        observers.forEach((observer) => observer && observer.disconnect());
        resolve(replays);
      }, 2500);
    }));
    const twitchy = Object.entries(counted).filter(([, count]) => count > 0);
    if (twitchy.length) {
      throw new Error(`a refresh with unchanged values must not replay the entry animation, got ${JSON.stringify(counted)}`);
    }

    if (pageErrors.length) {
      throw new Error(`dashboard raised ${pageErrors.length} page error(s): ${pageErrors[0].message}`);
    }
    await context.close();
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
    process.exit(0);
  }

  if (scenario === 'share-metric-persists') {
    await page.waitForTimeout(600);
    const initial = await page.locator('[data-model-share-metric="requests"]').getAttribute('aria-pressed');
    if (initial !== 'true') {
      throw new Error(`the donut must default to requests, got aria-pressed=${initial}`);
    }

    // "cost" is the load-bearing case: syncModelShareMetricButtons downgrades it
    // to "requests" until the cost payload lands, so a naive restore loses it.
    for (const metric of ['tokens', 'cost']) {
      await page.locator(`[data-model-share-metric="${metric}"]`).click();
      await page.waitForTimeout(600);
      if (savedMetric !== metric) {
        throw new Error(`choosing ${metric} must persist it, stub saved ${JSON.stringify(savedMetric)}`);
      }
      await page.reload({ waitUntil: 'domcontentloaded' });
      await page.waitForTimeout(1200);
      const pressed = await page.locator(`[data-model-share-metric="${metric}"]`).getAttribute('aria-pressed');
      if (pressed !== 'true') {
        throw new Error(`${metric} must survive a reload, got aria-pressed=${pressed}`);
      }
    }

    if (pageErrors.length) {
      throw new Error(`dashboard raised ${pageErrors.length} page error(s): ${pageErrors[0].message}`);
    }
    await context.close();
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
    process.exit(0);
  }

  if (scenario === 'granularity-persists') {
    await page.waitForTimeout(600);
    // The retained window is three hours, so an unpinned granularity has to
    // resolve to "minute" -- the far-past placeholder would yield "month".
    const derived = await page.locator('#granularity').inputValue();
    if (derived !== 'minute') {
      throw new Error(`an unpinned granularity must follow the resolved range, got ${derived}`);
    }

    await page.locator('#granularity').evaluate((select) => {
      select.value = 'day';
      select.dispatchEvent(new Event('change', { bubbles: true }));
    });
    await page.waitForTimeout(600);
    if (savedGranularity !== 'day') {
      throw new Error(`choosing a granularity must persist it, stub saved ${JSON.stringify(savedGranularity)}`);
    }

    await page.reload({ waitUntil: 'domcontentloaded' });
    await page.waitForTimeout(900);
    const restored = await page.locator('#granularity').inputValue();
    if (restored !== 'day') {
      throw new Error(`a saved granularity must survive a reload, got ${restored}`);
    }

    if (pageErrors.length) {
      throw new Error(`dashboard raised ${pageErrors.length} page error(s): ${pageErrors[0].message}`);
    }
    await context.close();
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
    process.exit(0);
  }

  if (scenario === 'all-range-cold') {
    // Restoring the saved "All" preset issues its first query before any
    // response has revealed retained_since, so the status has to be corrected
    // once it is known instead of quoting the placeholder start forever.
    await page.waitForTimeout(600);
    const status = await page.locator('#status').textContent();
    if (status.includes('2000')) {
      throw new Error(`a restored "All" preset must report the retained window, got ${status}`);
    }
    if (!status.includes('2026/08/23')) {
      throw new Error(`a restored "All" preset must name the retained window start, got ${status}`);
    }
    const bars = await page.locator('#chart rect.bar-input').count();
    if (bars < 1) {
      throw new Error(`a restored "All" preset must draw the token trend, got ${bars} bars`);
    }
    if (pageErrors.length) {
      throw new Error(`dashboard raised ${pageErrors.length} page error(s): ${pageErrors[0].message}`);
    }
    await context.close();
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
    process.exit(0);
  }

  await page.locator('#rangeButton').click();
  if (scenario === 'all-range-trend') {
    const appliedResponse = page.waitForResponse((response) => new URL(response.url()).pathname === `${resourceBase}/stats/initial`);
    await page.locator('[data-range-preset="all"]').click();
    await page.locator('#confirmDateRange').click();
    const applied = new URL((await appliedResponse).url());
    if (applied.searchParams.get('start') !== retainedSince) {
      throw new Error(`"All" must query from the retained window start ${retainedSince}, got ${applied.searchParams.get('start')}`);
    }

    // continuousBuckets fills every slot between the range bounds and gives up
    // after 100000 steps, so a far-past start leaves the minute and hour trends
    // without a single drawn bar even though the payload carries data.
    for (const granularity of ['hour', 'minute']) {
      await page.locator('#granularity').evaluate((select, value) => {
        select.value = value;
        select.dispatchEvent(new Event('change', { bubbles: true }));
      }, granularity);
      await page.waitForTimeout(300);
      const bars = await page.locator('#chart rect.bar-input').count();
      if (bars < 1) {
        throw new Error(`"All" range must still draw the token trend at ${granularity} granularity, got ${bars} bars`);
      }
    }

    // The status must quote the range that was actually queried.
    const status = await page.locator('#status').textContent();
    if (status.includes('2000')) {
      throw new Error(`"All" status must report the retained window, got ${status}`);
    }
    if (!status.includes('2026/08/23')) {
      throw new Error(`"All" status must name the retained window start, got ${status}`);
    }

    if (pageErrors.length) {
      throw new Error(`dashboard raised ${pageErrors.length} page error(s): ${pageErrors[0].message}`);
    }
    await context.close();
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
    process.exit(0);
  }
  if (scenario === 'quick-preset') {
    await page.locator('[data-range-preset="last_30_days"]').click();

    // "Last 30 days" counts today as one of the thirty and runs to the current
    // instant; anchoring the end at midnight dropped today's traffic entirely.
    const expectedBoundaryTimes = { start: '00:00:00', end: '12:00:00' };
    for (const boundary of ['start', 'end']) {
      const button = page.locator(`#${boundary}TimeButton`);
      if (await button.isDisabled()) {
        throw new Error(`${boundary} time must remain editable after choosing a quick range`);
      }
      const shown = await button.textContent();
      if (shown !== expectedBoundaryTimes[boundary]) {
        throw new Error(`last 30 days ${boundary} must show ${expectedBoundaryTimes[boundary]}, got ${shown}`);
      }
    }

    await page.locator('#startTimeButton').click();
    await setTimePickerValue(page, 'start', { hour: '01', minute: '02', second: '03' });
    const confirmedResponse = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return url.pathname === `${resourceBase}/stats/initial`
        && url.searchParams.get('start') === '2026-07-25T01:02:03.000Z';
    });
    await page.locator('#confirmDateRange').click();
    const confirmed = new URL((await confirmedResponse).url());
    // The end tracks the current instant, so only the second is deterministic.
    const confirmedEnd = confirmed.searchParams.get('end');
    if (!String(confirmedEnd).startsWith('2026-08-23T12:00:00')) {
      throw new Error(`expected manually edited quick range end at 2026-08-23T12:00:00, got ${confirmedEnd}`);
    }
  } else if (scenario === 'reverse') {
    await page.locator('[data-date="2026-08-23"]').click();
    await page.locator('[data-date="2026-08-21"]').click();
  } else {
    await page.locator('[data-date="2026-08-21"]').click();
    await page.locator('[data-date="2026-08-23"]').click();
  }

  if (scenario === 'quick-preset') {
    // Verified above.
  } else if (scenario === 'exclusive') {
    const selectedEnd = page.locator('[data-date="2026-08-23"].range-end');
    if (await selectedEnd.count() !== 1) {
      throw new Error('selecting 2026-08-21 through 2026-08-23 must mark 2026-08-23 as .range-end');
    }

    const confirmedResponse = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return url.pathname === `${resourceBase}/stats/initial`
        && url.searchParams.get('start') === '2026-08-21T00:00:00.000Z';
    });
    await page.locator('#confirmDateRange').click();
    const confirmed = new URL((await confirmedResponse).url());
    if (confirmed.searchParams.get('start') !== '2026-08-21T00:00:00.000Z') {
      throw new Error(`expected confirmed start=2026-08-21T00:00:00.000Z, got ${confirmed.searchParams.get('start')}`);
    }
    if (confirmed.searchParams.get('end') !== '2026-08-24T00:00:00.000Z') {
      throw new Error(`expected confirmed end=2026-08-24T00:00:00.000Z, got ${confirmed.searchParams.get('end')}`);
    }
  } else if (scenario === 'los-angeles-dst') {
    const selectedEnd = page.locator('[data-date="2026-08-23"].range-end');
    if (await selectedEnd.count() !== 1) {
      throw new Error('America/Los_Angeles selection must mark 2026-08-23 as .range-end');
    }

    const confirmedResponse = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return url.pathname === `${resourceBase}/stats/initial`
        && url.searchParams.get('start') === '2026-08-21T07:00:00.000Z';
    });
    await page.locator('#confirmDateRange').click();
    const confirmed = new URL((await confirmedResponse).url());
    if (confirmed.searchParams.get('start') !== '2026-08-21T07:00:00.000Z') {
      throw new Error(`expected America/Los_Angeles start=2026-08-21T07:00:00.000Z, got ${confirmed.searchParams.get('start')}`);
    }
    if (confirmed.searchParams.get('end') !== '2026-08-24T07:00:00.000Z') {
      throw new Error(`expected America/Los_Angeles end=2026-08-24T07:00:00.000Z, got ${confirmed.searchParams.get('end')}`);
    }
  } else if (scenario === 'end-time' || scenario === 'end-time-reset') {
    await page.locator('#endTimeButton').click();
    await setTimePickerValue(page, 'end', { hour: '12', minute: '00', second: '00' });

    if (scenario === 'end-time-reset') {
      await setTimePickerValue(page, 'end', { hour: '00', minute: '00', second: '00' });
    }

    const selectedEnd = page.locator('[data-date="2026-08-23"].range-end');
    if (await selectedEnd.count() !== 1) {
      throw new Error(scenario === 'end-time'
        ? 'setting end time to 12:00:00 must keep 2026-08-23 as .range-end'
        : 'resetting end time from 12:00:00 to 00:00:00 must keep 2026-08-23 as .range-end');
    }

    const confirmedResponse = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return url.pathname === `${resourceBase}/stats/initial`
        && url.searchParams.get('start') === '2026-08-21T00:00:00.000Z';
    });
    await page.locator('#confirmDateRange').click();
    const confirmed = new URL((await confirmedResponse).url());
    const expectedEnd = scenario === 'end-time' ? '2026-08-23T12:00:00.000Z' : '2026-08-24T00:00:00.000Z';
    if (confirmed.searchParams.get('end') !== expectedEnd) {
      throw new Error(`expected ${scenario} end=${expectedEnd}, got ${confirmed.searchParams.get('end')}`);
    }
  } else {
    const selectedStart = page.locator('[data-date="2026-08-21"].range-start');
    if (await selectedStart.count() !== 1) {
      throw new Error('reverse selection 2026-08-23 through 2026-08-21 must mark 2026-08-21 as .range-start');
    }
    const selectedEnd = page.locator('[data-date="2026-08-23"].range-end');
    if (await selectedEnd.count() !== 1) {
      throw new Error('reverse selection 2026-08-23 through 2026-08-21 must mark 2026-08-23 as .range-end');
    }

    const confirmedResponse = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return url.pathname === `${resourceBase}/stats/initial`
        && url.searchParams.get('start') === '2026-08-21T00:00:00.000Z';
    });
    await page.locator('#confirmDateRange').click();
    const confirmed = new URL((await confirmedResponse).url());
    if (confirmed.searchParams.get('end') !== '2026-08-24T00:00:00.000Z') {
      throw new Error(`expected reverse-selection end=2026-08-24T00:00:00.000Z, got ${confirmed.searchParams.get('end')}`);
    }
  }
  if (pageErrors.length) {
    throw pageErrors[0];
  }
} finally {
  await browser.close();
  await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
}
