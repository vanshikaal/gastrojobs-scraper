/**
 * ╔══════════════════════════════════════════════════════════╗
 * ║   Gastrojobs / Hotelcareer Full Scraper — Apify Actor   ║
 * ║   Supports: gastrojobs.de | hotelcareer.de              ║
 * ║   Both operated by StepStone Deutschland GmbH           ║
 * ╚══════════════════════════════════════════════════════════╝
 *
 * FIELDS EXTRACTED PER JOB:
 *   job_id, company_id, job_title, company_name,
 *   location, employment_type, date_posted, has_express_apply,
 *   introduction, tasks, requirements, benefits,
 *   contact_person, contact_phone, contact_email,
 *   company_address, postal_code, company_email, company_website,
 *   related_tags, job_url, source_domain, scraped_at
 *
 * PAGINATION:
 *   The site uses AJAX POST to /popup.php?sei_id=N where sei_id
 *   is dynamic per page/session. The actor intercepts the first
 *   pagination request to capture the live sei_id, then replicates
 *   subsequent POSTs directly — fast and no extra page loads.
 *
 * ──────────────────────────────────────────────────────────
 * DEPLOY TO APIFY
 * ──────────────────────────────────────────────────────────
 * 1. console.apify.com → New Actor → upload main.js + package.json
 * 2. Set Node.js 20+, Build command: npm install
 * 3. Run with INPUT (see bottom of file for full schema)
 */

import { Actor }                from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';

await Actor.init();

// ── INPUT ─────────────────────────────────────────────────────────────────────
const input = await Actor.getInput() ?? {};
const {
  domain        = 'gastrojobs.de',
  startPath     = '/jobs/hotellerie-gastronomie-touristik-deutschland',
  maxJobs       = 10000,
  offersPerPage = 25,
  maxConcurrency = 3,
  proxyConfig   = { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] },
} = input;

const BASE_URL  = `https://www.${domain}`;
const START_URL = `${BASE_URL}${startPath}`;
let   totalSaved = 0;

console.log(`Target: ${START_URL}`);
console.log(`Max jobs: ${maxJobs} | Concurrency: ${maxConcurrency}`);

// ── DETAIL PAGE EXTRACTOR ─────────────────────────────────────────────────────

/**
 * Extracts all text content under a given h2 heading.
 * Walks nextElementSibling until the next h2 or end of parent.
 */
async function extractSection(page, headingText) {
  return page.evaluate((heading) => {
    const target = Array.from(document.querySelectorAll('h2'))
      .find(h => h.textContent.trim() === heading);
    if (!target) return null;
    const parts = [];
    let el = target.nextElementSibling;
    while (el && el.tagName !== 'H2') {
      const text = el.textContent.trim();
      if (text) parts.push(text);
      el = el.nextElementSibling;
    }
    return parts.join('\n').trim() || null;
  }, headingText);
}

/**
 * Parses the Kontakt block into structured contact fields.
 * Structure: h2 > p (person) > p (phone) > p (email) > strong (company)
 *            > span.postalcode > div.job_section > a (email) > a (website)
 */
/**
 * Try multiple heading variants, return first match.
 * Handles wide variation in German job posting heading styles.
 */
async function extractSectionMulti(page, headings) {
  return page.evaluate((headingList) => {
    const allH2 = Array.from(document.querySelectorAll('h2'));
    for (const heading of headingList) {
      const target = allH2.find(h =>
        h.textContent.trim().replace(/:$/, '') === heading.replace(/:$/, '')
      );
      if (!target) continue;
      const parts = [];
      let el = target.nextElementSibling;
      while (el && el.tagName !== 'H2') {
        const text = el.textContent.trim();
        if (text) parts.push(text);
        el = el.nextElementSibling;
      }
      if (parts.length) return parts.join('\n').trim();
    }
    return null;
  }, headings);
}

async function extractContact(page) {
  return page.evaluate(() => {
    const contactH2 = Array.from(document.querySelectorAll('h2'))
      .find(h => h.textContent.trim() === 'Kontakt');
    if (!contactH2) return {};

    const block = contactH2.parentElement;
    const paras  = Array.from(block.querySelectorAll('p'))
      .map(p => p.textContent.trim()).filter(Boolean);
    const links  = Array.from(block.querySelectorAll('a'));
    const emails = links.map(a => a.textContent.trim()).filter(t => t.includes('@'));
    const sites  = links.map(a => a.textContent.trim())
      .filter(t => t.includes('.') && !t.includes('@') && t.length > 4);

    // Heuristic parsing: person name (no digits), phone (mostly digits), email (@)
    const person = paras.find(p => !p.includes('@') && !/^\+?[\d\s\-\/\(\)]{6,}$/.test(p)) ?? null;
    const phone  = paras.find(p => /^\+?[\d\s\-\/\(\)]{6,}$/.test(p)) ?? null;
    const emailP = paras.find(p => p.includes('@')) ?? null;

    return {
      contact_person:  person,
      contact_phone:   phone,
      contact_email:   emailP ?? emails[0] ?? null,
      company_address: block.querySelector('strong')?.textContent?.trim() ?? null,
      postal_code:     block.querySelector('span.postalcode')?.textContent?.trim() ?? null,
      company_email:   emails.find(e => e !== emailP) ?? null,
      company_website: sites[0] ?? null,
    };
  });
}

/** Scrape every field from a job detail page */
async function scrapeDetailPage(page, url) {
  // Core header fields
  const title      = await page.$eval('h1', el => el.textContent.trim()).catch(() => null);
  const company    = await page.$eval('.ycg_info_line .clearfix', el => el.textContent.trim()).catch(() => null);
  const location   = await page.$eval('.meta_info_container span.location',   el => el.textContent.trim()).catch(() => null);
  const employment = await page.$eval('.meta_info_container span.employment', el => el.textContent.trim()).catch(() => null);
  const datePosted = await page.$eval('.meta_info_container span.date',       el => el.textContent.trim()).catch(() => null);

  // Content sections — try multiple heading variants per field
  // (employers use different German headings for the same content)
  const intro   = await extractSectionMulti(page, [
    'Einleitung', 'Über uns', 'Wir sind', 'Das sind wir'
  ]);
  const tasks   = await extractSectionMulti(page, [
    'Ihre Aufgaben', 'Aufgaben', 'Das sind Ihre Aufgaben',
    'Deine Aufgaben', 'Ihre Tätigkeiten', 'Aufgabenbereich'
  ]);
  const reqs    = await extractSectionMulti(page, [
    'Ihr Profil', 'Das bringen Sie mit', 'Was wir von Ihnen erwarten:',
    'Was wir von Ihnen erwarten', 'Ihr Profil:', 'Anforderungen',
    'Das bringst du mit', 'Dein Profil', 'Voraussetzungen'
  ]);
  const benefits= await extractSectionMulti(page, [
    'Wir bieten', 'Das bieten wir', 'Was wir Ihnen bieten:',
    'Was wir Ihnen bieten', 'Wir bieten Ihnen', 'Ihre Vorteile',
    'Benefits', 'Das bieten wir Ihnen', 'Deine Benefits'
  ]);

  // Contact block
  const contact = await extractContact(page);

  // Quick-apply badge
  const hasExpress = await page.$('[class*="express"]').then(el => !!el).catch(() => false);

  // Parse numeric IDs from URL path
  // Pattern: /jobs/{company-slug}-{companyId}/{title-slug}-{jobId}
  const ids       = url.match(/\/jobs\/[^\/]+-(\d+)\/[^\/]+-(\d+)/);
  const companyId = ids?.[1] ?? null;
  const jobId     = ids?.[2] ?? null;

  // Related roles/locations shown at page bottom
  const related = await page.evaluate(() => {
    const el = document.querySelector('.lp_links');
    return el ? el.innerText.trim().replace(/\s+/g, ' ') : null;
  });

  return {
    // Identifiers
    job_id:           jobId,
    company_id:       companyId,

    // Core listing info
    job_title:        title,
    company_name:     company,
    location:         location,
    employment_type:  employment,
    date_posted:      datePosted,
    has_express_apply: hasExpress,

    // Structured content
    introduction:     intro,
    tasks:            tasks,
    requirements:     reqs,
    benefits:         benefits,

    // Contact details
    contact_person:   contact.contact_person,
    contact_phone:    contact.contact_phone,
    contact_email:    contact.contact_email,
    company_address:  contact.company_address,
    postal_code:      contact.postal_code,
    company_email:    contact.company_email,
    company_website:  contact.company_website,

    // Meta
    related_tags:     related,
    job_url:          url,
    source_domain:    domain,
    scraped_at:       new Date().toISOString(),
  };
}

// ── LISTING PAGE ─────────────────────────────────────────────────────────────

/** Extract all job card links and hidden form state from a listing page */
async function extractListingData(page) {
  return page.evaluate(() => {
    const jobLinks = Array.from(document.querySelectorAll('a.link-blue-none'))
      .map(a => a.getAttribute('href'))
      .filter(h => h && /\/jobs\/.+-\d{5,}/.test(h))
      .map(h => h.startsWith('http') ? h : location.origin + h);

    // Total result count from h1 e.g. "1.252 Stellenangebote online"
    const h1      = document.querySelector('h1')?.textContent ?? '';
    const numMatch = h1.match(/[\d.]+/);
    const totalJobs = numMatch ? parseInt(numMatch[0].replace(/\./g, ''), 10) : 0;

    // All hidden form inputs needed by the pagination POST
    const hiddenInputs = Object.fromEntries(
      Array.from(document.querySelectorAll('input[type=hidden]'))
        .filter(i => i.name)
        .map(i => [i.name, i.value])
    );

    return { jobLinks, totalJobs, hiddenInputs };
  });
}

/** Parse job URLs out of the raw HTML response from /popup.php */
function parseJobLinksFromHtml(html, base) {
  const links = [];
  // Match href="/jobs/{company-slug}-{id}/{title-slug}-{id}"
  const re = /href="(\/jobs\/[^"]*-\d{5,}[^"]*)"/g;
  let m;
  while ((m = re.exec(html)) !== null) {
    const clean = `${base}${m[1].split('?')[0]}`;
    if (!links.includes(clean)) links.push(clean);
  }
  return links;
}

// ── CRAWLER ───────────────────────────────────────────────────────────────────

const proxy = await Actor.createProxyConfiguration(proxyConfig);

const crawler = new PlaywrightCrawler({
  proxyConfiguration: proxy,
  launchContext: {
    launchOptions: { headless: true },
  },

  // Stealth: mask webdriver signal + set German headers
  preNavigationHooks: [
    async ({ page }) => {
      await page.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
      });
      await page.setExtraHTTPHeaders({
        'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
      });
    },
  ],

  requestHandlerTimeoutSecs: 60,
  maxConcurrency,

  async requestHandler({ request, page, crawler: self }) {
    const { label } = request.userData ?? {};

    // ── DETAIL PAGE ──────────────────────────────────────────────────────────
    if (label === 'DETAIL') {
      await page.waitForSelector('h1', { timeout: 15000 });
      const data = await scrapeDetailPage(page, request.url);
      await Dataset.pushData(data);
      totalSaved++;
      if (totalSaved % 100 === 0) {
        console.log(`Progress: ${totalSaved} jobs saved`);
      }
      return;
    }

    // ── LISTING PAGE ─────────────────────────────────────────────────────────
    await page.waitForSelector('.ycg-job-item', { timeout: 20000 });
    const { jobLinks, totalJobs, hiddenInputs } = await extractListingData(page);
    const totalPages = Math.ceil(totalJobs / offersPerPage);

    console.log(`Total jobs on site: ${totalJobs} | Pages: ${totalPages}`);
    console.log(`Page 1: ${jobLinks.length} job links found`);

    // Queue detail pages from page 1
    for (const url of jobLinks) {
      if (totalSaved + (await self.requestQueue.countFinished?.() ?? 0) >= maxJobs) break;
      await self.addRequests([{ url, userData: { label: 'DETAIL' } }], { forefront: false });
    }

    // ── INTERCEPT sei_id ────────────────────────────────────────────────────
    // Capture the live popup.php URL (including sei_id) from the first real
    // pagination request made by the page's own JS.
    let capturedPopupUrl = null;

    await page.route('**/popup.php**', async (route) => {
      // Capture full URL including SID and k tokens — required by server
      if (!capturedPopupUrl) capturedPopupUrl = route.request().url();
      await route.continue();
    });

    // Dismiss any modal blocking the button (e.g. job-alert signup popup)
    await page.evaluate(() => {
      document.querySelector('#jobfinderBox')?.remove();
      document.querySelectorAll('.modal-backdrop').forEach(el => el.remove());
      document.body.classList.remove('modal-open');
      document.body.style.overflow = '';
    });

    // Trigger first pagination to capture sei_id
    const nextBtn = await page.$('.js-next-page');
    if (!nextBtn) {
      console.log('No next page button found — only 1 page of results.');
      return;
    }

    // JS click bypasses any remaining overlay interception
    await page.evaluate(() => document.querySelector('.js-next-page').click());

    // Wait for capturedPopupUrl to be set (max 5s)
    const deadline = Date.now() + 5000;
    while (!capturedPopupUrl && Date.now() < deadline) {
      await page.waitForTimeout(100);
    }

    if (!capturedPopupUrl) {
      console.warn('Could not capture popup URL — falling back to DOM pagination.');
      // Fallback: scrape remaining pages by clicking through
      await scrapePaginationByClicking(page, self, totalPages, 2);
      return;
    }

    // Extract sei_id from captured URL e.g. "/popup.php?sei_id=42"
    const seiMatch = capturedPopupUrl.match(/sei_id=(\d+)/);
    const seiId    = seiMatch?.[1] ?? '407';
    console.log(`Captured sei_id=${seiId} from live pagination request`);

    // Unregister route — we have what we need
    await page.unroute('**/popup.php**');

    // ── PAGINATE via direct POST ─────────────────────────────────────────────
    // Grab session cookies once
    const cookies   = await page.context().cookies();
    const cookieStr = cookies.map(c => `${c.name}=${c.value}`).join('; ');

    for (let pageNum = 2; pageNum <= totalPages; pageNum++) {
      if (totalSaved >= maxJobs) break;

      const formData = new URLSearchParams({
        action:                    'resultlist',
        mode:                      'next',
        site_number:               String(pageNum),
        offers_per_page:           String(offersPerPage),
        number_of_pages:           String(totalPages),
        added_filter_property:     '',
        added_filter_value:        '',
        deleted_filter_property:   '',
        deleted_filter_prop_group: '',
        deleted_filter_value:      '',
        search_type:               'mb',
        ...hiddenInputs,
      });

      // POST from within page context — inherits live session/cookies + SID/k tokens
      const html = await page.evaluate(
        async ({ postUrl, body }) => {
          const r = await fetch(postUrl, {
            method:      'POST',
            headers:     {
              'Content-Type':     'application/x-www-form-urlencoded; charset=UTF-8',
              'X-Requested-With': 'XMLHttpRequest',
            },
            body,
            credentials: 'include',
          });
          const json = await r.json();
          // Response: { liste: "<html string of job cards>", site_number, number_of_pages, ... }
          return json.liste || '';
        },
        {
          // Use full captured URL — includes SID and k tokens the server requires
          postUrl: capturedPopupUrl.replace(/sei_id=\d+/, `sei_id=${seiId}`),
          body:    formData.toString(),
        }
      );

      const pageLinks = parseJobLinksFromHtml(html, BASE_URL);
      console.log(`Page ${pageNum}/${totalPages}: ${pageLinks.length} jobs`);

      for (const url of pageLinks) {
        if (totalSaved >= maxJobs) break;
        await self.addRequests([{ url, userData: { label: 'DETAIL' } }], { forefront: false });
      }

      // Respectful delay between page requests (0.6–1.0s)
      await page.waitForTimeout(600 + Math.random() * 400);
    }
  },

  failedRequestHandler({ request, error }) {
    console.error(`FAILED [${request.retryCount}x]: ${request.url} — ${error.message}`);
  },
});

/** Fallback pagination: click through pages via DOM (slower but always works) */
async function scrapePaginationByClicking(page, self, totalPages, startPage) {
  for (let p = startPage; p <= totalPages; p++) {
    if (totalSaved >= maxJobs) break;
    const nextBtn = await page.$('.js-next-page.show');
    if (!nextBtn) break;

    const prevCount = await page.$$eval('.ycg-job-item', els => els.length);

    // Dismiss modal before each click
    await page.evaluate(() => {
      document.querySelector('#jobfinderBox')?.remove();
      document.querySelectorAll('.modal-backdrop').forEach(el => el.remove());
      document.body.classList.remove('modal-open');
      document.body.style.overflow = '';
    });
    await page.evaluate(() => document.querySelector('.js-next-page')?.click());

    // Wait for new cards to load
    await page.waitForFunction(
      (prev) => document.querySelectorAll('.ycg-job-item').length > prev,
      prevCount,
      { timeout: 10000 }
    ).catch(() => {});

    const links = await page.$$eval(
      'a.link-blue-none',
      (els, base) => els
        .map(a => a.getAttribute('href'))
        .filter(h => h && /\/jobs\/.+-\d{5,}/.test(h))
        .map(h => h.startsWith('http') ? h : base + h),
      BASE_URL
    );

    console.log(`[Fallback] Page ${p}/${totalPages}: ${links.length} jobs`);
    for (const url of links) {
      await self.addRequests([{ url, userData: { label: 'DETAIL' } }], { forefront: false });
    }

    await page.waitForTimeout(800 + Math.random() * 400);
  }
}

// ── START ─────────────────────────────────────────────────────────────────────
await crawler.run([{ url: START_URL, userData: { label: 'LISTING' } }]);

console.log(`\n✓ Complete. Total jobs saved: ${totalSaved}`);
await Actor.exit();

/*
 * ──────────────────────────────────────────────────────────
 * INPUT SCHEMA (save as .actor/INPUT_SCHEMA.json on Apify)
 * ──────────────────────────────────────────────────────────
 * {
 *   "title": "Gastrojobs / Hotelcareer Scraper",
 *   "type": "object",
 *   "properties": {
 *     "domain": {
 *       "title": "Domain",
 *       "type": "string",
 *       "enum": ["gastrojobs.de", "hotelcareer.de"],
 *       "default": "gastrojobs.de",
 *       "description": "Which StepStone hospitality site to scrape"
 *     },
 *     "startPath": {
 *       "title": "Start Path",
 *       "type": "string",
 *       "default": "/jobs/hotellerie-gastronomie-touristik-deutschland",
 *       "description": "URL path to start from. Use /jobs/hotellerie-gastronomie-touristik-deutschland for all Germany jobs, or /jobs/koch for a specific role, or /jobs/hotellerie-gastronomie-touristik-berlin for a city."
 *     },
 *     "maxJobs": {
 *       "title": "Max Jobs",
 *       "type": "integer",
 *       "default": 10000,
 *       "description": "Hard cap on total jobs scraped"
 *     },
 *     "offersPerPage": {
 *       "title": "Offers Per Page",
 *       "type": "integer",
 *       "default": 25,
 *       "description": "Matches site pagination size — do not change unless site changes"
 *     },
 *     "maxConcurrency": {
 *       "title": "Max Concurrency",
 *       "type": "integer",
 *       "default": 3,
 *       "description": "Parallel requests. Keep low (2-4) to avoid bot detection"
 *     }
 *   }
 * }
 *
 * ──────────────────────────────────────────────────────────
 * USEFUL START PATHS
 * ──────────────────────────────────────────────────────────
 *   All Germany jobs:    /jobs/hotellerie-gastronomie-touristik-deutschland
 *   All Germany (hotel): use domain=hotelcareer.de with same path
 *   By city (Berlin):    /jobs/hotellerie-gastronomie-touristik-berlin
 *   By role (Köche):     /jobs/koch
 *   By role (Kellner):   /jobs/kellner
 *   By role (Rezeption): /jobs/rezeptionist
 */
