
# Regex patterns for Cloudflare Worker / WAF

This file lists **User‑Agent** and **Referer** regex patterns derived from your
Optimizely events. Use them in a Cloudflare **Worker** to remove the Optimizely
snippet from HTML responses for bots, or in **WAF** rules to block/challenge
access to Optimizely assets.

## User‑Agent patterns (families)

```js
// Keep lists small and precise; expand as you validate in logs
export const UA_REGEXES = [
  /(^|[^a-z])petalbot([^a-z]|$)/i,
  /headlesschrome/i,
  /google-read[- ]?aloud/i,
  /phantomjs/i,
  /(ahrefs|semrush|mj12)/i,
  /(bot|crawl|spider|slurp|crawler|scrap(e|er)|fetch)/i
];
```

## Referer patterns (translation/proxy)

```js
export const REF_REGEXES = [
  /(translate\.google|translate\.goog|googleweblight|webcache\.googleusercontent|translatetheweb\.com|translate\.bing\.com)/i
];
```

---

## Cloudflare Worker (with Bot Management signals)

```js
// worker.js
import { UA_REGEXES, REF_REGEXES } from './patterns.js';

export default {
  async fetch(request, env, ctx) {
    const ua = request.headers.get('user-agent') || '';
    const ref = request.headers.get('referer') || '';

    // Bot Management signals (Enterprise plans)
    const bm = (request.cf && request.cf.botManagement) || {};
    const score = bm.score;                 // 1..99 (lower = more bot-like)
    const verified = bm.verifiedBot === true;

    // Fallback: UA/Ref heuristics
    const uaLooksBot = UA_REGEXES.some(r => r.test(ua));
    const refLooksProxy = REF_REGEXES.some(r => r.test(ref));

    const isBotByScore = (typeof score === 'number' && score < 30 && !verified);
    const isKnownBot = isBotByScore || uaLooksBot || refLooksProxy;

    const originRes = await fetch(request);
    const ct = originRes.headers.get('content-type') || '';
    if (!isKnownBot || !ct.includes('text/html')) return originRes;

    // Remove Optimizely: CDN-hosted and self-hosted variants
    return new HTMLRewriter()
      .on('script[src^="https://cdn.optimizely.com/js"]', { element(el){ el.remove(); } })
      .on('script[src*="optimizely"]',                    { element(el){ el.remove(); } })
      .on('script', {
        element(el) {
          const src = el.getAttribute('src') || '';
          if (!src && /optimizely|window\.optimizely|optimizelySdk/i.test(el.text)) {
            el.remove();
          }
        }
      })
      .transform(originRes);
  }
}
```

## Cloudflare Worker (without Bot Management)

```js
// worker-lite.js
import { UA_REGEXES, REF_REGEXES } from './patterns.js';

export default {
  async fetch(request, env, ctx) {
    const ua = request.headers.get('user-agent') || '';
    const ref = request.headers.get('referer') || '';

    const isKnownBot = UA_REGEXES.some(r => r.test(ua)) || REF_REGEXES.some(r => r.test(ref));

    const originRes = await fetch(request);
    const ct = originRes.headers.get('content-type') || '';
    if (!isKnownBot || !ct.includes('text/html')) return originRes;

    return new HTMLRewriter()
      .on('script[src^="https://cdn.optimizely.com/js"]', { element(el){ el.remove(); } })
      .on('script[src*="optimizely"]',                    { element(el){ el.remove(); } })
      .on('script', {
        element(el) {
          const src = el.getAttribute('src') || '';
          if (!src && /optimizely|window\.optimizely|optimizelySdk/i.test(el.text)) {
            el.remove();
          }
        }
      })
      .transform(originRes);
  }
}
```

## Optional WAF expressions (if self-hosting the JS)

**With Bot Management**:
```
(cf.bot_management.score < 30 and not cf.bot_management.verified_bot)
and (http.request.uri.path contains "/optimizely" or http.request.uri.query contains "optimizely")
```

**Without Bot Management (UA/Referer only)**:
```
(
  lower(http.user_agent) contains "petalbot" or
  lower(http.user_agent) contains "headlesschrome" or
  lower(http.user_agent) contains "google-read-aloud" or
  lower(http.referer) contains "translate.google" or
  lower(http.referer) contains "googleweblight" or
  lower(http.referer) contains "webcache.googleusercontent" or
  lower(http.referer) contains "translatetheweb.com" or
  lower(http.referer) contains "translate.bing.com"
)
and (http.request.uri.path contains "/optimizely" or http.request.uri.query contains "optimizely")
```

## Tips
- Start with **Managed Challenge** for a few days, then move to **Block**.
- Keep your regex list **tight** to avoid false positives (e.g., internal tools).
- Rebuild the UA lists monthly from BigQuery and prune stale entries.
