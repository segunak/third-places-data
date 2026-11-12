---
name: Reddit Alerts
on:
  schedule:
    # Sunday at 2 PM Eastern Time; America/New_York handles EST/EDT automatically.
    # GitHub Actions: https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#onschedule
    # gh-aw: https://github.github.com/gh-aw/reference/schedule-syntax/#iana-timezone-field
    - cron: "0 14 * * 0"
      timezone: "America/New_York"
    # Monday, Wednesday, and Friday at 5 PM Eastern Time.
    - cron: "0 17 * * 1,3,5"
      timezone: "America/New_York"
  workflow_dispatch:
    inputs:
      mode:
        description: "Run Mode"
        required: false
        type: choice
        default: real
        options:
          - real
          - test
permissions:
  contents: read
  copilot-requests: write
timeout-minutes: 60
concurrency:
  job-discriminator: ${{ github.run_id }}
engine:
  id: copilot
  model: copilot # Aliases: https://github.github.com/gh-aw/reference/model-tables/#model-aliases
imports:
  - shared/email-report.md
tools:
  repo-memory:
    - id: third-place-alerts
      branch-name: memory/third-place-alerts
      description: "Durable state for third-place Reddit alert dedupe."
      file-glob:
        - "reddit/*.json"
      allowed-extensions: [".json"]
      max-file-size: 5242880
      max-file-count: 1
      max-patch-size: 524288
      create-orphan: true
      format-json: true
  bash:
    - "cat:*"
    - "date:*"
    - "jq:*"
    - "mkdir:*"
    - "node:*"
    - "python3:*"
    - "safeoutputs:*"
network:
  allowed:
    - defaults
    - github
    - www.reddit.com
safe-outputs:
  github-token: ${{ secrets.GH_AW_GITHUB_TOKEN }}
  report-failure-as-issue: false
  noop:
    max: 1
    report-as-issue: false
steps:
  # Keep query data separate to stay below the GitHub Actions script limit.
  - name: Write Reddit Alert Config
    run: |
      set -euo pipefail
      mkdir -p /tmp/gh-aw/agent

      node <<'NODE'
      const fs = require('fs');
      const queryFamilies = [
        { family: 'core-third-place', query: '"third place" OR "third places" OR "third-place" OR "third space" OR "third spaces"', terms: ['third place', 'third places', 'third-place', 'third space', 'third spaces'] },
        { family: 'coffee-cafe', query: 'coffee OR "coffee shop" OR "coffee shops" OR coffeeshop OR cafe OR café OR "coffee house" OR "cat cafe" OR latte OR cappuccino', terms: ['coffee', 'coffee shop', 'coffee shops', 'coffeeshop', 'cafe', 'café', 'coffee house', 'cat cafe', 'latte', 'cappuccino'] },
        { family: 'bakery-dessert', query: 'bakery OR bakeries OR pastry OR pastries OR bread OR sourdough OR baguette OR cookies OR dessert OR "ice cream" OR creamery', terms: ['bakery', 'bakeries', 'pastry', 'pastries', 'bread', 'sourdough', 'baguette', 'cookies', 'dessert', 'ice cream', 'creamery'] },
        { family: 'tea-boba', query: 'boba OR "bubble tea" OR matcha OR chai OR "tea house" OR "tea shop" OR "tea room" OR "earl grey"', terms: ['boba', 'bubble tea', 'matcha', 'chai', 'tea house', 'tea shop', 'tea room', 'earl grey'] },
        { family: 'study-work-laptop', query: '"study spot" OR "study spots" OR "best study spots" OR "place to study" OR "remote work" OR "work remotely" OR "laptop friendly" OR "laptop-friendly" OR "working on laptop" OR wifi OR "wi-fi"', terms: ['study spot', 'study spots', 'best study spots', 'place to study', 'remote work', 'work remotely', 'laptop friendly', 'laptop-friendly', 'working on laptop', 'wifi', 'wi-fi'] },
        { family: 'quiet-cozy-reading', query: '"quiet cafe" OR "quiet spot" OR "cozy cafe" OR "cozy spot" OR "places to read" OR "sit and read" OR "rocking chairs" OR "not too busy" OR "not too packed"', terms: ['quiet cafe', 'quiet spot', 'cozy cafe', 'cozy spot', 'places to read', 'sit and read', 'rocking chairs', 'not too busy', 'not too packed'] },
        { family: 'patio-outdoor', query: '"outdoor seating" OR "patio seating" OR "outdoor patio" OR patio OR "dog friendly patio" OR "dogs allowed outside"', terms: ['outdoor seating', 'patio seating', 'outdoor patio', 'patio', 'dog friendly patio', 'dogs allowed outside'] },
        { family: 'library-bookstore', query: 'library OR libraries OR "public library" OR bookstore OR bookstores OR "book store" OR "book stores" OR "book club" OR "book crawl"', terms: ['library', 'libraries', 'public library', 'bookstore', 'bookstores', 'book store', 'book stores', 'book club', 'book crawl'] },
        { family: 'meet-community', query: '"meet people" OR "meet new people" OR "make friends" OR "community space" OR "meeting space" OR "hobby group" OR "trivia night" OR "date spot"', terms: ['meet people', 'meet new people', 'make friends', 'community space', 'meeting space', 'hobby group', 'trivia night', 'date spot'] },
        { family: 'creative-markets', query: '"creative scene" OR "local art" OR "art collective" OR makerspace OR "maker market" OR "vintage market" OR "pop-up market" OR "community events" OR "live music"', terms: ['creative scene', 'local art', 'art collective', 'makerspace', 'maker market', 'vintage market', 'pop-up market', 'community events', 'live music'] },
        { family: 'game-social-hobby', query: '"board game" OR "board games" OR "board game cafe" OR "game night" OR "game nights" OR "game store" OR arcade OR "nerdy nooks" OR chess', terms: ['board game', 'board games', 'board game cafe', 'game night', 'game nights', 'game store', 'arcade', 'nerdy nooks', 'chess'] },
        { family: 'markets-local', query: '"farmers market" OR "farmer\'s market" OR "fresh produce" OR "local businesses" OR "neighborhood market" OR "public market"', terms: ['farmers market', "farmer's market", 'fresh produce', 'local businesses', 'neighborhood market', 'public market'] },
        { family: 'brewery-wine-hangout', query: 'brewery OR breweries OR taproom OR taprooms OR "wine bar" OR "wine bars" OR "bottle shop" OR "beer garden" OR "coffee and beer" OR "happy hour"', terms: ['brewery', 'breweries', 'taproom', 'taprooms', 'wine bar', 'wine bars', 'bottle shop', 'beer garden', 'coffee and beer', 'happy hour'] },
        { family: 'transit-adjacent', query: '"best coffee on the lightrail" OR "coffee on the lightrail" OR "lightrail coffee" OR "light rail coffee" OR "coffee near light rail" OR "cafe near light rail"', terms: ['best coffee on the lightrail', 'coffee on the lightrail', 'lightrail coffee', 'light rail coffee', 'coffee near light rail', 'cafe near light rail'] },
        { family: 'opening-closing', query: '"coming soon" OR "now open" OR "new cafe" OR "new coffee shop" OR "new bakery" OR "new taproom" OR "new bookstore" OR "new location" OR closing OR closed OR rebrand OR reopening', terms: ['coming soon', 'now open', 'new cafe', 'new coffee shop', 'new bakery', 'new taproom', 'new bookstore', 'new location', 'closing', 'closed', 'rebrand', 'reopening'] }
      ];

      fs.writeFileSync('/tmp/gh-aw/agent/reddit-query-families.json', JSON.stringify(queryFamilies));
      NODE

  - name: Collect Reddit Alert Candidates
    env:
      MODE: ${{ github.event.inputs.mode }}
    run: |
      set -euo pipefail
      mkdir -p /tmp/gh-aw/agent
      export MEMORY_DIR="/tmp/gh-aw/repo-memory/third-place-alerts"
      mkdir -p "$MEMORY_DIR/reddit"
      export MODE="${MODE:-real}"

      node <<'NODE'
      const fs = require('fs');
      const crypto = require('crypto');

      const mode = process.env.MODE || 'real';
      const memoryDir = process.env.MEMORY_DIR || '/tmp/gh-aw/repo-memory/third-place-alerts';
      const seenPath = `${memoryDir}/reddit/seen.json`;
      const outPath = '/tmp/gh-aw/agent/reddit-candidates.json';
      const logPath = '/tmp/gh-aw/agent/reddit-collection-log.json';
      const now = new Date();
      const sinceMs = now.getTime() - 48 * 60 * 60 * 1000;
      const mentionSinceMs = now.getTime() - 7 * 24 * 60 * 60 * 1000;
      const userAgent = 'script:charlotte-third-places-alerts:v1.0.0';
      const queryFamilies = JSON.parse(fs.readFileSync('/tmp/gh-aw/agent/reddit-query-families.json', 'utf8'));

      const categoryTerms = [...new Set(queryFamilies.flatMap((item) => item.terms))];
      const intentTerms = ['recommend', 'recommendation', 'best', 'where', 'looking for', 'iso', 'anyone know', 'suggest', 'favorite', 'place to', 'spots', 'near'];
      const highValueTerms = ['third place', 'third places', 'third-place', 'third space', 'third spaces'];
      const mentionQuery = '"Charlotte Third Places" OR "charlottethirdplaces.com" OR "6762573563" OR "com.charlottethirdplaces.app" OR url:charlottethirdplaces.com OR url:6762573563 OR url:com.charlottethirdplaces.app';
      const log = { mode, started_at: now.toISOString(), urls: [], errors: [] };

      function ensureSeenFile() {
        if (!fs.existsSync(seenPath)) fs.writeFileSync(seenPath, JSON.stringify({ items: [] }, null, 2) + String.fromCharCode(10));
      }

      function hashText(value) {
        return crypto.createHash('sha256').update(String(value || '').trim().toLowerCase()).digest('hex');
      }

      function readSeen() {
        ensureSeenFile();
        try {
          const parsed = JSON.parse(fs.readFileSync(seenPath, 'utf8'));
          return Array.isArray(parsed.items) ? parsed.items : [];
        } catch {
          return [];
        }
      }

      function decodeXml(value) {
        return String(value || '').replaceAll('&amp;', '&').replaceAll('&lt;', '<').replaceAll('&gt;', '>').replaceAll('&quot;', '"').replaceAll('&#39;', "'");
      }

      function tagValue(xml, tagName) {
        const open = xml.indexOf(`<${tagName}`);
        if (open < 0) return '';
        const contentStart = xml.indexOf('>', open) + 1;
        const close = xml.indexOf(`</${tagName}>`, contentStart);
        if (contentStart <= 0 || close < 0) return '';
        return decodeXml(xml.slice(contentStart, close)).replaceAll('<![CDATA[', '').replaceAll(']]>', '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
      }

      function linkHref(entry) {
        const marker = '<link';
        const open = entry.indexOf(marker);
        if (open < 0) return '';
        const end = entry.indexOf('>', open);
        const chunk = entry.slice(open, end);
        const key = 'href="';
        const hrefStart = chunk.indexOf(key);
        if (hrefStart < 0) return '';
        const valueStart = hrefStart + key.length;
        const valueEnd = chunk.indexOf('"', valueStart);
        return decodeXml(chunk.slice(valueStart, valueEnd));
      }

      function isSeen(candidate, seenItems) {
        return seenItems.some((item) => item.id === candidate.id || item.permalink === candidate.permalink || (!candidate.mention_matches?.length && item.text_hash === candidate.text_hash));
      }

      function findBrandMentions(value) {
        const text = decodeXml(String(value || ''));
        const matches = new Map();
        const name = text.replace(/[*_`]/g, '').match(/\bCharlotte[\s-]+Third[\s-]+Places\b/i);
        if (name) matches.set('name', { type: 'name', value: name[0].replace(/\s+/g, ' ') });
        for (const match of text.matchAll(/https?:\/\/[^\s<>"']+|(?<![\w.-])(?:www\.)?charlottethirdplaces\.com(?![\w-]|\.[\w-])(?:[/?#][^\s<>"']*)?/gi)) {
          const value = match[0].replace(/[)\],.;!?]+$/, '');
          const absolute = /^https?:\/\//i.test(value) ? value : `https://${value}`;
          if (!URL.canParse(absolute)) continue;
          const url = new URL(absolute);
          const host = url.hostname.replace(/\.$/, '');
          let type;
          if (host === 'charlottethirdplaces.com' || host === 'www.charlottethirdplaces.com') type = 'website';
          if (host === 'apps.apple.com' && /\/id6762573563(?:\/|$)/.test(url.pathname)) type = 'ios-app';
          if (host === 'play.google.com' && url.pathname === '/store/apps/details' && url.searchParams.get('id') === 'com.charlottethirdplaces.app') type = 'android-app';
          if (type && !matches.has(type)) matches.set(type, { type, value: url.toString() });
        }
        return [...matches.values()];
      }

      function withBrandMentions(candidate, text) {
        const matches = findBrandMentions(text);
        return { ...candidate, mention_matches: matches, relevance_score: matches.length ? 200 : candidate.relevance_score };
      }

      function scoreCandidate(text, sourceKind, titleMatch) {
        const lower = String(text || '').toLowerCase();
        if (highValueTerms.some((term) => lower.includes(term))) return 100;
        if (sourceKind.includes('comments')) return 80;
        if (titleMatch) return 70;
        if (sourceKind.includes('recent')) return 40;
        return 60;
      }

      function matchesCommentFetchRule(text) {
        const lower = String(text || '').toLowerCase();
        if (categoryTerms.some((term) => lower.includes(term))) return true;
        return intentTerms.some((term) => lower.includes(term)) && categoryTerms.some((term) => lower.includes(term));
      }

      function isHighValueThread(text) {
        const lower = String(text || '').toLowerCase();
        return highValueTerms.some((term) => lower.includes(term));
      }

      async function fetchWithRetry(url) {
        const delays = [0, 5000, 12000];
        let lastError = null;
        for (let attempt = 0; attempt < delays.length; attempt += 1) {
          if (delays[attempt] > 0) await new Promise((resolve) => setTimeout(resolve, delays[attempt] + Math.floor(Math.random() * 1000)));
          try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 30000);
            const response = await fetch(url, { headers: { 'User-Agent': userAgent }, signal: controller.signal });
            clearTimeout(timeout);
            const body = await response.text();
            if (response.ok) return { ok: true, status: response.status, body };
            lastError = { status: response.status, body: body.slice(0, 500) };
            const retryAfter = Number(response.headers.get('retry-after') || 0);
            if (retryAfter > 0 && attempt < delays.length - 1) await new Promise((resolve) => setTimeout(resolve, Math.min(retryAfter * 1000, 90000)));
          } catch (error) {
            lastError = { status: 'network', body: String(error.message || error) };
          }
        }
        return { ok: false, error: lastError };
      }

      function normalizePost(child, family, sourceKind, sourceUrl) {
        const data = child.data || child;
        const created = Number(data.created_utc || 0) * 1000;
        const title = data.title || '';
        const text = data.selftext || '';
        const outboundUrl = data.url_overridden_by_dest || data.url || null;
        const permalink = data.permalink ? `https://www.reddit.com${data.permalink}` : null;
        return withBrandMentions({
          id: data.name || (data.id ? `t3_${data.id}` : null),
          post_id: data.id || null,
          comment_id: null,
          permalink,
          created_at: created ? new Date(created).toISOString() : null,
          author: data.author || null,
          score: typeof data.score === 'number' ? data.score : null,
          title,
          text,
          outbound_url: outboundUrl,
          source_query_family: family,
          source_kind: sourceKind,
          source_url: sourceUrl,
          text_hash: hashText([title, text].join(' ')),
          relevance_score: scoreCandidate([title, text].join(' '), sourceKind, Boolean(title))
        }, [title, text, outboundUrl].join(' '));
      }

      function normalizeComment(child, post, sourceUrl) {
        const data = child.data || child;
        const created = Number(data.created_utc || 0) * 1000;
        const text = data.body || '';
        const permalink = data.permalink ? new URL(data.permalink, 'https://www.reddit.com').toString() : (post.permalink && data.id ? `${post.permalink}${data.id}/` : post.permalink);
        return withBrandMentions({
          id: data.name || (data.id ? `t1_${data.id}` : null),
          post_id: post.post_id || String(data.link_id || '').replace(/^t3_/, '') || null,
          comment_id: data.id || null,
          permalink,
          created_at: created ? new Date(created).toISOString() : null,
          author: data.author || null,
          score: typeof data.score === 'number' ? data.score : null,
          title: post.title || data.link_title || 'Comment in r/Charlotte',
          text,
          source_query_family: post.source_query_family,
          source_kind: 'comments-json',
          source_url: sourceUrl,
          text_hash: hashText([post.title, text].join(' ')),
          relevance_score: scoreCandidate([post.title, text].join(' '), 'comments-json', false)
        }, text);
      }

      async function collectJsonListing(base, family, sourceKind, { normalize = normalizePost, cutoffMs = sinceMs, maxPages = 5 } = {}) {
        const collected = [];
        let after = '';
        for (let page = 1; page <= maxPages; page += 1) {
          const url = new URL(base);
          url.searchParams.set('limit', '100');
          url.searchParams.set('raw_json', '1');
          if (after) url.searchParams.set('after', after);
          const fullUrl = url.toString();
          log.urls.push(fullUrl);
          const response = await fetchWithRetry(fullUrl);
          if (!response.ok) throw new Error(`json fetch failed: ${JSON.stringify(response.error)}`);
          let parsed;
          try { parsed = JSON.parse(response.body); } catch (error) { throw new Error(`invalid json: ${error.message}`); }
          const children = parsed?.data?.children || [];
          collected.push(...children.map((child) => normalize(child, family, sourceKind, fullUrl)).filter((item) => item.created_at && Date.parse(item.created_at) >= cutoffMs));
          after = parsed?.data?.after || '';
          const newestInsideLookback = children.some((child) => Number(child?.data?.created_utc || 0) * 1000 >= cutoffMs);
          if (!after || children.length < 100 || !newestInsideLookback) break;
          if (page === maxPages && family === 'brand-mentions') {
            log.errors.push({ family, source_kind: sourceKind, error: 'Listing limit reached before the end of the mention lookback window.' });
            console.warn(`Brand mention coverage is incomplete for ${fullUrl}.`);
          }
        }
        return collected;
      }

      async function collectRss(query, family, { sitewide = false, comments = false } = {}) {
        const url = comments
          ? 'https://www.reddit.com/r/Charlotte/comments.rss'
          : (sitewide ? `https://www.reddit.com/search.rss?sort=new&t=week&q=${encodeURIComponent(query)}` : `https://www.reddit.com/r/Charlotte/search.rss?restrict_sr=on&sort=new&t=day&q=${encodeURIComponent(query)}`);
        log.urls.push(url);
        const response = await fetchWithRetry(url);
        if (!response.ok) {
          log.errors.push({ family, rss_error: response.error });
          return [];
        }
        return response.body.split('<entry>').slice(1).map((entryChunk) => {
          const entry = entryChunk.split('</entry>')[0] || '';
          const title = tagValue(entry, 'title');
          const text = tagValue(entry, 'content');
          const permalink = linkHref(entry) || null;
          const createdAt = tagValue(entry, 'updated') || null;
          const rawContent = entry.match(/<content\b[^>]*>([\s\S]*?)<\/content>/i)?.[1] || '';
          return withBrandMentions({
            id: permalink ? `rss_${hashText(permalink).slice(0, 20)}` : `rss_${hashText([title, createdAt].join(' ')).slice(0, 20)}`,
            post_id: null,
            comment_id: null,
            permalink,
            created_at: createdAt,
            author: null,
            score: null,
            title,
            text,
            source_query_family: family,
            source_kind: comments ? 'comments-rss' : 'search-rss',
            source_url: url,
            text_hash: hashText([title, text].join(' ')),
            relevance_score: scoreCandidate([title, text].join(' '), 'search-rss', Boolean(title))
          }, comments ? rawContent : entry);
        }).filter((item) => item.created_at && Date.parse(item.created_at) >= (sitewide || comments ? mentionSinceMs : sinceMs));
      }

      async function collectBrandMentions() {
        let posts;
        const url = new URL('https://www.reddit.com/search.json');
        url.searchParams.set('sort', 'new');
        url.searchParams.set('t', 'week');
        url.searchParams.set('q', mentionQuery);
        try {
          posts = await collectJsonListing(url.toString(), 'brand-mentions', 'brand-search-json', { cutoffMs: mentionSinceMs, maxPages: 10 });
        } catch (error) {
          log.errors.push({ family: 'brand-mentions', json_error: error.message });
          posts = await collectRss(mentionQuery, 'brand-mentions', { sitewide: true });
        }
        let comments;
        try {
          comments = await collectJsonListing('https://www.reddit.com/r/Charlotte/comments.json', 'brand-mentions', 'brand-comments-json', {
            normalize: (child, family, _sourceKind, sourceUrl) => normalizeComment(child, { source_query_family: family }, sourceUrl),
            cutoffMs: mentionSinceMs,
            maxPages: 10
          });
        } catch (error) {
          log.errors.push({ family: 'brand-mentions', comments_error: error.message });
          comments = await collectRss('', 'brand-mentions', { comments: true });
        }
        return [...posts, ...comments].filter((item) => item.mention_matches.length > 0);
      }

      async function collectComments(posts) {
        const comments = [];
        for (const post of posts.slice(0, 40)) {
          const combined = [post.title, post.text].join(' ');
          if (!matchesCommentFetchRule(combined)) continue;
          const postId = post.post_id || (post.permalink && post.permalink.includes('/comments/') ? post.permalink.split('/comments/')[1].split('/')[0] : null);
          if (!postId) continue;
          const url = `https://www.reddit.com/r/Charlotte/comments/${postId}.json?sort=new&limit=100&raw_json=1`;
          log.urls.push(url);
          const response = await fetchWithRetry(url);
          if (!response.ok) {
            log.errors.push({ post_id: postId, comments_error: response.error });
            continue;
          }
          try {
            const parsed = JSON.parse(response.body);
            const children = parsed?.[1]?.data?.children || [];
            const maxComments = isHighValueThread(combined) ? 200 : 100;
            for (const child of children.slice(0, maxComments)) {
              if (child.kind !== 't1') continue;
              const item = normalizeComment(child, post, url);
              if (item.created_at && Date.parse(item.created_at) >= sinceMs && matchesCommentFetchRule([post.title, item.text].join(' '))) comments.push(item);
            }
          } catch (error) {
            log.errors.push({ post_id: postId, comments_parse_error: error.message });
          }
        }
        return comments;
      }

      async function main() {
        ensureSeenFile();
        if (mode === 'test') {
          fs.writeFileSync(outPath, JSON.stringify({
            mode,
            test: true,
            generated_at: now.toISOString(),
            candidates: [{
              id: 'fixture_reddit_third_place',
              post_id: 'test',
              comment_id: null,
              permalink: 'https://www.reddit.com/r/Charlotte/',
              created_at: now.toISOString(),
              author: null,
              score: null,
              title: 'Test: Looking for a cozy third place near Plaza Midwood',
              text: 'Synthetic test candidate for validating the Third Place Reddit Alerts email path.',
              source_query_family: 'core-third-place',
              source_kind: 'test',
              source_url: null,
              text_hash: hashText('fixture_reddit_third_place'),
              relevance_score: 100
            }]
          }, null, 2));
          fs.writeFileSync(logPath, JSON.stringify(log, null, 2));
          return;
        }
        const seen = readSeen();
        const all = [];
        for (const entry of queryFamilies) {
          const url = new URL('https://www.reddit.com/r/Charlotte/search.json');
          url.searchParams.set('restrict_sr', '1');
          url.searchParams.set('sort', 'new');
          url.searchParams.set('t', 'day');
          url.searchParams.set('q', entry.query);
          try {
            all.push(...await collectJsonListing(url.toString(), entry.family, 'search-json'));
          } catch (error) {
            log.errors.push({ family: entry.family, json_error: error.message });
            all.push(...await collectRss(entry.query, entry.family));
          }
        }
        try {
          all.push(...await collectJsonListing('https://www.reddit.com/r/Charlotte/new.json?limit=100&raw_json=1', 'recent-post-sweep', 'recent-post-sweep'));
        } catch (error) {
          log.errors.push({ family: 'recent-post-sweep', json_error: error.message });
        }
        all.push(...await collectComments(all));
        all.push(...await collectBrandMentions());
        const byId = new Map();
        for (const item of all) {
          const key = item.id || item.permalink || item.text_hash;
          if (!key) continue;
          if (!byId.has(key) || byId.get(key).relevance_score < item.relevance_score) byId.set(key, item);
        }
        const candidates = [...byId.values()]
          .filter((item) => !isSeen(item, seen))
          .sort((a, b) => (b.relevance_score - a.relevance_score) || (Date.parse(b.created_at || 0) - Date.parse(a.created_at || 0)))
          .slice(0, 200);
        fs.writeFileSync(outPath, JSON.stringify({ mode, test: false, generated_at: now.toISOString(), lookback_hours: 48, mention_lookback_hours: 168, query_families: [...queryFamilies.map(({ family, query }) => ({ family, query })), { family: 'brand-mentions', query: mentionQuery }], candidates }, null, 2));
        fs.writeFileSync(logPath, JSON.stringify(log, null, 2));
      }

      main().catch((error) => {
        log.errors.push({ fatal: error.message, stack: error.stack });
        fs.writeFileSync(logPath, JSON.stringify(log, null, 2));
        fs.writeFileSync(outPath, JSON.stringify({ mode, test: false, generated_at: now.toISOString(), candidates: [], error: error.message }, null, 2));
      });
      NODE
---

## Reddit Alerts

You monitor `r/Charlotte` for third-place leads. You also monitor posts across Reddit and recent `r/Charlotte` comments for Charlotte Third Places mentions.

## Required Inputs

Read these files first:

- `/tmp/gh-aw/agent/reddit-candidates.json`
- `/tmp/gh-aw/agent/reddit-collection-log.json`
- `/tmp/gh-aw/repo-memory/third-place-alerts/reddit/seen.json`

## Test Rules

If `mode` is `test`, send exactly one test email using `send_email_report`, then call no other safe-output tool. Do not update repo memory in test mode.

## Brand Mentions

Candidates with a non-empty `mention_matches` array are always relevant. Include positive, negative, and neutral mentions of:

- The name `Charlotte Third Places`.
- `charlottethirdplaces.com`, with or without `www`, including all paths.
- The iOS app, identified by `apps.apple.com` and app ID `6762573563`.
- The Android app, identified by `play.google.com` and package ID `com.charlottethirdplaces.app`.

Do not require a place recommendation, a Charlotte subreddit, or any other lead category for these matches. Use the Reddit permalink and the evidence in `mention_matches`.

Put mentions first in a `Charlotte Third Places Mentions` section. Fill the existing 20-item report limit with mentions before ordinary leads. Only add items to memory if the email includes them. Any remaining items must stay eligible for a later run.

Mention checks use the existing workflow schedule and a seven-day overlap. API and listing limits can prevent complete coverage. If the collection log reports a mention error or limit, include a short coverage note in the report or `noop` reason.

## Relevance Rules

Other Reddit candidates must match at least one of these exact categories:

- Requests for coffee shops, cafes, tea shops, bakeries, bookstores, libraries, markets, community spaces, study spots, remote-work spots, quiet/cozy hangouts, creative spaces, game/social hobby spaces, or places to meet people.
- Chatter about new or upcoming third-place openings.
- Comments that recommend a place or reveal unmet demand for a kind of third place.
- Restaurant/bar chatter only when it includes one of these third-place signals: coffee/cafe/tea/bakery/dessert, brewery/taproom/wine bar/bottle shop, bookstore/library, market, coworking/workspace, board games/games/arcade, live music/events, patio/hangout/cozy/quiet, laptop/Wi-Fi/study/work, community/meetup/group language, or a named neighborhood gathering-place angle.

Not relevant:

- Generic crime, politics, sports, weather, traffic, or unrelated local news.
- Restaurants/bars that only mention a meal, menu item, chef, lawsuit, price, health score, or generic dining experience without any third-place signal.
- Old/repeated items already in memory.

## Email Requirements

If relevant new items exist, call `send_email_report` with:

- `subject`: `Third Place Reddit Alerts - YYYY-MM-DD`
- `text_body`: a plain text report with the same items.

Formatting rules:

- Do not provide `html_body`; the shared email job generates HTML from `text_body`.
- `text_body` must be readable plain text. Do not include HTML tags, `<!doctype html>`, `(!doctype html)`, pseudo-tags like `(div ...)`, or Markdown table formatting.
- Simple Markdown headings, bullets, bold, italic, and links are allowed in `text_body` because the shared email job renders them deterministically.

Include at most 20 items. Rank items in this order:

1. Charlotte Third Places mentions identified by `mention_matches`.
2. Exact `third place` / `third space` matches.
3. Openings, now-open items, reopens, and new locations.
4. Closings, rebrands, expansions, and renovations.
5. Requests for recommendations, unmet-demand posts, and recommendation comments.
6. Reviews, spotlights, features, recurring events, and community/creative-scene leads.
7. Everything else that still passed relevance.

Each item must include title, source/permalink, why it matters, and matched evidence.

## Repo Memory Update

After calling `send_email_report` in real mode, update `/tmp/gh-aw/repo-memory/third-place-alerts/reddit/seen.json` with only the relevant notified items. Keep this shape:

```json
{
  "items": [
    {
      "id": "t3_example",
      "permalink": "https://www.reddit.com/r/Charlotte/comments/example/",
      "title": "Example",
      "text_hash": "sha256",
      "first_seen_at": "2026-07-07T00:00:00.000Z",
      "last_notified_at": "2026-07-07T00:00:00.000Z",
      "source_query_family": "coffee-cafe"
    }
  ]
}
```

Prune entries older than 90 days based on `last_notified_at` when present, otherwise `first_seen_at`. Keep at most 10,000 entries.

If there are no relevant new items, call `noop` with a short reason and do not update repo memory.
