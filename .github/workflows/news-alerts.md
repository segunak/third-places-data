---
name: News Alerts
on:
  schedule:
    # Daily at 5 PM ET during daylight saving time, 21:00 UTC.
    - cron: "0 21 * * *"
    # Daily at 5 PM ET during standard time, 22:00 UTC.
    # The America/New_York gate below skips whichever UTC run is not currently 5 PM ET.
    - cron: "0 22 * * *"
  workflow_dispatch:
    inputs:
      mode:
        description: "Run mode. Use test to verify email formatting without repo-memory writes."
        required: false
        type: choice
        default: real
        options:
          - real
          - test
      aw_context:
        default: "{}"
        description: "Agent caller context (used internally by Agentic Workflows)."
        required: false
        type: string
permissions:
  contents: read
  copilot-requests: write
timeout-minutes: 60
# Docs: https://github.github.com/gh-aw/reference/model-tables/#model-aliases
# Docs: https://github.github.com/gh-aw/specs/model-alias-specification/#61-effort
engine:
  id: copilot
  model: copilot
imports:
  - shared/email-report.md
tools:
  repo-memory:
    - id: third-place-alerts
      branch-name: memory/third-place-alerts
      description: "Durable state for third-place Reddit and news alert dedupe."
      file-glob:
        - "reddit/*.json"
        - "news/*.json"
      allowed-extensions: [".json"]
      max-file-size: 5242880
      max-file-count: 2
      max-patch-size: 524288
      create-orphan: true
      format-json: true
  bash:
    - "cat:*"
    - "date:*"
    - "mkdir:*"
    - "node:*"
    - "python3:*"
    - "safeoutputs:*"
network:
  allowed:
    - defaults
    - github
    - news.google.com
    - www.charlotteobserver.com
    - www.axios.com
    - www.wsoctv.com
    - www.wbtv.com
    - www.wcnc.com
    - www.bizjournals.com
    - www.charlottemagazine.com
    - qcnerve.com
    - www.wfae.org
    - www.qcitymetro.com
    - www.heraldonline.com
    - www.charlottestories.com
    - www.ballantynemagazine.com
    - www.qcnews.com
    - www.wccbcharlotte.com
    - whatnow.com
    - www.thecharlotteledger.com
    - southparkmagazine.com
    - www.lakenormanpublications.com
    - mooresvilletribune.com
    - independenttribune.com
    - enquirerjournal.com
    - www.corneliustoday.com
    - www.newsofdavidson.org
    - newsofdavidson.org
    - www.salisburypost.com
    - www.gastongazette.com
    - statesville.com
    - clclt.com
    - www.pmg-sc.com
safe-outputs:
  github-token: ${{ secrets.GH_AW_GITHUB_TOKEN }}
  report-failure-as-issue: false
  noop:
    max: 1
    report-as-issue: false
steps:
  - name: Write News Alert Config
    run: |
      set -euo pipefail
      mkdir -p /tmp/gh-aw/agent

      node <<'NODE'
      const fs = require('fs');

      const locationTerms = [
        'Charlotte', 'Charlotte, NC', 'CLT', 'Mecklenburg County',
        'Arbor Glen', 'Ashley Park', 'Ballantyne', 'Belmont', 'Bradfield Farms', 'Carmel', 'China Grove', 'Closeburn - Glenkirk', 'Collingwood', 'Commonwealth Park', 'Concord', 'Cornelius', 'Cotswold', 'Dallas', 'Davidson', 'Davis Lake - Eastfield', 'Denver', 'Dilworth', 'Druid Hills South', 'East Forest', 'Eastland - Wilora Lake', 'Eastover', 'Eastway', 'Elizabeth', 'Enderly Park', 'Fort Mill', 'Gastonia', 'Grier Heights', 'Harrisburg', 'Hickory Grove', 'Highland Creek', 'Huntersville', 'Independence Blvd', 'Indian Land', 'Indian Trail', 'Kannapolis', 'Kings Mountain', 'Landis', 'Lincoln Heights', 'Lockwood', 'LoSo', 'Lowell', 'Madison Park', 'Matthews', 'Midtown', 'Mineral Springs', 'Mint Hill', 'Monroe', 'Montclaire South', 'Montford', 'Mooresville', 'Mount Holly', 'Mountain Island', 'Myers Park', 'NoDa', 'North Sharon Amity / Reddman Road', 'Northwest Charlotte', 'Oakhurst', 'Olde Whitehall', 'Optimist Park', 'Pineville', 'Piper Glen', 'Plaza Midwood', 'Plaza Shamrock', 'Providence Crossing', 'Providence Park', 'Quail Hollow', 'Raintree', 'Renaissance Park', 'Rock Hill', 'Sardis Crossing', 'Sardis Woods', 'Seversville', 'Sheffield Park', 'South End', 'SouthPark', 'Stallings', 'Stanley', 'Starmount', 'Steele Creek', 'Sugar Creek', 'Tega Cay', 'University City', 'Uptown', 'Washington Heights', 'Waverly', 'Waxhaw', 'Wedgewood', 'Wesley Heights', 'West Sugar Creek',
        'Cabarrus County', 'Union County', 'Gaston County', 'Iredell County', 'Lincoln County', 'York County', 'Lancaster County', 'Rowan County', 'Cleveland County', 'Lake Norman', 'Salisbury', 'Statesville', 'Troutman', 'Lincolnton', 'Lancaster', 'Sherrills Ford', 'Lowesville'
      ];
      const googleLocationTerms = ['Charlotte', '"Charlotte, NC"', 'CLT', '"Mecklenburg County"', '"South End"', 'NoDa', '"Plaza Midwood"', 'SouthPark', 'Ballantyne', '"Lake Norman"', 'Huntersville', 'Cornelius', 'Davidson', 'Mooresville', 'Concord', 'Kannapolis', 'Harrisburg', 'Matthews', 'Pineville', '"Mint Hill"', '"Indian Trail"', 'Waxhaw', 'Monroe', 'Gastonia', 'Belmont', '"Mount Holly"', '"Fort Mill"', '"Rock Hill"', '"Indian Land"', '"Tega Cay"', 'Denver', '"Kings Mountain"', 'Salisbury', 'Statesville', 'Lincolnton', '"Cabarrus County"', '"Union County"', '"Gaston County"', '"Iredell County"', '"Lincoln County"', '"York County"', '"Lancaster County"', '"Rowan County"', '"Cleveland County"'];
      const subjectTerms = ['coffee', 'espresso', 'cafe', 'café', 'coffee bar', 'coffeehouse', 'coffee shop', 'tea', 'tea house', 'tea room', 'tea bar', 'boba', 'bubble tea', 'bakery', 'pastry', 'pastry shop', 'donut shop', 'deli', 'eatery', 'restaurant', 'food truck', 'food hall', 'ice cream', 'creamery', 'gelato', 'dessert cafe', 'bookstore', 'book shop', 'bookshop', 'library', 'public library', 'branch library', 'community center', 'coworking', 'coworking space', 'co-working', 'workspace', 'study spot', 'brewery', 'taproom', 'beer garden', 'wine bar', 'bar', 'pub', 'speakeasy', 'bottle shop', 'beer shop', 'market', 'farmers market', 'grocery store', 'garden', 'community garden', 'park', 'greenway', 'public space', 'plaza', 'art gallery', 'art studio', 'arts center', 'performing arts', 'theater', 'music venue', 'live music', 'museum', 'makerspace', 'game store', 'board game', 'gaming lounge', 'arcade', 'book club', 'event venue', 'third place', 'third places', 'third space'];
      const actionTerms = ['grand opening', 'opening soon', 'now open', 'new location', 'new concept', 'debut', 'review', 'spotlight', 'feature', 'closes', 'closing', 'reopens', 'renovation', 'expansion'];
      const excludedHrefParts = ['login', 'subscribe', 'newsletter', '/author/', '/tag/', '/tags/', '/video/', '.jpg', '.png', '.gif', '.webp', 'mailto:', 'javascript:', '#'];
      const sources = [
        { name: 'Charlotte Observer / CharlotteFive', site: 'charlotteobserver.com/charlottefive', urls: ['https://www.charlotteobserver.com/charlottefive/', 'https://www.charlotteobserver.com/charlottefive/c5-food-drink/', 'https://www.charlotteobserver.com/charlottefive/c5-around-town/', 'https://www.charlotteobserver.com/charlottefive/c5-things-to-do/'] },
        { name: 'Axios Charlotte', site: 'www.axios.com/local/charlotte', ignore_url_location: true, urls: ['https://www.axios.com/local/charlotte', 'https://www.axios.com/local/charlotte/food-and-drink', 'https://www.axios.com/local/charlotte/things-to-do', 'https://www.axios.com/local/charlotte/business', 'https://www.axios.com/local/charlotte/newcomers'] },
        { name: 'WSOC-TV', site: 'wsoctv.com', urls: ['https://www.wsoctv.com/news/local/', 'https://www.wsoctv.com/community/', 'https://www.wsoctv.com/your704/'] },
        { name: 'WBTV', site: 'wbtv.com', urls: ['https://www.wbtv.com/news/local/', 'https://www.wbtv.com/community/', 'https://www.wbtv.com/content/food/', 'https://www.wbtv.com/community/features', 'https://www.wbtv.com/community/new-to-charlotte'] },
        { name: 'WCNC', site: 'wcnc.com', feeds: ['https://www.wcnc.com/feeds/syndication/rss/news/local'], urls: ['https://www.wcnc.com/local'] },
        { name: 'Queen City News', site: 'qcnews.com', feeds: ['https://www.qcnews.com/feed/'], urls: ['https://www.qcnews.com/news/local-news/', 'https://www.qcnews.com/news/u-s/north-carolina/', 'https://www.qcnews.com/charlotte/'] },
        { name: 'WCCB Charlotte', site: 'wccbcharlotte.com', feeds: ['https://www.wccbcharlotte.com/feed/'], urls: ['https://www.wccbcharlotte.com/category/local-news/', 'https://www.wccbcharlotte.com/category/entertainment/', 'https://www.wccbcharlotte.com/events/'] },
        { name: 'Charlotte Business Journal', site: 'bizjournals.com/charlotte', urls: ['https://www.bizjournals.com/charlotte/news/food-and-lifestyle', 'https://www.bizjournals.com/charlotte/news/food-and-lifestyle/restaurants', 'https://www.bizjournals.com/charlotte/news/food-and-lifestyle/bars', 'https://www.bizjournals.com/charlotte/news/food-and-lifestyle/arts', 'https://www.bizjournals.com/charlotte/news/food-and-lifestyle/country-clubs'] },
        { name: 'Charlotte Inno', site: 'bizjournals.com/charlotte/inno', urls: ['https://www.bizjournals.com/charlotte/inno'] },
        { name: 'What Now Charlotte', site: 'whatnow.com/charlotte/', ignore_url_location: true, feeds: ['https://whatnow.com/charlotte/feed/'], urls: ['https://whatnow.com/charlotte/restaurants/', 'https://whatnow.com/charlotte/retail/', 'https://whatnow.com/charlotte/real-estate/'] },
        { name: 'The Charlotte Ledger', site: 'thecharlotteledger.com', urls: ['https://www.thecharlotteledger.com/archive', 'https://www.thecharlotteledger.com/'] },
        { name: 'Charlotte Magazine', site: 'charlottemagazine.com', urls: ['https://www.charlottemagazine.com/food-drink/', 'https://www.charlottemagazine.com/things-to-do/', 'https://www.charlottemagazine.com/arts-culture/'] },
        { name: 'SouthPark Magazine', site: 'southparkmagazine.com', feeds: ['https://southparkmagazine.com/feed/'], urls: ['https://southparkmagazine.com/category/cuisine/', 'https://southparkmagazine.com/category/entertainment/', 'https://southparkmagazine.com/category/the-arts/'] },
        { name: 'Queen City Nerve / QC Nerve', site: 'qcnerve.com', feeds: ['https://qcnerve.com/feed/'], urls: ['https://qcnerve.com/category/food-drink/', 'https://qcnerve.com/category/guides-events/', 'https://qcnerve.com/category/arts-culture/', 'https://qcnerve.com/category/news-opinion/small-business/'] },
        { name: 'WFAE', site: 'wfae.org', feeds: ['https://www.wfae.org/index.rss'], urls: ['https://www.wfae.org/arts-culture', 'https://www.wfae.org/business', 'https://www.wfae.org/local-news'] },
        { name: 'QCity Metro', site: 'qcitymetro.com', urls: ['https://www.qcitymetro.com/latest-stories', 'https://www.qcitymetro.com/sections/things-to-do', 'https://www.qcitymetro.com/sections/food-drink', 'https://www.qcitymetro.com/sections/culture', 'https://www.qcitymetro.com/things-to-do/calendar/'] },
        { name: 'Creative Loafing / clclt.com', site: 'clclt.com', feeds: ['https://clclt.com/feed'], urls: [] },
        { name: 'Charlotte Stories', site: 'charlottestories.com', feeds: ['https://www.charlottestories.com/feed/'], urls: ['https://www.charlottestories.com/category/food/', 'https://www.charlottestories.com/category/development/', 'https://www.charlottestories.com/category/charlotte/'] },
        { name: 'Ballantyne Magazine', site: 'ballantynemagazine.com', urls: ['https://www.ballantynemagazine.com/', 'https://www.ballantynemagazine.com/food-drink/', 'https://www.ballantynemagazine.com/around-town/'] },
        { name: 'Herald Online / Rock Hill Herald', site: 'heraldonline.com', urls: ['https://www.heraldonline.com/news/local/', 'https://www.heraldonline.com/news/business/', 'https://www.heraldonline.com/living/food-drink/'] },
        { name: 'Lake Norman Publications', site: 'lakenormanpublications.com', feeds: ['https://www.lakenormanpublications.com/feed/'], urls: ['https://www.lakenormanpublications.com/category/news/', 'https://www.lakenormanpublications.com/category/business/'] },
        { name: 'Cornelius Today', site: 'corneliustoday.com', feeds: ['https://www.corneliustoday.com/feed/'], urls: ['https://www.corneliustoday.com/', 'https://www.corneliustoday.com/category/events/', 'https://www.corneliustoday.com/category/whats-delicious/'] },
        { name: 'News of Davidson', site: 'newsofdavidson.org', feeds: ['https://www.newsofdavidson.org/feed/'], urls: ['https://newsofdavidson.org/category/local-businesses/', 'https://newsofdavidson.org/category/news/', 'https://newsofdavidson.org/category/a-and-e/'] },
        { name: 'Mooresville Tribune', site: 'mooresvilletribune.com', feeds: ['https://mooresvilletribune.com/search/?f=rss&t=article&c=news/local&l=50&s=start_time&sd=desc'], urls: ['https://mooresvilletribune.com/news/local/'] },
        { name: 'Statesville Record & Landmark', site: 'statesville.com', feeds: ['https://statesville.com/search/?f=rss&t=article&c=news/local&l=50&s=start_time&sd=desc'], urls: ['https://statesville.com/news/local/'] },
        { name: 'Independent Tribune', site: 'independenttribune.com', feeds: ['https://independenttribune.com/search/?f=rss&t=article&c=news/local&l=50&s=start_time&sd=desc'], urls: ['https://independenttribune.com/news/local/'] },
        { name: 'Enquirer-Journal', site: 'enquirerjournal.com', feeds: ['https://enquirerjournal.com/search/?f=rss&t=article&c=news&l=50&s=start_time&sd=desc'], urls: ['https://enquirerjournal.com/news/', 'https://enquirerjournal.com/enquirer_journal/'] },
        { name: 'Salisbury Post', site: 'salisburypost.com', feeds: ['https://www.salisburypost.com/feed/'], urls: ['https://www.salisburypost.com/category/news/', 'https://www.salisburypost.com/category/lifestyle/'] },
        { name: 'Gaston Gazette', site: 'gastongazette.com', urls: ['https://www.gastongazette.com/'] },
        { name: 'Carolina Gateway', site: 'pmg-sc.com/carolina_gateway', feeds: ['https://www.pmg-sc.com/carolina_gateway/search/?f=rss&t=article&c=news&l=50&s=start_time&sd=desc'], urls: ['https://www.pmg-sc.com/carolina_gateway/'] }
      ];
      const googleTemplates = [
        { family: 'openings-food', template: 'site:{site} (coffee OR "coffee shop" OR cafe OR café OR bakery OR tea OR boba OR restaurant OR deli OR "food truck" OR "food hall" OR "ice cream") {locations} ("grand opening" OR "opening soon" OR "now open" OR "new location" OR debut) when:7d' },
        { family: 'places-community', template: 'site:{site} (bookstore OR library OR "community center" OR coworking OR makerspace OR "art gallery" OR "arts center" OR "performing arts" OR theater OR museum OR "game store" OR arcade OR market OR "farmers market" OR park OR greenway OR "third place") {locations} (opening OR new OR feature OR spotlight OR review OR event) when:7d' },
        { family: 'drinks-hangouts', template: 'site:{site} (brewery OR taproom OR "beer garden" OR "wine bar" OR bar OR pub OR speakeasy OR "bottle shop" OR "live music") {locations} (opening OR "now open" OR "new location" OR review OR feature) when:7d' },
        { family: 'closures-changes', template: 'site:{site} (coffee OR cafe OR restaurant OR bakery OR brewery OR bookstore OR library OR market OR coworking OR "arts center" OR "third place") {locations} (closes OR closing OR reopens OR renovation OR expansion OR rebrand) when:7d' }
      ];

      fs.writeFileSync('/tmp/gh-aw/agent/news-alert-config.json', JSON.stringify({
        locationTerms,
        googleLocationQuery: `(${googleLocationTerms.join(' OR ')})`,
        subjectTerms,
        actionTerms,
        excludedHrefParts,
        sources,
        googleTemplates
      }, null, 2));
      NODE

  - name: Collect News Alert Candidates
    env:
      MODE: ${{ github.event.inputs.mode }}
      EVENT_NAME: ${{ github.event_name }}
    run: |
      set -euo pipefail
      mkdir -p /tmp/gh-aw/agent
      export MEMORY_DIR="/tmp/gh-aw/repo-memory/third-place-alerts"
      mkdir -p "$MEMORY_DIR/news"
      export MODE="${MODE:-real}"
      export NY_HOUR="$(TZ=America/New_York date +%H:%M)"
      export SHOULD_RUN="true"
      export GATE_REASON="manual dispatch or active 5 PM ET collection window"
      if [ "$EVENT_NAME" = "schedule" ] && [ "$NY_HOUR" != "17:00" ]; then
        export SHOULD_RUN="false"
        export GATE_REASON="scheduled UTC run skipped because America/New_York local time is ${NY_HOUR}, not 17:00"
      fi

      node <<'NODE'
      const fs = require('fs');
      const crypto = require('crypto');

      const mode = process.env.MODE || 'real';
      const shouldRun = process.env.SHOULD_RUN || 'true';
      const gateReason = process.env.GATE_REASON || '';
      const memoryDir = process.env.MEMORY_DIR || '/tmp/gh-aw/repo-memory/third-place-alerts';
      const seenPath = `${memoryDir}/news/seen.json`;
      const outPath = '/tmp/gh-aw/agent/news-candidates.json';
      const logPath = '/tmp/gh-aw/agent/news-collection-log.json';
      const now = new Date();
      const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
      const sinceMs = now.getTime() - sevenDaysMs;
      const userAgent = 'script:charlotte-third-places-alerts:v1.0.0';
      const config = JSON.parse(fs.readFileSync('/tmp/gh-aw/agent/news-alert-config.json', 'utf8'));
      const { locationTerms, googleLocationQuery, subjectTerms, actionTerms, excludedHrefParts, sources, googleTemplates } = config;
      const log = { mode, gate: { should_run: shouldRun, reason: gateReason }, started_at: now.toISOString(), urls: [], errors: [] };

      function ensureSeenFile() {
        if (!fs.existsSync(seenPath)) fs.writeFileSync(seenPath, JSON.stringify({ items: [] }, null, 2) + String.fromCharCode(10));
      }

      function hashText(value) {
        return crypto.createHash('sha256').update(String(value || '').trim().toLowerCase()).digest('hex');
      }

      function decodeXml(value) {
        return String(value || '').replaceAll('&amp;', '&').replaceAll('&lt;', '<').replaceAll('&gt;', '>').replaceAll('&quot;', '"').replaceAll('&#39;', "'");
      }

      function stripTags(value) {
        return decodeXml(value).replaceAll('<![CDATA[', '').replaceAll(']]>', '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
      }

      function hasAny(text, terms) {
        const lower = String(text || '').toLowerCase();
        return terms.some((term) => lower.includes(term.toLowerCase()));
      }

      function locationEvidenceText(candidate) {
        const fields = [candidate.title, candidate.description, candidate.source_category, candidate.article_text_excerpt];
        if (!candidate.ignore_url_location) fields.push(candidate.url);
        return fields.join(' ');
      }

      function hasLocation(candidate) {
        return hasAny(locationEvidenceText(candidate), locationTerms);
      }

      function canonicalUrl(value) {
        try {
          const url = new URL(value);
          for (const key of [...url.searchParams.keys()]) {
            if (key.startsWith('utm_') || ['fbclid', 'gclid', 'mc_cid', 'mc_eid', 'cmpid', 'storylink'].includes(key)) url.searchParams.delete(key);
          }
          url.hash = '';
          return url.toString();
        } catch {
          return value || null;
        }
      }

      function scoreCandidate(candidate) {
        const text = [candidate.title, candidate.description, candidate.article_text_excerpt].join(' ').toLowerCase();
        if (text.includes('third place') || text.includes('third space')) return 100;
        if (hasAny(text, ['grand opening', 'opening soon', 'now open', 'new location', 'debut', 'reopens'])) return 90;
        if (hasAny(text, ['closes', 'closing', 'rebrand', 'renovation', 'expansion'])) return 80;
        if (candidate.source_kind === 'source-page') return 70;
        if (candidate.source_kind === 'google-news-rss' && hasAny(candidate.title, subjectTerms.concat(actionTerms))) return 60;
        return 40;
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

      function isSeen(candidate, seenItems) {
        return seenItems.some((item) => item.url === candidate.url || item.title_hash === candidate.title_hash);
      }

      async function fetchText(url) {
        log.urls.push(url);
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 30000);
        try {
          const response = await fetch(url, { headers: { 'User-Agent': userAgent }, signal: controller.signal });
          const text = await response.text();
          clearTimeout(timeout);
          if (!response.ok) return { ok: false, status: response.status, body: text.slice(0, 500) };
          return { ok: true, status: response.status, body: text };
        } catch (error) {
          clearTimeout(timeout);
          return { ok: false, status: 'network', body: String(error.message || error) };
        }
      }

      function parseFeedItems(xml, source, sourceUrl, sourceKind) {
        const items = [];
        const rssItems = xml.split('<item>').slice(1).map((part) => part.split('</item>')[0] || '');
        const atomItems = xml.split('<entry>').slice(1).map((part) => part.split('</entry>')[0] || '');
        for (const item of rssItems.slice(0, 100)) {
          const title = stripTags(tagValue(item, 'title'));
          const link = canonicalUrl(stripTags(tagValue(item, 'link')));
          const description = stripTags(tagValue(item, 'description'));
          const pubDate = stripTags(tagValue(item, 'pubDate')) || null;
          items.push(makeCandidate(source, sourceUrl, sourceKind, title, link, description, pubDate, null, null));
        }
        for (const item of atomItems.slice(0, 100)) {
          const title = stripTags(tagValue(item, 'title'));
          const link = canonicalUrl(linkHref(item));
          const description = stripTags(tagValue(item, 'content') || tagValue(item, 'summary'));
          const pubDate = stripTags(tagValue(item, 'updated') || tagValue(item, 'published')) || null;
          items.push(makeCandidate(source, sourceUrl, sourceKind, title, link, description, pubDate, null, null));
        }
        return items.filter((item, index) => item.url && item.title && (item.published_at ? Date.parse(item.published_at) >= sinceMs : index < 30));
      }

      function tagValue(xml, tagName) {
        const open = xml.indexOf(`<${tagName}`);
        if (open < 0) return '';
        const contentStart = xml.indexOf('>', open) + 1;
        const close = xml.indexOf(`</${tagName}>`, contentStart);
        if (contentStart <= 0 || close < 0) return '';
        return xml.slice(contentStart, close);
      }

      function linkHref(entry) {
        const open = entry.indexOf('<link');
        if (open < 0) return '';
        const end = entry.indexOf('>', open);
        const chunk = entry.slice(open, end);
        const key = 'href="';
        const start = chunk.indexOf(key);
        if (start < 0) return '';
        const valueStart = start + key.length;
        const valueEnd = chunk.indexOf('"', valueStart);
        return decodeXml(chunk.slice(valueStart, valueEnd));
      }

      function makeCandidate(source, sourceUrl, sourceKind, title, url, description, publishedAt, category, family, articleText = null) {
        const normalizedUrl = canonicalUrl(url);
        const candidate = {
          url: normalizedUrl,
          source: source.name,
          source_url: sourceUrl,
          title: title || null,
          description: description || null,
          published_at: publishedAt && !Number.isNaN(Date.parse(publishedAt)) ? new Date(Date.parse(publishedAt)).toISOString() : null,
          discovered_at: now.toISOString(),
          source_category: category || null,
          matching_keyword_family: family || null,
          location_evidence: [],
          source_kind: sourceKind,
          article_text_excerpt: articleText ? articleText.slice(0, 1000) : null,
          ignore_url_location: source.ignore_url_location === true
        };
        candidate.title_hash = hashText([candidate.source, candidate.title, candidate.published_at || ''].join('|'));
        candidate.relevance_score = scoreCandidate(candidate);
        candidate.location_evidence = locationTerms.filter((term) => hasAny(locationEvidenceText(candidate), [term]));
        delete candidate.ignore_url_location;
        return candidate;
      }

      function sameDomain(source, href) {
        try {
          const url = new URL(href);
          return url.hostname.replace(/^www\./, '').includes(source.site.split('/')[0].replace(/^www\./, ''));
        } catch {
          return false;
        }
      }

      function absolutize(base, href) {
        try { return new URL(href, base).toString(); } catch { return null; }
      }

      function parseLinks(html, source, sourceUrl) {
        const links = [];
        for (const match of html.matchAll(/<a\s+[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi)) {
          const href = absolutize(sourceUrl, decodeXml(match[1]));
          if (!href) continue;
          const lowerHref = href.toLowerCase();
          if (excludedHrefParts.some((part) => lowerHref.includes(part))) continue;
          if (!sameDomain(source, href)) continue;
          const title = stripTags(match[2]);
          if (!title || title.length < 8) continue;
          links.push({ url: canonicalUrl(href), title });
        }
        const byUrl = new Map();
        for (const link of links) if (!byUrl.has(link.url)) byUrl.set(link.url, link);
        return [...byUrl.values()].slice(0, 50);
      }

      async function main() {
        ensureSeenFile();
        if (shouldRun === 'false') {
          fs.writeFileSync(outPath, JSON.stringify({ mode, should_run: false, gate_reason: gateReason, generated_at: now.toISOString(), candidates: [] }, null, 2));
          fs.writeFileSync(logPath, JSON.stringify(log, null, 2));
          return;
        }
        if (mode === 'test') {
          fs.writeFileSync(outPath, JSON.stringify({
            mode,
            should_run: true,
            test: true,
            generated_at: now.toISOString(),
            candidates: [makeCandidate({ name: 'Test News', site: 'example.com' }, 'test', 'test', 'Test: New coffee shop opens as a third place in Charlotte', 'https://example.com/test-third-place', 'Synthetic test candidate for validating the Third Place News Alerts email path in Charlotte.', now.toISOString(), null, 'openings-food', 'Charlotte third place coffee shop')]
          }, null, 2));
          fs.writeFileSync(logPath, JSON.stringify(log, null, 2));
          return;
        }

        const seen = readSeen();
        const all = [];
        for (const source of sources) {
          for (const feed of source.feeds || []) {
            const response = await fetchText(feed);
            if (!response.ok) { log.errors.push({ source: source.name, url: feed, error: response }); continue; }
            all.push(...parseFeedItems(response.body, source, feed, 'feed'));
          }
          for (const sourceUrl of source.urls || []) {
            const response = await fetchText(sourceUrl);
            if (!response.ok) { log.errors.push({ source: source.name, url: sourceUrl, error: response }); continue; }
            const links = parseLinks(response.body, source, sourceUrl);
            for (const link of links) {
              if (!hasAny([link.title, link.url].join(' '), subjectTerms.concat(actionTerms))) continue;
              let articleText = null;
              const articleResponse = await fetchText(link.url);
              if (articleResponse.ok) articleText = stripTags(articleResponse.body).slice(0, 1000);
              all.push(makeCandidate(source, sourceUrl, 'source-page', link.title, link.url, null, null, null, 'source-page', articleText));
            }
          }
          for (const template of googleTemplates) {
            const query = template.template.replaceAll('{site}', source.site).replaceAll('{locations}', googleLocationQuery);
            const rssUrl = `https://news.google.com/rss/search?q=${encodeURIComponent(query)}&hl=en-US&gl=US&ceid=US:en`;
            const response = await fetchText(rssUrl);
            if (!response.ok) { log.errors.push({ source: source.name, url: rssUrl, error: response }); continue; }
            all.push(...parseFeedItems(response.body, source, rssUrl, 'google-news-rss').slice(0, 50).map((item) => ({ ...item, matching_keyword_family: template.family, relevance_score: scoreCandidate({ ...item, matching_keyword_family: template.family }) })));
          }
        }

        const byUrl = new Map();
        for (const item of all) {
          if (!item.url || !item.title) continue;
          if (!hasLocation(item)) continue;
          const key = item.url;
          if (!byUrl.has(key) || byUrl.get(key).relevance_score < item.relevance_score) byUrl.set(key, item);
        }
        const candidates = [...byUrl.values()]
          .filter((item) => !isSeen(item, seen))
          .sort((a, b) => (b.relevance_score - a.relevance_score) || (Date.parse(b.published_at || b.discovered_at) - Date.parse(a.published_at || a.discovered_at)))
          .slice(0, 250);
        fs.writeFileSync(outPath, JSON.stringify({ mode, should_run: true, test: false, generated_at: now.toISOString(), candidates }, null, 2));
        fs.writeFileSync(logPath, JSON.stringify(log, null, 2));
      }

      main().catch((error) => {
        log.errors.push({ fatal: error.message, stack: error.stack });
        fs.writeFileSync(logPath, JSON.stringify(log, null, 2));
        fs.writeFileSync(outPath, JSON.stringify({ mode, should_run: true, test: false, generated_at: now.toISOString(), candidates: [], error: error.message }, null, 2));
      });
      NODE
---

## News Alerts

You monitor Charlotte local-news sources for Charlotte Third Places leads.

## Required Inputs

Read these files first:

- `/tmp/gh-aw/agent/news-candidates.json`
- `/tmp/gh-aw/agent/news-collection-log.json`
- `/tmp/gh-aw/repo-memory/third-place-alerts/news/seen.json`

## Gate And Test Rules

If `news-candidates.json` has `should_run: false`, do not analyze anything. You MUST call `noop` with the gate reason.

If `mode` is `test`, send exactly one test email using `send_email_report`, then call no other safe-output tool. Do not update repo memory in test mode.

## Relevance Rules

Relevant news candidates must satisfy all three requirements:

1. Has deterministic location evidence from the closed location list.
2. Has a named place, venue, business, institution, market, event series, neighborhood project, or public space.
3. Matches at least one relevant category below.

Relevant categories:

- Openings, opening soon, now open, new locations, reopens, expansions, closings, rebrands, reviews, spotlights, or features involving coffee shops, cafes, tea shops, bakeries, dessert shops, bookstores, libraries, breweries, taprooms, wine bars, bottle shops, markets, coworking spaces, community centers, art spaces, museums, makerspaces, game stores, or arcades.
- Restaurant or bar items only when they include a third-place signal: coffee/cafe/tea/bakery/dessert, brewery/taproom/wine bar/bottle shop, patio/hangout/cozy/quiet language, events/live music/trivia/game nights/book clubs, laptop/Wi-Fi/study/work language, neighborhood gathering-place framing, or explicit `third place`/`third space` language.
- Event or community coverage only when it names a recurring or place-based gathering space, market, creative community, museum/gallery/studio, bookstore/library, brewery/taproom, community center, or public venue.
- Paywalled articles are allowed when title, source, URL, timestamp, location evidence, and category match are enough to understand the lead.

Not relevant:

- Generic restaurant news about a meal, menu item, chef, lawsuit, price, health score, ownership dispute, or corporate finance with no third-place signal.
- Non-Charlotte results from Google News RSS, even if the source is local.
- Crime, weather, sports, traffic, politics, or general civic news unless a relevant named third place is central to the item.
- Sponsored ads, paid directory listings, coupon/deal pages, job postings, classifieds, newsletters, and event calendar entries with no physical place or gathering-space relevance.

## Email Requirements

If relevant new items exist, call `send_email_report` with:

- `subject`: `Third Place News Alerts - YYYY-MM-DD`
- `text_body`: a plain text report with the same items.

Formatting rules:

- Do not provide `html_body`; the shared email job generates HTML from `text_body`.
- `text_body` must be readable plain text. Do not include HTML tags, `<!doctype html>`, `(!doctype html)`, pseudo-tags like `(div ...)`, or Markdown table formatting.
- Simple Markdown headings, bullets, bold, italic, and links are allowed in `text_body` because the shared email job renders them deterministically.

Include at most 20 items. Rank items in this order:

1. Exact `third place` / `third space` matches.
2. Openings, now-open items, reopens, and new locations.
3. Closings, rebrands, expansions, and renovations.
4. Requests for recommendations, unmet-demand posts, and recommendation comments.
5. Reviews, spotlights, features, recurring events, and community/creative-scene leads.
6. Everything else that still passed relevance.

Each item must include title, source/permalink, why it matters, and matched evidence.

## Repo Memory Update

After calling `send_email_report` in real mode, update `/tmp/gh-aw/repo-memory/third-place-alerts/news/seen.json` with only the relevant notified items. Keep this shape:

```json
{
  "items": [
    {
      "url": "https://example.com/story",
      "source": "Example Source",
      "title": "Example",
      "title_hash": "sha256",
      "first_seen_at": "2026-07-07T00:00:00.000Z",
      "last_notified_at": "2026-07-07T00:00:00.000Z",
      "relevance_category": "openings-food"
    }
  ]
}
```

Prune entries older than 90 days based on `last_notified_at` when present, otherwise `first_seen_at`. Keep at most 10,000 entries.

If there are no relevant new items, call `noop` with a short reason and do not update repo memory.
