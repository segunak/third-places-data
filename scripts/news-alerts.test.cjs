const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const root = path.join(__dirname, '..');
const workflow = fs.readFileSync(path.join(root, '.github', 'workflows', 'news-alerts.md'), 'utf8');
const collector = [...workflow.matchAll(/^      node <<'NODE'\r?\n([\s\S]*?)^      NODE\s*$/gm)]
  .at(-1)[1].replace(/^      /gm, '');
const redditWorkflow = fs.readFileSync(path.join(root, '.github', 'workflows', 'reddit-alerts.md'), 'utf8');
const redditScripts = [...redditWorkflow.matchAll(/^      node <<'NODE'\r?\n([\s\S]*?)^      NODE\s*$/gm)]
  .map((match) => match[1].replace(/^      /gm, ''));
const redditCollector = redditScripts.at(-1);
const emailWorkflow = fs.readFileSync(path.join(root, '.github', 'workflows', 'shared', 'email-report.md'), 'utf8');
const emailScript = emailWorkflow.match(/^            script: \|\r?\n([\s\S]*?)(?=^        - name: Send Email)/m)[1]
  .replace(/^              /gm, '');
const source = { name: 'Test Publisher', site: 'publisher.example', feeds: ['https://publisher.example/feed'], urls: [] };
const seenPath = '/tmp/gh-aw/repo-memory/third-place-alerts/news/seen.json';

function memoryFs(initial) {
  const files = new Map(Object.entries(initial));
  return {
    files,
    existsSync: (file) => files.has(file),
    readFileSync: (file) => {
      assert.ok(files.has(file), `Unexpected file read: ${file}`);
      return files.get(file);
    },
    writeFileSync: (file, value) => files.set(file, value),
    mkdirSync: () => {}
  };
}

function response(body, status = 200) {
  return { ok: status >= 200 && status < 300, status, headers: { get: () => null }, text: async () => body };
}

function feed(items) {
  return `<rss><channel>${items.map((item) => `<item><title>${item.title || 'New cafe opens in Charlotte'}</title><link>${item.url.replaceAll('&', '&amp;')}</link><pubDate>${item.date || new Date().toISOString()}</pubDate></item>`).join('')}</channel></rss>`;
}

async function collect(items, { seen = [], resolve, page = '<div data-n-a-sg="signature" data-n-a-ts="1234567890">', rpcStatus = 200, rpcBody } = {}) {
  const config = {
    locationTerms: ['Charlotte'],
    googleLocationQuery: '(Charlotte)',
    subjectTerms: ['cafe'],
    actionTerms: ['opening'],
    excludedHrefParts: [],
    sources: [source],
    googleTemplates: []
  };
  const disk = memoryFs({
    '/tmp/gh-aw/agent/news-alert-config.json': JSON.stringify(config),
    [seenPath]: JSON.stringify({ items: seen })
  });
  const requests = [];
  const warnings = [];
  const context = vm.createContext({
    require: (name) => {
      if (name === 'fs') return disk;
      if (name === 'crypto') return crypto;
      throw new Error(`Unexpected module: ${name}`);
    },
    process: { env: { MODE: 'real' } },
    URL,
    URLSearchParams,
    AbortController,
    setTimeout,
    clearTimeout,
    console: { warn: (message) => warnings.push(message) },
    fetch: async (url, options) => {
      requests.push({ url, options });
      if (url === source.feeds[0]) return response(feed(items));
      if (url.startsWith('https://news.google.com/rss/articles/')) return response(page);
      assert.equal(url, 'https://news.google.com/_/DotsSplashUi/data/batchexecute');
      assert.equal(options.method, 'POST');
      const envelopes = JSON.parse(new URLSearchParams(options.body).get('f.req'))[0];
      const rows = envelopes.map((envelope) => {
        const request = JSON.parse(envelope[1]);
        assert.equal(request[0], 'garturlreq');
        return ['wrb.fr', 'Fbv4je', JSON.stringify(['garturlres', resolve?.(request[2])]), null, null, null, envelope[3]];
      }).reverse();
      return response(rpcBody ?? `)]}'\n\n${rows.map((row) => JSON.stringify([row])).join('\n')}\n`, rpcStatus);
    }
  });
  await new vm.Script(collector).runInContext(context, { timeout: 5000 });
  const output = JSON.parse(disk.files.get('/tmp/gh-aw/agent/news-candidates.json'));
  const log = JSON.parse(disk.files.get('/tmp/gh-aw/agent/news-collection-log.json'));
  assert.equal(output.error, undefined);
  assert.deepEqual(JSON.parse(disk.files.get(seenPath)), { items: seen });
  return { ...output, log, requests, warnings };
}

function redditApi(reply = () => { throw new Error('Unexpected Reddit request'); }) {
  const disk = memoryFs({ '/tmp/gh-aw/repo-memory/third-place-alerts/reddit/seen.json': '{"items":[]}' });
  new vm.Script(redditScripts[0]).runInNewContext({
    require: (name) => {
      assert.equal(name, 'fs');
      return disk;
    }
  }, { timeout: 5000 });
  const requests = [];
  const context = vm.createContext({
    require: (name) => {
      if (name === 'fs') return disk;
      if (name === 'crypto') return crypto;
      throw new Error(`Unexpected module: ${name}`);
    },
    process: { env: { MODE: 'real' } },
    URL,
    URLSearchParams,
    AbortController,
    setTimeout: (callback, delay) => {
      if (delay !== 30000) callback();
      return 0;
    },
    clearTimeout: () => {},
    console: { warn: () => {} },
    fetch: async (value) => {
      const url = new URL(value);
      assert.equal(url.hostname, 'www.reddit.com');
      requests.push(url);
      return reply(url);
    }
  });
  const definitions = redditCollector.slice(0, redditCollector.lastIndexOf('main().catch'));
  const api = new vm.Script(`${definitions}\n({ findBrandMentions, normalizePost, normalizeComment, isSeen, collectBrandMentions, main, log })`)
    .runInContext(context, { timeout: 5000 });
  return { api, disk, requests };
}

async function render(textBody, { news = true, output = true, items } = {}) {
  const disk = memoryFs(output ? {
    'agent-output.json': JSON.stringify({
      items: items ?? [{ type: 'send_email_report', subject: 'Third Place News Alerts', text_body: textBody }]
    })
  } : {});
  const outputs = {};
  const failures = [];
  const context = vm.createContext({
    require: (name) => {
      if (name === 'fs') return disk;
      if (name === 'path') return path;
      throw new Error(`Unexpected module: ${name}`);
    },
    process: { env: {
      GH_AW_AGENT_OUTPUT: 'agent-output.json',
      GITHUB_WORKFLOW_REF: `owner/repo/.github/workflows/${news ? 'news' : 'reddit'}-alerts.lock.yml@refs/heads/master`,
      RUNNER_TEMP: 'test-temp'
    } },
    URL,
    core: {
      setFailed: (message) => failures.push(message),
      setOutput: (name, value) => { outputs[name] = value; },
      info: () => {},
      warning: () => {}
    }
  });
  await new vm.Script(`(async () => {\n${emailScript}\n})()`).runInContext(context, { timeout: 5000 });
  return { outputs, failures, files: disk.files };
}

test('resolves a Google News batch by request ID and removes publisher tracking', async () => {
  const date = new Date().toISOString();
  const result = await collect([
    { title: 'Charlotte cafe opens', date, url: 'https://news.google.com/rss/articles/article_one' },
    { title: 'Charlotte library opens', date, url: 'https://news.google.com/read/article_two' }
  ], {
    resolve: (id) => `https://publisher.example/stories/${id}?utm_source=google#section`
  });
  assert.deepEqual(result.candidates.map((item) => [item.title, item.url]), [
    ['Charlotte cafe opens', 'https://publisher.example/stories/article_one'],
    ['Charlotte library opens', 'https://publisher.example/stories/article_two']
  ]);
  assert.equal(result.requests.filter((item) => item.options.method === 'POST').length, 1);
  assert.equal(result.log.errors.length, 0);
});

test('unwraps Google URL parameters and deduplicates against decoded publisher links', async () => {
  const article = 'https://publisher.example/stories/new-cafe';
  const wrapper = `https://www.google.com/url?q=${encodeURIComponent(article)}`;
  const result = await collect([
    { url: `https://www.google.com/url?url=${encodeURIComponent(wrapper)}` },
    { url: 'https://news.google.com/rss/articles/article_one' },
    { url: `${article}?utm_source=newsletter` }
  ], { resolve: () => article });
  assert.equal(result.candidates.length, 1);
  assert.equal(result.candidates[0].url, article);
});

test('omits unresolved and foreign destinations with explicit warnings', async (t) => {
  for (const [name, options] of [
    ['missing decoding fields', { page: '<html></html>' }],
    ['rate limiting', { rpcStatus: 429 }],
    ['malformed RPC response', { rpcBody: 'not JSON' }],
    ['missing RPC result', {}],
    ['another publisher', { resolve: () => 'https://other.example/story' }],
    ['publisher name in another host', { resolve: () => 'https://publisher.example.other.example/story' }],
    ['invalid protocol', { resolve: () => 'javascript:alert(1)' }],
    ['publisher homepage', { resolve: () => 'https://publisher.example/' }],
    ['Google result', { resolve: () => 'https://news.google.com/rss/articles/article_two' }]
  ]) {
    await t.test(name, async () => {
      const result = await collect([{ url: 'https://news.google.com/rss/articles/article_one' }], options);
      assert.deepEqual(result.candidates, []);
      assert.equal(result.log.errors.length, 1);
      assert.equal(result.log.errors[0].stage, 'resolve-publisher-url');
      assert.equal(result.warnings.length, 1);
    });
  }
});

test('preserves legacy title hashes and checks resolved URLs against notification history', async () => {
  const date = new Date().toISOString();
  const title = 'Charlotte cafe opens';
  const article = 'https://publisher.example/stories/new-cafe';
  const titleHash = crypto.createHash('sha256')
    .update([source.name, title, date].join('|').trim().toLowerCase()).digest('hex');
  const legacy = await collect([{ title, date, url: 'https://news.google.com/rss/articles/article_one' }], {
    seen: [{ url: 'https://news.google.com/rss/articles/old_article', title_hash: titleHash }]
  });
  assert.deepEqual(legacy.candidates, []);
  assert.equal(legacy.requests.length, 1);
  const publisherSeen = await collect([{ title, date, url: 'https://news.google.com/rss/articles/article_one' }], {
    seen: [{ url: article, title_hash: 'another-title-hash' }],
    resolve: () => article
  });
  assert.deepEqual(publisherSeen.candidates, []);
});

test('news email rejects Google URLs before any email files are written', async (t) => {
  for (const body of [
    'Article: https://news.google.com/rss/articles/article_one?oc=5',
    '[Read article](https://www.google.com/url?q=https%3A%2F%2Fpublisher.example%2Fstory)',
    '[Read article](HTTPS://NEWS.GOOGLE.COM./read/article_one)',
    'Article: https://www.google.co.uk/url?q=https%3A%2F%2Fpublisher.example%2Fstory',
    'Article: news.google.com/rss/articles/article_one',
    'Article: https://%67oogle.com/url?q=https%3A%2F%2Fpublisher.example%2Fstory'
  ]) {
    await t.test(body, async () => {
      const result = await render(body);
      assert.equal(result.failures.length, 1);
      assert.match(result.failures[0], /Google URL/);
      assert.equal(result.outputs.send, undefined);
      assert.equal(result.files.size, 1);
    });
  }
});

test('publisher links render in both email parts and Reddit behavior is unchanged', async () => {
  const article = 'https://publisher.example/stories/new-cafe';
  const news = await render(`[Read article](${article})`);
  assert.deepEqual(news.failures, []);
  assert.equal(news.outputs.send, 'true');
  const html = [...news.files.entries()].find(([file]) => file.endsWith('body.html'))[1];
  assert.ok(html.includes(`href="${article}"`));
  const text = [...news.files.entries()].find(([file]) => file.endsWith('body.txt'))[1];
  assert.equal(text, `[Read article](${article})`);
  const reddit = await render('https://news.google.com/read/article_one', { news: false });
  assert.deepEqual(reddit.failures, []);
  assert.equal(reddit.outputs.send, 'true');
});

test('Reddit mention matching checks exact names, website hosts, and app identities', () => {
  const { api } = redditApi();
  for (const [value, type] of [
    ['Have you tried CHARLOTTE **THIRD PLACES**?', 'name'],
    ['https://www.charlottethirdplaces.com/places/one?source=reddit', 'website'],
    ['www.charlottethirdplaces.com/collections/reading', 'website'],
    ['charlottethirdplaces.com.', 'website'],
    ['https://apps.apple.com/gb/app/third-places/id6762573563?platform=iphone', 'ios-app'],
    ['https://play.google.com/store/apps/details?hl=en&amp;id=com.charlottethirdplaces.app', 'android-app']
  ]) {
    assert.deepEqual(Array.from(api.findBrandMentions(value), (match) => match.type), [type], value);
  }
  for (const value of [
    'https://notcharlottethirdplaces.com/',
    'https://charlottethirdplaces.com.other.example/story',
    'charlottethirdplaces.com.other.example',
    'https://apps.apple.com/us/app/other/id67625735630',
    'https://play.google.com/store/apps/details?id=com.charlottethirdplaces.app.debug',
    'https://other.example/store/apps/details?id=com.charlottethirdplaces.app'
  ]) {
    assert.equal(api.findBrandMentions(value).length, 0, value);
  }
});

test('Reddit collects global link posts and local comments ahead of ordinary leads', async () => {
  const created = Math.floor(Date.now() / 1000);
  const targets = [
    { title: 'Useful resource', url_overridden_by_dest: 'https://www.charlottethirdplaces.com/places/one' },
    { title: 'Try this app', url: 'https://apps.apple.com/us/app/third-places/id6762573563' },
    { title: 'An Android option', url: 'https://play.google.com/store/apps/details?id=com.charlottethirdplaces.app' },
    { title: 'Charlotte Third Places feedback', selftext: 'The latest change did not work for me.' }
  ].map((data, index) => ({ kind: 't3', data: {
    ...data, id: `mention${index}`, name: `t3_mention${index}`,
    permalink: `/r/webdev/comments/mention${index}/example/`, created_utc: created - 3 * 86400
  } }));
  const leads = Array.from({ length: 205 }, (_value, index) => ({ kind: 't3', data: {
    id: `lead${index}`, name: `t3_lead${index}`, title: `Ordinary post ${index}`,
    permalink: `/r/Charlotte/comments/lead${index}/example/`, created_utc: created
  } }));
  const { api, disk, requests } = redditApi((url) => {
    if (url.pathname === '/search.json') {
      assert.equal(url.searchParams.get('restrict_sr'), null);
      assert.equal(url.searchParams.get('t'), 'week');
      return response(JSON.stringify({ data: { children: targets, after: null } }));
    }
    if (url.pathname === '/r/Charlotte/comments.json') {
      return response(JSON.stringify({ data: { after: null, children: [
        { kind: 't1', data: {
          id: 'localmention', name: 't1_localmention', link_id: 't3_oldthread',
          link_title: 'An unrelated old thread', body: 'https://charlottethirdplaces.com/map',
          permalink: '/r/Charlotte/comments/oldthread/example/localmention/', created_utc: created - 3 * 86400
        } },
        { kind: 't1', data: {
          id: 'thanks', name: 't1_thanks', link_id: 't3_brandthread',
          link_title: 'Charlotte Third Places', body: 'Thank you!',
          permalink: '/r/Charlotte/comments/brandthread/example/thanks/', created_utc: created
        } }
      ] } }));
    }
    if (url.pathname === '/r/Charlotte/new.json') {
      const start = Number(url.searchParams.get('after') || 0);
      const children = leads.slice(start, start + 100);
      return response(JSON.stringify({ data: { children, after: start + 100 < leads.length ? String(start + 100) : null } }));
    }
    assert.equal(url.pathname, '/r/Charlotte/search.json');
    return response('{"data":{"children":[],"after":null}}');
  });
  await api.main();
  const output = JSON.parse(disk.files.get('/tmp/gh-aw/agent/reddit-candidates.json'));
  assert.equal(output.error, undefined);
  assert.equal(output.candidates.length, 200);
  assert.deepEqual(output.candidates.slice(0, 5).map((item) => item.id).sort(), [
    't1_localmention', 't3_mention0', 't3_mention1', 't3_mention2', 't3_mention3'
  ]);
  assert.ok(output.candidates.slice(0, 5).every((item) => item.mention_matches.length > 0));
  assert.equal(output.candidates.some((item) => item.id === 't1_thanks'), false);
  assert.equal(output.candidates.find((item) => item.id === 't3_mention0').outbound_url, targets[0].data.url_overridden_by_dest);
  assert.ok(requests.some((url) => url.pathname === '/r/Charlotte/comments.json'));
});

test('Reddit mentions deduplicate by item identity rather than identical wording', () => {
  const { api } = redditApi();
  const post = (id, text) => api.normalizePost({ name: id, title: 'Useful resource', selftext: text, permalink: `/r/Charlotte/comments/${id}/example/` }, 'test', 'search-json', 'test');
  const first = post('t3_first', 'https://charlottethirdplaces.com/');
  const second = post('t3_second', 'https://charlottethirdplaces.com/');
  assert.equal(api.isSeen(second, [first]), false);
  assert.equal(api.isSeen(first, [first]), true);
  assert.equal(api.isSeen(post('t3_fourth', 'Ordinary text'), [post('t3_third', 'Ordinary text')]), true);
});

test('Reddit mention RSS fallbacks preserve URL evidence without matching a comment parent title', async () => {
  const updated = new Date(Date.now() - 3 * 86400000).toISOString();
  const entry = (title, text, id) => `<entry><title>${title}</title><link href="https://www.reddit.com/r/Charlotte/comments/example/thread/${id}/"/><updated>${updated}</updated><content type="html">${text}</content></entry>`;
  const { api } = redditApi((url) => {
    if (url.pathname.endsWith('.json')) return response('Denied', 403);
    if (url.pathname === '/search.rss') {
      return response(`<feed>${entry('Useful resource', '&lt;a href=&quot;https://charlottethirdplaces.com/map&quot;&gt;link&lt;/a&gt;', 'post')}</feed>`);
    }
    assert.equal(url.pathname, '/r/Charlotte/comments.rss');
    return response(`<feed>${entry('Charlotte Third Places', 'Thanks!', 'thanks')}${entry('An unrelated thread', 'https://play.google.com/store/apps/details?id=com.charlottethirdplaces.app', 'comment')}</feed>`);
  });
  const mentions = await api.collectBrandMentions();
  assert.equal(mentions.length, 2);
  assert.deepEqual(Array.from(mentions, (item) => item.mention_matches[0].type).sort(), ['android-app', 'website']);
  assert.equal(api.log.errors.length, 2);
});

test('Reddit mention emails keep the Google Play link', async () => {
  const url = 'https://play.google.com/store/apps/details?id=com.charlottethirdplaces.app';
  const result = await render(`[Android app](${url})`, { news: false });
  assert.deepEqual(result.failures, []);
  assert.equal(result.outputs.send, 'true');
  assert.ok([...result.files.values()].some((value) => value.includes(`href="${url}"`)));
});
