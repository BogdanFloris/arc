import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { expect, test, type Page, type WebSocketRoute } from '@playwright/test';
import { Role, SessionRole, Source } from '../../src/lib/arc/gen/events_pb';
import {
  ClientFrameSchema, ServerFrameSchema, SessionInfoSchema, SessionListSchema,
  JobListSchema, SessionHistorySchema, HistoryEntrySchema, HistoryMessageSchema,
  MessageAcceptedSchema, DeltaSchema, StreamEndSchema, ProjectListSchema, ModelListSchema, ModelChoiceSchema, SessionStatusSchema,
} from '../../src/lib/arc/gen/wire_pb';

const userText = '**Keep my input literal**\nline two';
const markdown = [
  '# Readable Markdown',
  '',
  'A **strong** point, some *emphasis*, ~~old text~~, and `inline code`.',
  'A second line.',
  '',
  '## Next steps',
  '',
  '- First item',
  '  - Nested item',
  '- Second item',
  '',
  '3. Third step',
  '4. Fourth step',
  '',
  '> A quieter quote.',
  '',
  '[Reference](https://example.com/reference)',
  '',
  '| Column | Value |',
  '| --- | ---: |',
  '| A | 42 |',
  `| ${'X'.repeat(300)} | Preserved |`,
  '',
  '```rust',
  '// A useful comment',
  'fn main() {',
  '    let answer = 42;',
  '    println!("hello");',
  `    // ${'preserve columns '.repeat(35)}`,
  '}',
  '```',
  '',
  '```unknown',
  '<script>window.arcInjected = true</script>',
  '```',
  '',
  '---',
  '',
  'Final paragraph.',
].join('\n');

function entry(seq: bigint, role: Role, content: string, source = role === Role.USER ? Source.USER : Source.MODEL) {
  return create(HistoryEntrySchema, { seq, entry: { case: 'message', value: create(HistoryMessageSchema, { role, source, content }) } });
}

async function setup(page: Page, content: string) {
  let entries = [entry(1n, Role.USER, userText), entry(2n, Role.ASSISTANT, content)];
  let liveReply: ((msg: Parameters<typeof create<typeof ServerFrameSchema>>[1]['msg']) => void) | undefined;
  await page.addInitScript(() => localStorage.setItem('arc-web.local.v1', JSON.stringify({
    hosts: [{ id: 'markdown-test', name: 'Markdown daemon', endpoint: 'ws://127.0.0.1:8787/markdown', kind: 'daemon' }],
    activeHostId: 'markdown-test', selected: { 'markdown-test': 'markdown-chat' }, drafts: {}, inputs: {},
  })));
  await page.routeWebSocket('ws://127.0.0.1:8787/markdown', (route: WebSocketRoute) => route.onMessage((data) => {
    const request = fromBinary(ClientFrameSchema, new Uint8Array(data as Buffer));
    const reply = (msg: Parameters<typeof create<typeof ServerFrameSchema>>[1]['msg']) =>
      route.send(Buffer.from(toBinary(ServerFrameSchema, create(ServerFrameSchema, { requestId: request.requestId, msg }))));
    switch (request.msg.case) {
      case 'listSessions':
        reply({ case: 'sessionList', value: create(SessionListSchema, { sessions: [
          create(SessionInfoSchema, { id: 'markdown-chat', title: 'Markdown fixture', source: Source.USER }),
        ] }) });
        break;
      case 'listJobs': reply({ case: 'jobList', value: create(JobListSchema) }); break;
      case 'listProjects': reply({ case: 'projectList', value: create(ProjectListSchema) }); break;
      case 'listModels': reply({ case: 'modelList', value: create(ModelListSchema, { choices: [
        create(ModelChoiceSchema, { role: SessionRole.CHAT, name: 'default', provider: 'openai', model: 'gpt-test', selected: true }),
      ] }) }); break;
      case 'fetchStatus':
      case 'setSessionThinking':
        reply({ case: 'sessionStatus', value: create(SessionStatusSchema, {
          sessionId: request.msg.value.sessionId, effectiveThinking: 'low', supportedThinking: ['low', 'medium', 'high'],
        }) });
        break;
      case 'fetchHistory':
        reply({ case: 'sessionHistory', value: create(SessionHistorySchema, { sessionId: 'markdown-chat', entries }) });
        break;
      case 'sendMessage':
        entries = [...entries, entry(3n, Role.USER, request.msg.value.content)];
        liveReply = reply;
        reply({ case: 'messageAccepted', value: create(MessageAcceptedSchema, { sessionId: 'markdown-chat' }) });
        break;
    }
  }));
  await page.goto('/');
  await expect(page.locator('.status')).toContainText('Connected');
  await expect(page.locator('.message')).toHaveCount(2);
  return {
    delta(text: string) {
      liveReply!({ case: 'delta', value: create(DeltaSchema, { sessionId: 'markdown-chat', text }) });
    },
    finish(content: string) {
      entries = [...entries, entry(4n, Role.ASSISTANT, content)];
      liveReply!({ case: 'streamEnd', value: create(StreamEndSchema, { sessionId: 'markdown-chat' }) });
    },
  };
}

for (const width of [320, 1100]) {
  test(`Markdown has TUI colours and contained code/tables at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 844 });
    await setup(page, markdown);
    const body = page.locator('.message .markdown');
    await expect(body.getByRole('heading', { name: 'Readable Markdown', level: 1 })).toBeVisible();
    await expect(body.locator('strong')).toHaveText('strong');
    await expect(body.locator('em')).toHaveText('emphasis');
    await expect(body.locator('s')).toHaveText('old text');
    await expect(body.locator('ul ul li')).toHaveText('Nested item');
    await expect(body.locator('ol')).toHaveAttribute('start', '3');
    await expect(body.locator('blockquote')).toContainText('A quieter quote.');
    await expect(body.getByRole('link', { name: 'Reference' })).toHaveAttribute('href', 'https://example.com/reference');
    await expect(body.locator('pre')).toHaveCount(2);
    await expect(body.locator('pre').first()).toContainText('    let answer = 42;');
    await expect(body.locator('.hljs-keyword').first()).toHaveText('fn');
    await expect(body.locator('.hljs-number')).toHaveText('42');
    await expect(body.locator('pre').last()).toContainText('<script>window.arcInjected = true</script>');
    await expect(body.locator('hr')).toHaveCount(1);
    await expect(page.locator('.from-user')).toContainText(userText);
    await expect(page.locator('.from-user strong, .from-user .markdown')).toHaveCount(0);

    const styles = await body.evaluate((element) => {
      const colour = (selector: string) => getComputedStyle(element.querySelector(selector)!).color;
      return {
        heading: colour('h1'), inlineCode: colour('p code'), keyword: colour('.hljs-keyword'),
        string: colour('.hljs-string'), number: colour('.hljs-number'),
        comment: colour('.hljs-comment'), quote: colour('blockquote'),
        codeWhitespace: getComputedStyle(element.querySelector('pre')!).whiteSpace,
        codeOverflows: element.querySelector('pre')!.scrollWidth > element.querySelector('pre')!.clientWidth,
        tableOverflows: element.querySelector('.markdown-table')!.scrollWidth > element.querySelector('.markdown-table')!.clientWidth,
        pageOverflows: document.documentElement.scrollWidth > innerWidth,
      };
    });
    expect(styles).toEqual({
      heading: 'rgb(254, 128, 25)', inlineCode: 'rgb(142, 192, 124)',
      keyword: 'rgb(251, 73, 52)', string: 'rgb(184, 187, 38)', number: 'rgb(211, 134, 155)',
      comment: 'rgb(146, 131, 116)', quote: 'rgb(189, 174, 147)', codeWhitespace: 'pre',
      codeOverflows: true, tableOverflows: true, pageOverflows: false,
    });
    const code = body.locator('pre').first();
    await code.focus();
    await page.keyboard.press('ArrowRight');
    await expect.poll(() => code.evaluate((element) => element.scrollLeft)).toBeGreaterThan(0);
    const table = body.getByRole('region', { name: 'Table', exact: true });
    await table.focus();
    await page.keyboard.press('ArrowRight');
    await expect.poll(() => table.evaluate((element) => element.scrollLeft)).toBeGreaterThan(0);
  });
}

test('untrusted Markdown cannot execute HTML or load images', async ({ page }) => {
  let externalRequests = 0;
  await page.route('https://images.example/**', (route) => { externalRequests++; return route.abort(); });
  const payload = [
    '<img src="https://images.example/raw" onerror="window.arcInjected=true">',
    '<script>window.arcInjected=true</script>',
    '<iframe srcdoc="<script>window.arcInjected=true</script>"></iframe>',
    '[script](javascript:alert(1))',
    '[encoded](jav&#x61;script:alert(1))',
    '[file](file:///etc/passwd)',
    '[data](data:text/html;base64,PHNjcmlwdD4=)',
    '![Picture](https://images.example/markdown "title\\" onerror=\\"window.arcInjected=true")',
    '[safe](https://example.com "title\\" onclick=\\"window.arcInjected=true")',
    '',
    '```html',
    '<svg onload="window.arcInjected=true"></svg>',
    '```',
  ].join('\n');
  await setup(page, payload);
  const body = page.locator('.markdown');
  await expect(body).toContainText('<img src=');
  await expect(body).toContainText('<script>window.arcInjected=true</script>');
  await expect(body.locator('img, script, iframe, svg, style')).toHaveCount(0);
  await expect(body.getByRole('link', { name: 'safe', exact: true })).toHaveAttribute('href', 'https://example.com');
  await expect(body.locator('a[href^="javascript:"], a[href^="data:"], a[href^="file:"]')).toHaveCount(0);
  await expect(body.locator('[onerror], [onclick], [onload]')).toHaveCount(0);
  expect(await page.evaluate(() => Reflect.get(window, 'arcInjected'))).toBeUndefined();
  expect(externalRequests).toBe(0);
});

test('streamed Markdown updates preserve reader position and finish immediately', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const server = await setup(page, `${markdown}\n\n${'Older paragraph.\n\n'.repeat(30)}`);
  const transcript = page.locator('.transcript');
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await composer.fill('Stream some code');
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  const prefix = '## Live answer\n\n```rust\nfn main() {\n';
  server.delta(prefix);
  const live = page.locator('.message').last().locator('.markdown');
  await expect(live.getByRole('heading', { name: 'Live answer' })).toBeVisible();
  await expect(live.locator('pre')).toContainText('fn main() {');
  await expect.poll(() => transcript.evaluate((element) => element.scrollHeight - element.scrollTop - element.clientHeight)).toBeLessThan(48);
  await transcript.evaluate((element) => { element.scrollTop = 0; });
  await expect(page.getByRole('button', { name: 'Jump to latest' })).toBeVisible();
  await composer.fill('Next draft');
  const initialHeading = await page.locator('.message').nth(1).getByRole('heading', { name: 'Readable Markdown' }).elementHandle();
  const suffix = '    println!("done");\n}\n```\n\n**Finished**.';
  server.delta(suffix.slice(0, 20));
  server.delta(suffix.slice(20));
  await expect(live.locator('strong')).toHaveText('Finished');
  expect(await transcript.evaluate((element) => element.scrollTop)).toBe(0);
  expect(await initialHeading!.evaluate((element) => element.isConnected)).toBe(true);
  await expect(composer).toHaveValue('Next draft');
  server.finish(prefix + suffix);
  await expect(page.locator('.meta').filter({ hasText: 'responding' })).toHaveCount(0);
  await expect(page.locator('.message').last().locator('strong')).toHaveText('Finished');
  expect(await transcript.evaluate((element) => element.scrollTop)).toBe(0);
  await page.getByRole('button', { name: 'Jump to latest' }).click();
  await expect.poll(() => transcript.evaluate((element) => element.scrollHeight - element.scrollTop - element.clientHeight)).toBeLessThan(48);
});
