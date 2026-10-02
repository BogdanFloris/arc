import { create, fromBinary, toBinary } from '@bufbuild/protobuf';
import { TimestampSchema } from '@bufbuild/protobuf/wkt';
import { expect, test, type WebSocketRoute } from '@playwright/test';
import { BranchMarked_Disposition, Role, SessionRole, Source, ToolOutcome } from '../../src/lib/arc/gen/events_pb';
import {
  ClientFrameSchema, ServerFrameSchema, SessionInfoSchema, HistoryEntrySchema,
  HistoryMessageSchema, HistoryToolCallSchema, HistoryToolResultSchema,
  SessionHistorySchema, SessionListSchema, MessageAcceptedSchema, DeltaSchema,
  ToolCallStartedSchema, ToolCallEndedSchema, StreamEndSchema, JobListSchema, JobInfoSchema, JobInfo_State,
  type JobInfo, type SessionInfo,
} from '../../src/lib/arc/gen/wire_pb';

const direct = create(SessionInfoSchema, {
  id: 'chat-1', title: 'Protocol history', preview: 'Latest answer',
  source: Source.USER, role: SessionRole.CHAT,
});
const secondDirect = create(SessionInfoSchema, {
  id: 'chat-2', title: 'Another conversation', preview: 'A separate history',
  source: Source.USER, role: SessionRole.CHAT,
});
const job = create(SessionInfoSchema, {
  id: 'job-1', title: 'Model job', source: Source.MODEL,
  role: SessionRole.EXECUTOR, dispatchedBy: 'chat-1',
});
const durableHistory = create(SessionHistorySchema, {
  sessionId: 'chat-1',
  entries: [
    create(HistoryEntrySchema, { seq: 1n, entry: { case: 'message', value: create(HistoryMessageSchema, { role: Role.USER, source: Source.USER, content: 'Question' }) } }),
    create(HistoryEntrySchema, { seq: 2n, entry: { case: 'toolCall', value: create(HistoryToolCallSchema, { callId: 'c1', name: 'search', argumentsJson: '{"q":"topic"}' }) } }),
    create(HistoryEntrySchema, { seq: 3n, entry: { case: 'toolResult', value: create(HistoryToolResultSchema, { callId: 'c1', outcome: ToolOutcome.OK, content: 'Found it' }) } }),
    create(HistoryEntrySchema, { seq: 4n, entry: { case: 'message', value: create(HistoryMessageSchema, { role: Role.USER, source: Source.SYSTEM, content: 'Job 01234567-89ab-cdef-0123-456789abcdef finished.\n**Handed back** to the user' }) } }),
  ],
});

type Request = ReturnType<typeof fromBinary<typeof ClientFrameSchema>>;
type Daemon = {
  sent: Request[];
  histories: Map<string, ReturnType<typeof create<typeof SessionHistorySchema>>>;
  setHistory: (history: ReturnType<typeof create<typeof SessionHistorySchema>>) => void;
  attach: (route: WebSocketRoute, options?: { deny?: boolean; streamGate?: () => Promise<void> }) => void;
};

function daemon(sessions: SessionInfo[] = [direct, secondDirect, job], jobs: JobInfo[] = []): Daemon {
  const sent: Request[] = [];
  const histories = new Map([
    [durableHistory.sessionId, durableHistory],
    ['chat-2', create(SessionHistorySchema, {
      sessionId: 'chat-2',
      entries: [create(HistoryEntrySchema, { seq: 1n, entry: { case: 'message', value: create(HistoryMessageSchema, {
        role: Role.USER, source: Source.USER, content: 'Second conversation history',
      }) } })],
    })],
  ]);
  const sockets = new Set<WebSocketRoute>();
  const attach = (route: WebSocketRoute, options: { deny?: boolean; streamGate?: () => Promise<void> } = {}) => {
  sockets.add(route);
  route.onMessage((data) => {
    if (typeof data === 'string') throw new Error('Expected binary protobuf frame');
    const request = fromBinary(ClientFrameSchema, new Uint8Array(data as Buffer));
    sent.push(request);
    const id = request.requestId;
    const reply = (msg: Parameters<typeof create<typeof ServerFrameSchema>>[1]['msg']) => {
      const bytes = toBinary(ServerFrameSchema, create(ServerFrameSchema, { requestId: id, msg }));
      route.send(Buffer.from(bytes));
    };
    if (options.deny) return;
    switch (request.msg.case) {
      case 'subscribe': break;
      case 'listSessions': reply({ case: 'sessionList', value: create(SessionListSchema, { sessions }) }); break;
      case 'listJobs': reply({ case: 'jobList', value: create(JobListSchema, { jobs }) }); break;
      case 'fetchHistory': {
        const history = histories.get(request.msg.value.sessionId);
        if (history) reply({ case: 'sessionHistory', value: history });
        break;
      }
      case 'sendMessage': {
        const sessionId = request.msg.value.sessionId || 'chat-1';
        void (async () => {
          reply({ case: 'messageAccepted', value: create(MessageAcceptedSchema, { sessionId }) });
          reply({ case: 'delta', value: create(DeltaSchema, { sessionId, text: 'Streamed ' }) });
          reply({ case: 'toolCallStarted', value: create(ToolCallStartedSchema, { sessionId, callId: 'live-tool', name: 'lookup', argumentsJson: '{}' }) });
          reply({ case: 'toolCallEnded', value: create(ToolCallEndedSchema, { sessionId, callId: 'live-tool', outcome: ToolOutcome.OK, content: 'Tool output' }) });
          await options.streamGate?.();
          reply({ case: 'delta', value: create(DeltaSchema, { sessionId, text: 'answer' }) });
          histories.set(sessionId, create(SessionHistorySchema, {
            sessionId,
            entries: [
              ...durableHistory.entries,
              create(HistoryEntrySchema, { seq: 5n, entry: { case: 'message', value: create(HistoryMessageSchema, { role: Role.USER, source: Source.USER, content: request.msg.value.content }) } }),
              create(HistoryEntrySchema, { seq: 6n, entry: { case: 'message', value: create(HistoryMessageSchema, { role: Role.ASSISTANT, source: Source.MODEL, content: 'Streamed answer' }) } }),
            ],
          }));
          reply({ case: 'streamEnd', value: create(StreamEndSchema, { sessionId }) });
        })();
        break;
      }
    }
  });
  };
  return {
    sent, histories,
    setHistory: (history) => histories.set(history.sessionId, history),
    attach,
  };
}

async function addHost(page: import('@playwright/test').Page, name = 'Test daemon', endpoint = 'ws://127.0.0.1:8787') {
  await page.getByRole('button', { name: 'Settings', exact: true }).click();
  const settings = page.getByRole('dialog', { name: 'Settings', exact: true });
  await settings.getByRole('button', { name: 'Add daemon host' }).click();
  await settings.getByLabel('Name', { exact: true }).fill(name);
  await settings.getByLabel('Endpoint', { exact: true }).fill(endpoint);
  await settings.getByRole('button', { name: 'Add host', exact: true }).click();
  await expect(settings).toContainText(name);
  await expect(settings.getByRole('button', { name: 'Close', exact: true })).toBeVisible();
  await settings.getByRole('button', { name: 'Close', exact: true }).click();
}

test('binary daemon frames populate selectable conversation history and keep handbacks distinct', async ({ page }) => {
  const server = daemon();
  await page.routeWebSocket('ws://127.0.0.1:8787', (route) => server.attach(route));
  await page.setViewportSize({ width: 1100, height: 840 });
  await page.goto('/');
  await addHost(page);
  await expect(page.locator('.status')).toContainText('Connected');
  await expect(page.locator('.session-list')).toContainText('Protocol history');
  await expect(page.locator('.session-list')).not.toContainText('Model job');
  await expect(page.locator('.message')).toHaveCount(3);
  await expect(page.locator('.message').nth(1)).toContainText('search');
  await page.locator('.message').nth(1).locator('summary').click();
  await expect(page.locator('.message').nth(1)).toContainText('Found it');
  const handoff = page.locator('.handoff');
  await expect(handoff).not.toHaveAttribute('open');
  await expect(handoff.locator('.handoff-output')).not.toBeVisible();
  await expect(page.locator('.role').filter({ hasText: 'SYSTEM' })).toHaveCount(0);
  await handoff.locator('summary').click();
  await expect(handoff.locator('.handoff-output')).toContainText('Handed back to the user');
  await expect(handoff.locator('.handoff-output strong')).toHaveText('Handed back');
  await page.locator('.session-list').getByRole('button', { name: /Another conversation/ }).click();
  await expect(page.locator('.message')).toHaveCount(1);
  await expect(page.locator('.message').first()).toContainText('Second conversation history');
  expect(server.sent.some((frame) => frame.msg.case === 'listSessions')).toBe(true);
  expect(server.sent.some((frame) => frame.msg.case === 'fetchHistory' && frame.msg.value.sessionId === 'chat-1')).toBe(true);
});

test('send streams binary events then reconciles authoritative history without duplicates', async ({ page }) => {
  const server = daemon();
  let releaseStream!: () => void;
  const streamGate = new Promise<void>((resolve) => { releaseStream = resolve; });
  await page.routeWebSocket('ws://127.0.0.1:8787', (route) => server.attach(route, { streamGate: () => streamGate }));
  await page.setViewportSize({ width: 1100, height: 840 });
  await page.goto('/');
  await addHost(page);
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await composer.fill('Test one streamed turn');
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  await expect(page.locator('.message').filter({ hasText: 'Streamed' })).toHaveCount(1);
  const liveTool = page.locator('.tool').filter({ hasText: 'lookup' });
  await liveTool.locator('summary').click();
  await expect(liveTool.locator('.tool-output')).toContainText('Tool output');
  expect(server.sent.some((frame) => frame.msg.case === 'sendMessage')).toBe(true);
  releaseStream();
  await expect(page.locator('.message').filter({ hasText: 'Test one streamed turn' })).toHaveCount(1);
  await expect(page.locator('.message').filter({ hasText: 'Streamed answer' })).toHaveCount(1);
  await expect(page.locator('.message').filter({ hasText: 'Tool output' })).toHaveCount(0);
  await expect.poll(() => page.locator('.message').count()).toBe(5);
  expect(await page.locator('.message').allTextContents()).toEqual([
    expect.stringContaining('Question'),
    expect.stringContaining('search'),
    expect.stringContaining('Handed back to the user'),
    expect.stringContaining('Test one streamed turn'),
    expect.stringContaining('Streamed answer'),
  ]);
});

test('unreachable daemon retains its draft and disables send', async ({ page }) => {
  await page.routeWebSocket('ws://127.0.0.1:9999', (route) => daemon().attach(route, { deny: true }));
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await addHost(page, 'Offline daemon', 'ws://127.0.0.1:9999');
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await composer.fill('Keep while offline');
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeDisabled();
  await page.reload();
  await expect(composer).toHaveValue('Keep while offline');
});

test('switching daemon hosts closes the previous connection and isolates session data', async ({ page }) => {
  const first = daemon([
    create(SessionInfoSchema, { ...direct, project: 'arc' }),
    create(SessionInfoSchema, { ...secondDirect, project: 'arc-web' }),
    job,
  ]);
  const second = daemon();
  let firstSocketClosed = false;
  await page.routeWebSocket('ws://127.0.0.1:8787', (route) => {
    route.onClose(() => { firstSocketClosed = true; });
    first.attach(route);
  });
  await page.routeWebSocket('ws://127.0.0.1:9999', (route) => second.attach(route));
  await page.setViewportSize({ width: 1100, height: 840 });
  await page.goto('/');
  await addHost(page, 'First daemon');
  await expect(page.locator('.session-list')).toContainText('Protocol history');
  await page.locator('.sidebar').getByRole('combobox', { name: 'Project' }).selectOption('arc');
  await expect(page.locator('.session-list .session')).toHaveCount(1);
  await addHost(page, 'Second daemon', 'ws://127.0.0.1:9999');
  await expect(page.locator('.status')).toContainText('Second daemon');
  await expect(page.locator('.session-list')).toContainText('Protocol history');
  await expect(page.locator('.sidebar').getByRole('combobox', { name: 'Project' })).toHaveValue('');
  await expect(page.locator('.session-list .session')).toHaveCount(2);
  await expect.poll(() => firstSocketClosed).toBe(true);
  const firstCount = first.sent.length;
  await page.waitForTimeout(150);
  expect(first.sent).toHaveLength(firstCount);
  expect(second.sent.some((frame) => frame.msg.case === 'listSessions')).toBe(true);
});

test('sessions default to all projects, filter explicitly, and keep abandoned sessions hidden', async ({ page }) => {
  const sessions = [
    create(SessionInfoSchema, { ...secondDirect, project: 'arc', lastAt: create(TimestampSchema, { seconds: 90n }) }),
    create(SessionInfoSchema, { id: 'empty', source: Source.USER, startedAt: { seconds: 1000n } }),
    create(SessionInfoSchema, { ...job, lastAt: create(TimestampSchema, { seconds: 900n }) }),
    create(SessionInfoSchema, { id: 'legacy', title: 'Legacy job', source: Source.UNSPECIFIED, lastAt: { seconds: 950n } }),
    create(SessionInfoSchema, { id: 'abandoned', title: 'Wrong turn', project: 'arc-web', source: Source.USER, lastAt: { seconds: 300n }, disposition: BranchMarked_Disposition.ABANDONED }),
    create(SessionInfoSchema, { id: 'older', title: 'Earlier work', preview: 'Recent window changes', project: 'arc-web', source: Source.USER, lastAt: { seconds: 50n } }),
    create(SessionInfoSchema, { ...direct, project: 'arc-web', lastAt: create(TimestampSchema, { seconds: 100n }) }),
  ];
  const server = daemon(sessions);
  await page.routeWebSocket('ws://127.0.0.1:8787', (route) => server.attach(route));
  await page.setViewportSize({ width: 1100, height: 840 });
  await page.goto('/');
  await addHost(page);
  const sidebar = page.locator('.sidebar');
  const titles = sidebar.locator('.session span');
  await expect(titles).toHaveText(['Protocol history', 'Another conversation', 'Earlier work']);
  const project = sidebar.getByRole('combobox', { name: 'Project' });
  await expect(project).toHaveValue('');
  await project.selectOption('arc-web');
  await expect(titles).toHaveText(['Protocol history', 'Earlier work']);
  await expect(sidebar).not.toContainText('Wrong turn');
  await project.selectOption('');
  await expect(titles).toHaveText(['Protocol history', 'Another conversation', 'Earlier work']);
  await expect(page.getByRole('searchbox', { name: 'Filter sessions' })).toHaveCount(0);
  await expect(page.locator('.title')).toHaveText('Protocol history');
  await expect(page.locator('.message').first()).toContainText('Question');
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole('button', { name: 'Open sessions' }).click();
  const sheet = page.getByRole('dialog', { name: 'Sessions', exact: true });
  await expect(sheet.getByRole('combobox', { name: 'Project' })).toHaveValue('');
  await expect(sheet.getByRole('checkbox')).toHaveCount(0);
  await expect(sheet.getByText('Newest first')).toHaveCount(0);
  await sheet.getByRole('combobox', { name: 'Project' }).selectOption('arc');
  await expect(sheet.locator('.session span')).toHaveText(['Another conversation']);
  await expect(page.locator('.title')).toHaveText('Protocol history');
  await sheet.getByRole('button', { name: /Another conversation/ }).click();
  await expect(page.locator('.title')).toHaveText('Another conversation');
  await page.getByRole('button', { name: 'Open sessions' }).click();
  await expect(sheet.getByRole('combobox', { name: 'Project' })).toHaveValue('arc');
  await sheet.getByRole('button', { name: 'Close', exact: true }).click();
  await page.getByRole('button', { name: 'New conversation' }).click();
  await expect(page.locator('.title')).toHaveText('New conversation');
  await page.getByRole('button', { name: 'Open sessions' }).click();
  await expect(sheet.getByRole('combobox', { name: 'Project' })).toHaveValue('arc');
  await sheet.getByRole('button', { name: 'Close', exact: true }).click();
  await page.setViewportSize({ width: 1100, height: 840 });
  await expect(project).toHaveValue('arc');
  await expect(titles).toHaveText(['Another conversation']);
});

test('jobs show only daemon-reported jobs and never inflate the list with archived sessions', async ({ page }) => {
  const server = daemon([direct, job], [
    create(JobInfoSchema, { sessionId: 'current-job', title: 'Current daemon job', state: JobInfo_State.RUNNING }),
  ]);
  await page.routeWebSocket('ws://127.0.0.1:8787', (route) => server.attach(route));
  await page.goto('/');
  await addHost(page);
  await expect(page.locator('.job-status')).toContainText('1 job running');
  await page.locator('.job-status').click();
  const sheet = page.getByRole('dialog', { name: 'Jobs', exact: true });
  await expect(sheet.locator('.job')).toHaveCount(1);
  await expect(sheet.locator('.job')).toContainText('Current daemon job');
  await expect(sheet.locator('.job')).not.toContainText('Model job');
});

test('legacy Demo profiles are removed while the saved real host and draft survive', async ({ page }) => {
  const server = daemon();
  await page.routeWebSocket('ws://127.0.0.1:8787', (route) => server.attach(route));
  await page.addInitScript(() => {
    if (localStorage.getItem('arc-web.local.v1')) return;
    localStorage.setItem('arc-web.local.v1', JSON.stringify({
      hosts: [{ id: 'demo', name: 'Demo', kind: 'demo', endpoint: '' },
        { id: 'existing', name: 'Saved ARC', kind: 'daemon', endpoint: 'ws://127.0.0.1:8787' }],
      activeHostId: 'demo', selected: { existing: 'chat-1' },
      drafts: { existing: { 'chat-1': 'Keep my real draft' } }, inputs: {},
    }));
  });
  await page.goto('/');
  await expect(page.locator('.status')).toContainText('Saved ARC');
  await expect(page.getByRole('textbox', { name: 'Message to ARC' })).toHaveValue('Keep my real draft');
  await page.getByRole('button', { name: 'Settings', exact: true }).click();
  await expect(page.locator('.host-select')).toHaveCount(1);
  await expect(page.locator('.host-select')).not.toContainText('Demo');
  expect(await page.evaluate(() => JSON.parse(localStorage.getItem('arc-web.local.v1')!).hosts.map((host: { id: string }) => host.id))).toEqual(['existing']);
});

test('legacy browser state migrates without retrying uncertain input', async ({ page }) => {
  const server = daemon();
  await page.routeWebSocket('ws://127.0.0.1:8787', (route) => server.attach(route));
  await page.addInitScript(() => {
    if (localStorage.getItem('arc-web.local.v1')) return;
    localStorage.setItem('cairn.local.v1', JSON.stringify({
      hosts: [{ id: 'existing', name: 'Saved ARC', kind: 'daemon', endpoint: 'ws://127.0.0.1:8787' }],
      activeHostId: 'existing',
      selected: { existing: 'chat-1' },
      drafts: { existing: { 'chat-1': 'Keep this migrated draft' } },
      inputs: { existing: [
        { id: 'uncertain-input', sessionId: 'chat-1', content: 'Possibly already sent', state: 'uncertain' },
        { id: 'pending-input', sessionId: 'chat-1', content: 'Pending before reload', state: 'pending' },
      ] },
    }));
  });
  await page.setViewportSize({ width: 1100, height: 840 });
  await page.goto('/');
  await expect(page.locator('.status')).toContainText('Connected');
  await expect(page.locator('.title')).toHaveText('Protocol history');
  await expect(page.locator('.message')).toHaveCount(3);
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await expect(composer).toHaveValue('Keep this migrated draft');
  await expect(page.locator('.uncertain-input')).toContainText(['Possibly already sent', 'Pending before reload']);
  await expect.poll(() => page.evaluate(() => localStorage.getItem('arc-web.local.v1'))).not.toBeNull();
  expect(await page.evaluate(() => localStorage.getItem('cairn.local.v1'))).toBeNull();
  const saved = await page.evaluate(() => JSON.parse(localStorage.getItem('arc-web.local.v1')!));
  expect(saved.selected.existing).toBe('chat-1');
  expect(saved.drafts.existing['chat-1']).toBe('Keep this migrated draft');
  expect(saved.inputs.existing).toMatchObject([
    { id: 'uncertain-input', state: 'uncertain' },
    { id: 'pending-input', state: 'uncertain' },
  ]);
  expect(server.sent.some((frame) => frame.msg.case === 'sendMessage')).toBe(false);

  await page.reload();
  await expect(page.locator('.status')).toContainText('Connected');
  await expect(page.locator('.message')).toHaveCount(3);
  await expect(composer).toHaveValue('Keep this migrated draft');
  await expect(page.locator('.uncertain-input')).toContainText(['Possibly already sent', 'Pending before reload']);
  expect(await page.evaluate(() => localStorage.getItem('cairn.local.v1'))).toBeNull();
  expect(server.sent.some((frame) => frame.msg.case === 'sendMessage')).toBe(false);
});

test('consecutive tool calls have compact spacing while touch disclosures remain reachable', async ({ page }) => {
  const server = daemon();
  server.setHistory(create(SessionHistorySchema, { sessionId: 'chat-1', entries: [
    ...durableHistory.entries.slice(0, 3),
    create(HistoryEntrySchema, { seq: 5n, entry: { case: 'toolCall', value: { callId: 'c2', name: 'second tool' } } }),
    create(HistoryEntrySchema, { seq: 6n, entry: { case: 'toolResult', value: { callId: 'c2', outcome: ToolOutcome.OK, content: 'Second output' } } }),
    create(HistoryEntrySchema, { seq: 7n, entry: { case: 'message', value: { role: Role.USER, source: Source.SYSTEM, content: 'Internal system handoff prompt' } } }),
  ] }));
  await page.routeWebSocket('ws://127.0.0.1:8787', (route) => server.attach(route));
  await page.setViewportSize({ width: 1100, height: 840 });
  await page.goto('/');
  await addHost(page);
  await expect(page.locator('.tool-call')).toHaveCount(2);
  await expect(page.getByText('Internal system handoff prompt')).toHaveCount(0);
  const summaries = page.locator('.tool-call summary');
  const first = (await summaries.nth(0).boundingBox())!, second = (await summaries.nth(1).boundingBox())!;
  expect(second.y - (first.y + first.height)).toBeLessThanOrEqual(4);
  expect(first.height).toBeLessThanOrEqual(32);
  await page.setViewportSize({ width: 390, height: 844 });
  expect((await summaries.nth(0).boundingBox())!.height).toBeGreaterThanOrEqual(44);
});
