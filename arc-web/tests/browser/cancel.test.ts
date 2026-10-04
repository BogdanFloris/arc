import { expect, test } from '@playwright/test';
import { fixtureDaemon } from '../fixtures/daemon';

async function connect(page: import('@playwright/test').Page, attach: ReturnType<typeof fixtureDaemon>) {
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', (socket) => attach(socket));
  await page.addInitScript(() => {
    localStorage.setItem('arc-web.local.v1', JSON.stringify({
      hosts: [{ id: 'test-daemon', name: 'Test daemon', endpoint: 'ws://127.0.0.1:8787/arc', kind: 'daemon' }],
      activeHostId: 'test-daemon', selected: { 'test-daemon': 'test-planning' }, drafts: {}, inputs: {},
    }));
  });
  await page.goto('/');
}

test('a running turn can be cancelled without losing the next draft', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  let releaseStream!: () => void;
  const streamGate = new Promise<void>((resolve) => { releaseStream = resolve; });
  let releaseCancel!: () => void;
  const cancelGate = new Promise<void>((resolve) => { releaseCancel = resolve; });
  const cancellations: string[] = [];
  const attach = fixtureDaemon({
    streamEndGate: () => streamGate,
    cancelGate: () => cancelGate,
    onCancel: (_kind, sessionId) => cancellations.push(sessionId),
  });
  await connect(page, attach);
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await composer.fill('Start a turn');
  const sendBounds = await page.getByRole('button', { name: 'Send', exact: true }).boundingBox();
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  const cancel = page.getByRole('button', { name: 'Cancel', exact: true });
  await expect(page.locator('.message').last()).toContainText('I’ll');
  await expect(cancel).toBeEnabled();
  expect(await cancel.boundingBox()).toEqual(sendBounds);
  await page.screenshot({ path: '/tmp/arc-web-cancel-phone.png' });
  await cancel.click();
  await expect.poll(() => cancellations).toEqual(['test-planning']);
  await expect(cancel).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toHaveCount(0);
  await composer.fill('Next draft');
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeDisabled();
  releaseCancel();
  await expect(page.locator('.message').last()).toContainText('I’ll');
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeDisabled();
  releaseStream();
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeVisible();
  await expect(composer).toHaveValue('Next draft');
  await expect(page.locator('.message').filter({ hasText: 'Start a turn' })).toHaveCount(1);
  await expect(page.locator('.message').last()).toContainText('I’ll');
  await expect(page.locator('.message').last()).not.toContainText('work through');
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  await expect(page.locator('.message').last()).toContainText('I’ll help you work through: Next draft');
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeVisible();
});

test('Cancel remains available during tool work and restores the recorded result', async ({ page }) => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const attach = fixtureDaemon({ toolGate: () => gate });
  await connect(page, attach);
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await composer.fill('Run a tool');
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  const tool = page.locator('.tool').filter({ has: page.locator('.tool-name', { hasText: 'bash' }) });
  await expect(tool.locator('.tool-label')).toHaveText('running');
  const cancel = page.getByRole('button', { name: 'Cancel', exact: true });
  await expect(cancel).toBeEnabled();
  await cancel.click();
  await expect(cancel).toBeDisabled();
  await composer.fill('Next draft');
  release();
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeVisible();
  await expect(tool.locator('.tool-label')).toHaveText('failed');
  await tool.locator('summary').click();
  await expect(tool.locator('.tool-output')).toHaveText('Cancelled');
  await expect(composer).toHaveValue('Next draft');
});

test('Enter steers without cancelling and navigation keeps the active turn scoped', async ({ page }) => {
  const sent: string[] = [];
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const attach = fixtureDaemon({ streamGate: () => gate, onCancel: (_kind, id) => sent.push(`cancel:${id}`) });
  await connect(page, attach);
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await composer.fill('Active work');
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  const cancel = page.getByRole('button', { name: 'Cancel', exact: true });
  await expect(cancel).toBeVisible();
  await composer.fill('Next draft');
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeEnabled();
  await composer.press('Enter');
  await expect(composer).toHaveValue('');
  await expect(page.locator('.message').filter({ hasText: 'Next draft' })).toHaveCount(1);
  await expect(cancel).toBeEnabled();
  expect(sent).toEqual([]);
  await page.locator('.session-list').getByRole('button', { name: /Trace a flaky test/ }).click();
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Cancel', exact: true })).toHaveCount(0);
  await page.locator('.session-list').getByRole('button', { name: /Plan a focused week/ }).click();
  await expect(cancel).toBeVisible();
  await expect(composer).toHaveValue('');
  await expect(page.locator('.message').filter({ hasText: 'Next draft' })).toHaveCount(1);
  release();
  await expect(page.locator('.message').last()).toContainText('I’ll help');
});

test('failed cancellation reports an error and can be retried', async ({ page }) => {
  const requests: string[] = [];
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const attach = fixtureDaemon({ streamEndGate: () => gate, cancelError: 'Cancel rejected', onCancel: (_kind, id) => requests.push(id) });
  await connect(page, attach);
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await composer.fill('Keep running');
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  await expect(page.locator('.message').last()).toContainText('I’ll');
  await page.getByRole('button', { name: 'Cancel', exact: true }).click();
  const cancel = page.getByRole('button', { name: 'Cancel', exact: true });
  await expect(page.getByRole('button', { name: 'ARC status' })).toContainText('Connected');
  await expect(cancel).toBeEnabled();
  expect(requests).toEqual(['test-planning']);
  await page.getByRole('button', { name: 'ARC status' }).click();
  const status = page.getByRole('dialog', { name: 'ARC status' });
  await expect(status).toContainText('Could not cancel. Cancel rejected');
  await status.getByRole('button', { name: 'Close', exact: true }).click();
  attach.setCancelError(undefined);
  await cancel.click();
  await expect.poll(() => requests.length).toBe(2);
  await expect(cancel).toBeDisabled();
  await composer.fill('Preserve this draft');
  release();
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeVisible();
  await expect(composer).toHaveValue('Preserve this draft');
});

test('a running job uses cancelJob and waits for terminal job state', async ({ page }) => {
  const cancellations: string[] = [];
  const attach = fixtureDaemon({ onCancel: (kind, id) => cancellations.push(`${kind}:${id}`) });
  await connect(page, attach);
  await page.getByRole('button', { name: 'Jobs', exact: true }).click();
  await page.getByRole('dialog', { name: 'Jobs', exact: true }).getByRole('button', { name: /^Open job:/ }).click();
  const cancel = page.getByRole('button', { name: 'Cancel', exact: true });
  await expect(cancel).toBeVisible();
  await cancel.click();
  await expect.poll(() => cancellations).toEqual(['job:job-review']);
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Cancel', exact: true })).toHaveCount(0);
});

test('work started by another client restores Cancel on load and after reload', async ({ page }) => {
  const cancelled: string[] = [];
  const attach = fixtureDaemon({ runningSessions: ['test-planning'], onCancel: (kind, id) => cancelled.push(`${kind}:${id}`) });
  await connect(page, attach);
  const cancel = page.getByRole('button', { name: 'Cancel', exact: true });
  await expect(cancel).toBeEnabled();
  await page.reload();
  await expect(cancel).toBeEnabled();
  await cancel.click();
  await expect.poll(() => cancelled).toEqual(['turn:test-planning']);
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeVisible();
});

test('shared-session activity switches the control and shows steers without duplicate echoes', async ({ page }) => {
  const attach = fixtureDaemon();
  await connect(page, attach);
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeVisible();
  attach.setActivity('test-planning', true);
  const cancel = page.getByRole('button', { name: 'Cancel', exact: true });
  await expect(cancel).toBeEnabled();
  for (let i = 1; i <= 2; i++) {
    await composer.fill('Use the other approach');
    const send = page.getByRole('button', { name: 'Send', exact: true });
    await expect(send).toBeEnabled();
    await expect(send).toHaveAttribute('title', 'Send steer');
    await composer.press('Enter');
    await expect(composer).toHaveValue('');
    await expect(page.locator('.message').filter({ hasText: 'Use the other approach' })).toHaveCount(i);
    await expect(cancel).toBeEnabled();
  }
  attach.setActivity('test-planning', false);
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeVisible();
  await expect(page.locator('.message').filter({ hasText: 'Use the other approach' })).toHaveCount(2);
});

test('two clients share activity while delayed steering stays visible and preserves the original reply', async ({ page, context }) => {
  let releaseReply!: () => void, releaseSteers!: () => void;
  const replyGate = new Promise<void>((resolve) => { releaseReply = resolve; });
  const steerGate = new Promise<void>((resolve) => { releaseSteers = resolve; });
  const attach = fixtureDaemon({ streamEndGate: () => replyGate, steerGate: () => steerGate });
  await connect(page, attach);
  const other = await context.newPage();
  await connect(other, attach);
  await other.getByRole('textbox', { name: 'Message to ARC' }).fill('Work from another client');
  await other.getByRole('button', { name: 'Send', exact: true }).click();
  await expect(other.locator('.message').last()).toContainText('I’ll');
  const cancel = page.getByRole('button', { name: 'Cancel', exact: true });
  await expect(cancel).toBeEnabled();
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  for (let count = 1; count <= 2; count++) {
    await composer.fill('Same steer twice');
    await page.getByRole('button', { name: 'Send', exact: true }).click();
    await expect(page.locator('.message').filter({ hasText: 'Same steer twice' })).toHaveCount(count);
    await expect(cancel).toBeEnabled();
  }
  await expect(other.locator('.message').last()).toContainText('I’ll');
  releaseSteers();
  await expect.poll(() => attach.statusFetches()).toBeGreaterThan(2);
  await expect(page.locator('.message').filter({ hasText: 'Same steer twice' })).toHaveCount(2);
  releaseReply();
  await expect(other.locator('.message').last()).toContainText('I’ll help you work through: Work from another client');
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeVisible();
  await expect(page.locator('.message').filter({ hasText: 'Same steer twice' })).toHaveCount(2);
  await expect(page.locator('.message').last()).toContainText('I’ll help you work through: Work from another client');
});
