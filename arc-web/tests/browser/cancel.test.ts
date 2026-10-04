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
  await composer.fill('Next draft');
  await cancel.click();
  await expect.poll(() => cancellations).toEqual(['test-planning']);
  await expect(cancel).toBeDisabled();
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toHaveCount(0);
  releaseCancel();
  await expect(page.locator('.message').last()).toContainText('I’ll');
  await expect(cancel).toBeDisabled();
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
  await composer.fill('Next draft');
  const cancel = page.getByRole('button', { name: 'Cancel', exact: true });
  await expect(cancel).toBeEnabled();
  await cancel.click();
  await expect(cancel).toBeDisabled();
  release();
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeVisible();
  await expect(tool.locator('.tool-label')).toHaveText('failed');
  await tool.locator('summary').click();
  await expect(tool.locator('.tool-output')).toHaveText('Cancelled');
  await expect(composer).toHaveValue('Next draft');
});

test('Enter does not send or cancel and navigation keeps the active turn scoped', async ({ page }) => {
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
  await composer.press('Enter');
  await expect(composer).toHaveValue('Next draft');
  expect(sent).toEqual([]);
  await page.locator('.session-list').getByRole('button', { name: /Trace a flaky test/ }).click();
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Cancel', exact: true })).toHaveCount(0);
  await page.locator('.session-list').getByRole('button', { name: /Plan a focused week/ }).click();
  await expect(cancel).toBeVisible();
  await expect(composer).toHaveValue('Next draft');
  release();
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
  await composer.fill('Preserve this draft');
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
