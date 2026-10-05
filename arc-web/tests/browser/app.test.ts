import { expect, test } from '@playwright/test';
import { fixtureDaemon } from '../fixtures/daemon';
import { create } from '@bufbuild/protobuf';
import { AllowanceWindowSchema } from '../../src/lib/arc/gen/wire_pb';

test('a waiting reply shows three quiet dots without changing the header', async ({ page }) => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const attach = fixtureDaemon({ streamGate: () => gate });
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', (socket) => attach(socket));
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  const pending = page.getByRole('status', { name: 'Waiting for ARC' });
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await expect(pending).toHaveCount(0);
  await composer.fill('Wait for the first reply');
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  await expect(pending).toBeVisible();
  await expect(pending.locator('span')).toHaveCount(3);
  await expect(page.getByRole('button', { name: 'Cancel', exact: true })).toBeEnabled();
  await expect(page.locator('.message').last().locator('.role')).toHaveText('YOU');
  await expect(page.locator('.status')).toHaveText('Test daemon· Connected');
  await expect(page.locator('.transcript')).toHaveAttribute('aria-busy', 'true');
  await composer.fill('Next draft stays editable');
  await expect(composer).toHaveValue('Next draft stays editable');
  await page.emulateMedia({ reducedMotion: 'reduce' });
  expect(await pending.locator('span').first().evaluate((dot) => getComputedStyle(dot).animationName)).toBe('none');
  await page.screenshot({ path: '/tmp/arc-web-waiting-reply-phone.png' });
  release();
  await expect(page.locator('.message').last()).toContainText('I’ll help you work through:');
  await expect(pending).toHaveCount(0);
  await expect(composer).toHaveValue('Next draft stays editable');
  await expect(page.locator('.transcript')).toHaveAttribute('aria-busy', 'false');
});

test('job return restores the origin draft and keeps phone rows aligned', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await page.locator('textarea').fill('unsent origin draft');
  await page.locator('.job-status').click();
  await page.locator('.job-copy strong').click();
  await expect(page.locator('.job-return button')).toHaveText('Back');
  await expect(page.getByRole('dialog')).not.toBeVisible();
  await page.locator('.job-return button').click();
  await expect(page.locator('textarea')).toHaveValue('unsent origin draft');
  await page.locator('.job-status').click();
  const metrics = await page.locator('.job').evaluate((row) => {
    const copy = row.querySelector('.job-copy')!.getBoundingClientRect();
    const chevron = row.querySelector('.job-chevron')!.getBoundingClientRect();
    return { copyLeft: copy.left, chevronLeft: chevron.left, height: row.getBoundingClientRect().height, overflow: document.documentElement.scrollWidth > innerWidth };
  });
  expect(metrics.chevronLeft).toBeGreaterThan(metrics.copyLeft);
  expect(metrics.height).toBeGreaterThanOrEqual(44);
  expect(metrics.overflow).toBe(false);
  await expect(page.locator('.job')).toHaveRole('button');
  await expect(page.locator('.job')).not.toContainText('Open');
  await page.locator('.job').focus();
  await page.locator('.job').press('Enter');
  await expect(page.getByRole('dialog')).not.toBeVisible();
  await expect(page.locator('.job-return button')).toHaveText('Back');
  await expect(page.locator('.message').first()).toBeVisible();
  for (const width of [390, 1280]) {
    await page.setViewportSize({ width, height: 844 });
    const alignment = await page.locator('.job-return button').evaluate((button) => ({
      left: button.getBoundingClientRect().left,
      transcriptLeft: document.querySelector('.message .role')!.getBoundingClientRect().left,
      height: button.getBoundingClientRect().height,
      background: getComputedStyle(button).backgroundColor,
      border: getComputedStyle(button).borderWidth,
    }));
    expect(Math.abs(alignment.left - alignment.transcriptLeft)).toBeLessThan(1);
    expect(alignment.height).toBeGreaterThanOrEqual(44);
    expect(alignment.background).toBe('rgba(0, 0, 0, 0)');
    expect(alignment.border).toBe('0px');
    await page.screenshot({ path: `/tmp/arc-web-job-return-${width}.png` });
  }
});

test('job navigation and status refresh preserve live observation and reader position', async ({ page }) => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => release = resolve);
  const attach = fixtureDaemon({ streamGate: () => gate });
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', (socket) => attach(socket));
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  const send = page.getByRole('button', { name: 'Send', exact: true });
  const transcript = page.locator('.transcript');
  await composer.fill('Long source conversation for reading-position testing.\n'.repeat(35));
  await send.click();
  await expect(composer).toHaveValue('');
  const pending = page.getByRole('status', { name: 'Waiting for ARC' });
  await expect(pending).toBeVisible();
  await composer.fill('Next draft remains with the source');
  await expect.poll(() => transcript.evaluate((element) => element.scrollTop)).toBeGreaterThan(200);
  await transcript.evaluate((element) => element.scrollTop = 80);
  await expect(page.getByRole('button', { name: 'Jump to latest' })).toBeVisible();
  await page.getByRole('button', { name: 'Jobs', exact: true }).click();
  await page.screenshot({ path: '/tmp/arc-web-parity-phone-jobs.png' });
  await page.getByRole('dialog', { name: 'Jobs', exact: true }).getByRole('button', { name: /^Open job:/ }).click();
  await expect(page.locator('.title')).toHaveText('Review draft outline');
  await expect(pending).toHaveCount(0);
  await expect(composer).toHaveValue('');
  await page.getByRole('button', { name: 'Back to conversation', exact: true }).click();
  await expect(composer).toHaveValue('Next draft remains with the source');
  await expect(pending).toBeVisible();
  await expect(send).toBeEnabled();
  await expect(send).toHaveAttribute('title', 'Send steer');
  await expect.poll(() => transcript.evaluate((element) => element.scrollTop)).toBe(80);
  await expect(page.getByRole('button', { name: 'Jump to latest' })).toBeVisible();
  await page.getByRole('button', { name: 'ARC status' }).click();
  const status = page.getByRole('dialog', { name: 'ARC status' });
  await expect(page.locator('.status')).toHaveText('Test daemon· Connected');
  const connection = status.locator('.status-section').filter({ has: page.getByRole('heading', { name: 'Connection', exact: true }) });
  await expect(connection.locator('p')).toHaveText(['Test daemon · Connected']);
  const connectionHeight = (await connection.boundingBox())!.height;
  await expect(status.getByRole('button', { name: 'Refresh' })).toBeEnabled();
  await status.getByRole('button', { name: 'Refresh' }).click();
  await expect(status.getByRole('button', { name: 'Refresh' })).toBeEnabled();
  await expect(connection.locator('p')).toHaveText(['Test daemon · Connected']);
  expect((await connection.boundingBox())!.height).toBe(connectionHeight);
  await expect(status).not.toContainText('Changes apply');
  await expect(status).not.toContainText('Latest completed-step');
  await status.getByRole('button', { name: 'Close', exact: true }).click();
  await expect(send).toBeEnabled();
  await expect(send).toHaveAttribute('title', 'Send steer');
  release();
  await expect(send).toBeEnabled({ timeout: 10000 });
  await expect(pending).toHaveCount(0);
  await expect(page.locator('.message').last()).toContainText('I’ll help you work through:');
  await expect.poll(() => transcript.evaluate((element) => element.scrollTop)).toBe(80);
  await page.getByRole('button', { name: 'Jump to latest' }).click();
  await expect(page.getByRole('button', { name: 'Jump to latest' })).not.toBeVisible();
  await page.getByRole('button', { name: 'Jobs', exact: true }).click();
  await page.getByRole('dialog', { name: 'Jobs', exact: true }).getByRole('button', { name: /^Open job:/ }).click();
  await page.getByRole('button', { name: 'Back to conversation', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Jump to latest' })).not.toBeVisible();
  await expect.poll(() => transcript.evaluate((element) => element.scrollHeight - element.scrollTop - element.clientHeight)).toBeLessThan(48);
});

test('new-draft job return survives without overwriting and unrelated navigation clears the route', async ({ page }) => {
  const attach = fixtureDaemon({ jobsRunning: false });
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', (socket) => attach(socket));
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  const openJob = async () => {
    await page.getByRole('button', { name: 'Jobs', exact: true }).click();
    await page.getByRole('dialog', { name: 'Jobs', exact: true }).getByRole('button', { name: /^Open job:/ }).click();
    await expect(page.locator('.job-return')).toBeVisible();
  };
  await page.getByRole('button', { name: 'New conversation', exact: true }).click();
  await composer.fill('Unsent new conversation');
  await openJob();
  await page.getByRole('button', { name: 'Back to conversation', exact: true }).click();
  await expect(composer).toHaveValue('Unsent new conversation');
  await openJob();
  await page.getByRole('button', { name: 'Open sessions' }).click();
  await page.getByRole('dialog', { name: 'Sessions', exact: true }).getByRole('button', { name: /Notes on a first draft/ }).click();
  await expect(page.locator('.job-return')).toHaveCount(0);
  await openJob();
  await page.getByRole('button', { name: 'New conversation', exact: true }).click();
  await expect(page.locator('.job-return')).toHaveCount(0);
  await openJob();
  await expect(page.getByRole('combobox', { name: 'Model' })).toBeEnabled();
  await page.getByRole('combobox', { name: 'Model' }).selectOption('deep');
  await page.getByRole('button', { name: 'Fork', exact: true }).click();
  await expect(page.locator('.job-return')).toHaveCount(0);
  await openJob();
  await page.getByRole('button', { name: 'Settings', exact: true }).click();
  const settings = page.getByRole('dialog', { name: 'Settings', exact: true });
  await settings.getByRole('button', { name: 'Add daemon host' }).click();
  await settings.getByLabel('Name', { exact: true }).fill('Another host');
  await settings.getByLabel('Endpoint', { exact: true }).fill('ws://127.0.0.1:9999/arc');
  await settings.getByRole('button', { name: 'Add host', exact: true }).click();
  await settings.getByRole('button', { name: 'Close', exact: true }).click();
  await expect(page.locator('.job-return')).toHaveCount(0);
});

test.beforeEach(async ({ page }) => {
  const attachDaemon = fixtureDaemon();
  await page.addInitScript(() => {
    if (localStorage.getItem('arc-web.local.v1')) return;
    localStorage.setItem('arc-web.local.v1', JSON.stringify({
      hosts: [{ id: 'test-daemon', name: 'Test daemon', endpoint: 'ws://127.0.0.1:8787/arc', kind: 'daemon' }],
      activeHostId: 'test-daemon',
      selected: { 'test-daemon': 'test-planning' },
      drafts: {}, inputs: {},
    }));
  });
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', (socket) => attachDaemon(socket));
});

for (const width of [320, 390, 768, 1440]) {
  test(`layout stays usable at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 844 });
    await page.goto('/');
    await expect(page.locator('.message')).toHaveCount(3);
    await expect(page.locator('.main > .notice')).toHaveCount(0);
    await expect(page.locator('.job-status')).toHaveCount(1);
    await expect(page.locator('.job-status')).toBeVisible();
    await expect(page.locator('.brand img')).toHaveJSProperty('complete', true);
    if (width > 700) await expect(page.locator('.brand img')).toHaveJSProperty('naturalWidth', 256);

    const layout = await page.evaluate(() => {
      const overlaps = (a: DOMRect, b: DOMRect) =>
        Math.min(a.right, b.right) - Math.max(a.left, b.left) > 1 &&
        Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top) > 1;
      const visible = (element: Element) => element.getClientRects().length > 0;
      const header = [...document.querySelectorAll('.top > *')].filter(visible);
      const headerActions = [...document.querySelectorAll('.header-actions > *')].filter(visible);
      const context = [...document.querySelectorAll('.context > *')].filter(visible);
      const buttons = [...document.querySelectorAll('button')].filter(visible);
      const pane = document.querySelector('.main')!.getBoundingClientRect();
      const scroller = document.querySelector('.transcript')!.getBoundingClientRect();
      const scrollElement = document.querySelector<HTMLElement>('.transcript')!;
      const textColumn = document.querySelector('.transcript-content')!.getBoundingClientRect();
      const prompt = document.querySelector('.message.from-user')!;
      const reply = document.querySelector('.message:not(.from-user)')!;
      const promptStyle = getComputedStyle(prompt);
      const replyStyle = getComputedStyle(reply);
      const promptText = prompt.querySelector('p')!;
      const replyText = reply.querySelector('p')!;
      return {
        pageOverflow: document.documentElement.scrollWidth > innerWidth,
        extraPageScroll: document.body.scrollHeight > innerHeight + 1,
        headerOverlap: header.some((a, i) => header.slice(i + 1).some((b) =>
          overlaps(a.getBoundingClientRect(), b.getBoundingClientRect()))),
        headerActionOverlap: headerActions.some((a, i) => headerActions.slice(i + 1).some((b) =>
          overlaps(a.getBoundingClientRect(), b.getBoundingClientRect()))),
        statusOverlap: context.some((a, i) => context.slice(i + 1).some((b) =>
          overlaps(a.getBoundingClientRect(), b.getBoundingClientRect()))),
        smallButton: buttons.some((button) => {
          const bounds = button.getBoundingClientRect();
          return bounds.width < 44 || bounds.height < 44;
        }),
        scrollAtOuterEdge: Math.abs(scroller.right - pane.right) < 1 && Math.abs(scroller.width - pane.width) < 1,
        boundedTextColumn: textColumn.width <= 760,
        slimScrollbarGutters: scrollElement.offsetWidth - scrollElement.clientWidth <= 12,
        distinctPrompt: promptStyle.backgroundColor !== replyStyle.backgroundColor &&
          promptStyle.backgroundColor !== getComputedStyle(document.querySelector('.compose-row')!).backgroundColor,
        unboxedReply: replyStyle.backgroundColor === 'rgba(0, 0, 0, 0)',
        alignedMessageText: Math.abs(promptText.getBoundingClientRect().left - replyText.getBoundingClientRect().left) < 1,
        sharedMessageTypography: getComputedStyle(promptText).font === getComputedStyle(replyText).font &&
          getComputedStyle(promptText).color === getComputedStyle(replyText).color,
      };
    });
    expect(layout).toEqual({
      pageOverflow: false, extraPageScroll: false, headerOverlap: false, headerActionOverlap: false,
      statusOverlap: false, smallButton: false,
      scrollAtOuterEdge: true, boundedTextColumn: true,
      slimScrollbarGutters: true,
      distinctPrompt: true, unboxedReply: true,
      alignedMessageText: true, sharedMessageTypography: true,
    });
    await page.getByRole('button', { name: 'Settings', exact: true }).click();
    await expect(page.getByRole('dialog')).toBeVisible();
    await page.keyboard.press('Escape');
    await expect(page.getByRole('dialog')).not.toBeVisible();
    await expect(page.getByRole('button', { name: 'Settings', exact: true })).toBeFocused();
  });
}

for (const width of [390, 1440]) {
  test(`new conversation lives in the header and preserves drafts at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 844 });
    await page.goto('/');
    await expect(page.locator('.message')).toHaveCount(3);
    const create = page.getByRole('button', { name: 'New conversation', exact: true });
    await expect(create).toHaveCount(1);
    await expect(create).toBeVisible();
    expect(await create.evaluate((button) => !!button.closest('.top'))).toBe(true);
    await expect(page.locator('.sidebar').getByRole('button', { name: 'New conversation', exact: true })).toHaveCount(0);
    await expect(page.getByText('Newest first', { exact: true })).toHaveCount(0);
    await expect(page.getByRole('checkbox', { name: 'Show abandoned sessions' })).toHaveCount(0);
    const composer = page.getByRole('textbox', { name: 'Message to ARC' });
    await composer.fill('Keep this conversation draft');
    await create.click();
    await expect(page.locator('.title')).toHaveText('New conversation');
    await expect(composer).toBeFocused();
    await expect(composer).toHaveValue('');
    await composer.fill('Keep the new conversation draft too');
    if (width < 701) {
      await page.getByRole('button', { name: 'Open sessions' }).click();
      const sheet = page.getByRole('dialog', { name: 'Sessions', exact: true });
      await expect(sheet.getByRole('button', { name: 'New conversation', exact: true })).toHaveCount(0);
      await sheet.getByRole('button', { name: /Plan a focused week/ }).click();
    } else {
      await page.locator('.session-list').getByRole('button', { name: /Plan a focused week/ }).click();
    }
    await expect(composer).toHaveValue('Keep this conversation draft');
    await create.click();
    await expect(composer).toHaveValue('Keep the new conversation draft too');
  });
}

test('new conversation creates with selected project and preset', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await page.getByRole('button', { name: 'New conversation', exact: true }).click();
  await page.getByRole('combobox', { name: 'Project' }).selectOption('configured-project');
  await page.getByRole('combobox', { name: 'Model' }).selectOption('deep');
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await composer.fill('Use the selected setup');
  await page.getByRole('button', { name: 'Send' }).click();
  await expect(page.locator('.message.from-user')).toContainText('Use the selected setup');
  await expect(page.getByRole('combobox', { name: 'Model' })).toHaveValue('__recorded__');
  await expect(page.locator('.conversation-controls .control-fixed').first()).toHaveText('configured-project');
});

test('existing conversation requires explicit model fork and cancel preserves original', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await composer.fill('Keep this draft across a fork');
  await page.getByRole('combobox', { name: 'Model' }).selectOption('deep');
  await expect(page.getByRole('group', { name: 'Confirm model fork' })).toContainText('Fork conversation with deep?');
  await page.getByRole('button', { name: 'Cancel' }).click();
  await expect(page.getByRole('combobox', { name: 'Model' })).toHaveValue('__recorded__');
  await expect(page.locator('.title')).toContainText('Plan a focused week');
  await expect(composer).toHaveValue('Keep this draft across a fork');
  await page.getByRole('combobox', { name: 'Model' }).selectOption('deep');
  await page.getByRole('button', { name: 'Fork', exact: true }).click();
  await expect(page.locator('.title')).toHaveText('New conversation');
  await expect(composer).toHaveValue('Keep this draft across a fork');
});

test('composer controls fit phone width and meet touch target size', async ({ page }) => {
  await page.setViewportSize({ width: 320, height: 844 });
  await page.goto('/');
  const metrics = await page.locator('.conversation-controls').evaluate((row) => ({
    overflow: document.documentElement.scrollWidth > innerWidth,
    controls: [...row.querySelectorAll('select, button')].map((element) => {
      const rect = element.getBoundingClientRect();
      return { width: rect.width, height: rect.height };
    }),
  }));
  expect(metrics.overflow).toBe(false);
  expect(metrics.controls.length).toBeGreaterThanOrEqual(2);
  expect(metrics.controls.every(({ width, height }) => width >= 44 && height >= 44)).toBe(true);
});

test('status stays plain and context controls share the existing composer toolbar', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  const status = page.getByRole('button', { name: 'ARC status' });
  await expect(status).toContainText('Connected');
  const style = await status.evaluate((element) => ({
    glass: element.classList.contains('glass'),
    border: getComputedStyle(element).borderWidth,
    background: getComputedStyle(element).backgroundColor,
    shadow: getComputedStyle(element).boxShadow,
  }));
  expect(style).toEqual({ glass: false, border: '0px', background: 'rgba(0, 0, 0, 0)', shadow: 'none' });
  await expect(page.locator('.compose-toolbar .conversation-controls')).toHaveCount(1);
  await expect(page.locator('.composer > .conversation-controls')).toHaveCount(0);
  await expect(page.locator('.compose-toolbar')).not.toContainText('ARC');
  const controls = await page.locator('.conversation-controls select').evaluateAll((elements) => elements.map((element) => ({
    appearance: getComputedStyle(element).appearance,
    border: getComputedStyle(element).borderWidth,
  })));
  expect(controls.every((control) => control.appearance === 'none' && control.border === '0px')).toBe(true);
  await page.screenshot({ path: '/tmp/arc-web-quiet-footer.png' });
});

test('orange focus outlines hug the control without a tinted fill', async ({ page }) => {
  const attach = fixtureDaemon();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', () => {});
  await page.goto('/');
  const status = page.getByRole('button', { name: 'ARC status' });
  await expect(status).toContainText('Connecting');
  await expect(page.getByRole('textbox', { name: 'Message to ARC' })).toHaveAttribute('placeholder', 'Message ARC…');
  const dotColor = await status.locator('.status-dot').evaluate((dot) => getComputedStyle(dot).backgroundColor);
  expect(dotColor).toBe('rgb(189, 174, 147)');
  await status.focus();
  const focus = await status.evaluate((button) => {
    const style = getComputedStyle(button);
    return { color: style.outlineColor, background: style.backgroundColor, outline: style.outlineStyle,
      width: style.outlineWidth, offset: style.outlineOffset, radius: style.borderRadius, padding: style.paddingLeft };
  });
  expect(focus).toEqual({ color: 'rgba(254, 128, 25, 0.7)', background: 'rgba(0, 0, 0, 0)', outline: 'solid',
    width: '1px', offset: '0px', radius: '12px', padding: '8px' });
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', (socket) => attach(socket));
  await page.reload();
  await expect(status).toContainText('Connected');
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await expect(composer).toHaveAttribute('placeholder', 'Message ARC…');
  await composer.fill('Orange send control');
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeEnabled();
  const controls = await page.evaluate(() => {
    const send = getComputedStyle(document.querySelector('.send')!);
    const textarea = getComputedStyle(document.querySelector('textarea')!);
    const row = getComputedStyle(document.querySelector('.compose-row')!);
    return { sendColor: send.color, sendBackground: send.backgroundColor,
      textareaOutline: textarea.outlineStyle, composerOutline: row.outlineStyle,
      composerBorder: row.borderColor, composerBackground: row.backgroundColor };
  });
  expect(controls).toEqual({ sendColor: 'rgb(40, 40, 40)', sendBackground: 'rgb(254, 128, 25)',
    textareaOutline: 'none', composerOutline: 'none', composerBorder: 'rgb(80, 73, 69)',
    composerBackground: 'rgb(66, 61, 57)' });
  await page.screenshot({ path: '/tmp/arc-web-close-outline-phone.png' });

  await page.setViewportSize({ width: 1440, height: 900 });
  const settings = page.getByRole('button', { name: 'Settings', exact: true });
  await settings.focus();
  await page.screenshot({ path: '/tmp/arc-web-close-outline-desktop.png' });
});

test('composer focus rings stay separated and text has room inside them at 320px', async ({ page }) => {
  await page.setViewportSize({ width: 320, height: 844 });
  await page.goto('/');
  await page.getByRole('button', { name: 'New conversation', exact: true }).click();
  await page.getByRole('button', { name: 'Choose thinking' }).click();
  const thinking = page.getByRole('combobox', { name: 'Thinking' });
  await thinking.selectOption('medium');
  const choices = page.locator('.conversation-controls select');
  for (const choice of await choices.all()) {
    await choice.focus();
    const metrics = await choice.evaluate((element) => {
      const style = getComputedStyle(element);
      const bounds = element.getBoundingClientRect();
      const toolbar = element.closest('.compose-toolbar')!.getBoundingClientRect();
      const next = element.parentElement!.nextElementSibling?.getBoundingClientRect()
        ?? element.closest('.compose-toolbar')!.querySelector('.send')!.getBoundingClientRect();
      return { paddingLeft: style.paddingLeft, paddingRight: style.paddingRight,
        outline: style.outlineStyle, offset: style.outlineOffset,
        outlineLeft: bounds.left - 1, toolbarLeft: toolbar.left, outlineRight: bounds.right + 1, nextLeft: next.left,
        parentOverflow: getComputedStyle(element.parentElement!).overflow };
    });
    expect(metrics.paddingLeft).toBe('6px');
    expect(metrics.paddingRight).toBe('6px');
    expect(metrics.outline).toBe('solid');
    expect(metrics.offset).toBe('0px');
    expect(metrics.outlineLeft).toBeGreaterThan(metrics.toolbarLeft);
    expect(metrics.outlineRight).toBeLessThan(metrics.nextLeft);
    expect(metrics.parentOverflow).toBe('visible');
  }
  const fits = await thinking.evaluate((element) => {
    const style = getComputedStyle(element);
    const canvas = document.createElement('canvas');
    const context = canvas.getContext('2d')!;
    context.font = style.font;
    return context.measureText('medium').width <= element.clientWidth - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
  });
  expect(fits).toBe(true);
  expect(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)).toBe(false);
  await page.screenshot({ path: '/tmp/arc-web-close-outline-320.png' });
});

test('startup and history loading do not flash transcript placeholders', async ({ page }) => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const attach = fixtureDaemon({ historyGate: () => gate });
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', () => {});
  await page.goto('/');
  await expect(page.locator('.status')).toContainText('Connecting');
  await expect(page.locator('.transcript')).toBeEmpty();
  await expect(page.locator('.empty')).toHaveCount(0);
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', (socket) => attach(socket));
  await page.reload();
  await expect(page.locator('.status')).toContainText('Connected');
  await expect(page.locator('.transcript')).toHaveAttribute('aria-busy', 'true');
  await expect(page.locator('.empty')).toHaveCount(0);
  release();
  await expect(page.locator('.message').first()).toBeVisible();
  await expect(page.locator('.transcript')).toHaveAttribute('aria-busy', 'false');
  await page.getByRole('button', { name: 'New conversation', exact: true }).click();
  await expect(page.locator('.empty')).toHaveText('Start a conversation or choose one from Sessions.');
  expect(await page.locator('.empty').evaluate((element) => getComputedStyle(element).color)).toBe('rgb(189, 174, 147)');
});

test('recorded high remains visible when the daemon exposes no editable thinking levels', async ({ page }) => {
  const attach = fixtureDaemon();
  attach.setStatus({ effectiveThinking: 'high', supportedThinking: [] });
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', (socket) => attach(socket));
  await page.goto('/');
  await expect(page.locator('.thinking-readonly')).toHaveText('high');
  await expect(page.locator('.conversation-controls')).not.toContainText('unavailable');
  await expect(page.getByRole('combobox', { name: 'Thinking' })).toHaveCount(0);
  await page.getByRole('button', { name: 'ARC status' }).click();
  const status = page.getByRole('dialog', { name: 'ARC status' });
  await expect(status).toContainText('Thinking · high');
  await expect(status).not.toContainText('no editable levels');
});

test('thinking picker appears only after deliberate preparation and lists supported levels', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'New conversation', exact: true }).click();
  await page.getByRole('button', { name: 'Choose thinking' }).click();
  const thinking = page.getByRole('combobox', { name: 'Thinking' });
  await expect(thinking).toBeVisible();
  await expect(thinking.locator('option')).toHaveText(['low', 'medium', 'high']);
  await thinking.selectOption('high');
  await expect(thinking).toHaveValue('high');
});

test('status sheet reports measured and stale data, refreshes, and keeps recovery by composer', async ({ page }) => {
  const attach = fixtureDaemon();
  attach.setStatus({
    contextObservedAt: BigInt(Math.floor(Date.now() / 1000) - 300),
    allowanceObservedAt: BigInt(Math.floor(Date.now() / 1000) - 300),
    allowanceStale: true,
  });
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', (socket) => attach(socket));
  await page.addInitScript(() => {
    const state = JSON.parse(localStorage.getItem('arc-web.local.v1')!);
    state.inputs = { 'test-daemon': [{ id: 'recover', sessionId: 'test-planning', content: 'Keep near composer', state: 'uncertain' }] };
    localStorage.setItem('arc-web.local.v1', JSON.stringify(state));
  });
  await page.goto('/');
  await expect(page.locator('.uncertain-input')).toContainText('Keep near composer');
  await expect(page.locator('.main > .notice')).toHaveCount(0);
  const status = page.getByRole('button', { name: 'ARC status' });
  const bounds = await status.boundingBox();
  expect(bounds?.height).toBeGreaterThanOrEqual(44);
  await status.click();
  const dialog = page.getByRole('dialog', { name: 'ARC status' });
  await expect(dialog.getByRole('heading', { name: 'ARC status' })).toBeFocused();
  await expect(dialog).toContainText('1,200 tokens');
  await expect(dialog).not.toContainText('Latest completed-step context');
  await expect(dialog).not.toContainText('No local reply stream');
  await expect(dialog).not.toContainText('Changes apply to the next turn');
  await expect(dialog).not.toContainText('Context · stale');
  await expect(dialog).toContainText('week · 72% remaining');
  await expect(dialog).toContainText('stale');
  await page.screenshot({ path: '/tmp/arc-web-parity-desktop-status.png' });
  await expect.poll(() => status.innerText()).toContain('Connected');
  const refresh = dialog.getByRole('button', { name: 'Refresh' });
  await expect(refresh).toBeEnabled();
  await refresh.click();
  await expect.poll(() => status.innerText()).toContain('Connected');
  await page.keyboard.press('Escape');
  await expect(dialog).not.toBeVisible();
  await expect(status).toBeFocused();
  await expect(page.locator('.uncertain-input')).toContainText('Keep near composer');
});

test('allowance warnings stay in the panel and expire when stale', async ({ page }) => {
  const attach = fixtureDaemon();
  attach.setStatus({ allowance: [create(AllowanceWindowSchema, { remainingPercent: 5, windowSeconds: 604800n })] });
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', (socket) => attach(socket));
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  const button = page.getByRole('button', { name: 'ARC status' });
  await expect(button).toContainText('Connected');
  expect(await button.locator('.status-dot').evaluate((dot) => getComputedStyle(dot).backgroundColor)).toBe('rgb(184, 187, 38)');
  await button.click();
  const panel = page.getByRole('dialog', { name: 'ARC status' });
  await expect(panel).toContainText('Low allowance');
  await page.screenshot({ path: '/tmp/arc-web-parity-phone-status.png' });
  expect(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)).toBe(false);
  attach.setStatus({ allowance: [create(AllowanceWindowSchema, { remainingPercent: 5, windowSeconds: 604800n })],
    allowanceObservedAt: BigInt(Math.floor(Date.now() / 1000) - 300) });
  await expect(panel.getByRole('button', { name: 'Refresh' })).toBeEnabled();
  await panel.getByRole('button', { name: 'Refresh' }).click();
  await expect(button).toContainText('Connected');
  await expect(panel).toContainText('stale');
  await expect(panel).not.toContainText('Low allowance');
});

test('status panel states context is unmeasured when no reading is supplied', async ({ page }) => {
  const attach = fixtureDaemon();
  attach.setStatus({ context: undefined, allowance: [] });
  await page.routeWebSocket('ws://127.0.0.1:8787/arc', (socket) => attach(socket));
  await page.goto('/');
  await page.getByRole('button', { name: 'ARC status' }).click();
  const dialog = page.getByRole('dialog', { name: 'ARC status' });
  await expect(dialog).toContainText('Not measured');
  await expect(dialog).toContainText('window unknown');
  await expect(dialog).toContainText('Unavailable');
});

test('phone header leaves clearance below the top safe area', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await page.locator('.top').evaluate((header) => {
    (header as HTMLElement).style.setProperty('--safe-top', '59px');
  });
  const layout = await page.evaluate(() => {
    const header = document.querySelector('.top')!;
    const title = document.querySelector('.title')!;
    return {
      topPadding: getComputedStyle(header).paddingTop,
      opaqueHeader: getComputedStyle(header).backgroundColor === getComputedStyle(document.body).backgroundColor,
      titleBelowInset: title.getBoundingClientRect().top >= 79,
      pageOverflow: document.body.scrollHeight > innerHeight + 1,
    };
  });
  expect(layout).toEqual({
    topPadding: '79px', opaqueHeader: true,
    titleBelowInset: true, pageOverflow: false,
  });
});

test.describe('touch-first navigation', () => {
  test.use({ hasTouch: true });

  for (const width of [390, 844]) {
    test(`sessions open as a sheet without highlighting the first row at ${width}px`, async ({ page }) => {
      const height = width > 700 ? 390 : 844;
      await page.setViewportSize({ width, height });
      await page.goto('/');
      await expect(page.locator('.sidebar')).not.toBeVisible();
      await expect(page.locator('.keyboard-hint')).not.toBeVisible();
      await page.getByRole('button', { name: 'Open sessions' }).tap();
      const dialog = page.getByRole('dialog', { name: 'Sessions', exact: true });
      await expect(dialog.getByRole('heading', { name: 'Sessions', exact: true })).toBeFocused();
      const first = dialog.getByRole('button', { name: /Plan a focused week/ });
      await expect(first).not.toBeFocused();
      expect(await first.evaluate((row) => getComputedStyle(row).outlineStyle)).toBe('none');
      await expect(dialog.locator('.session[aria-current="true"]')).toHaveCount(1);
      const project = dialog.getByRole('combobox', { name: 'Project' });
      expect((await project.boundingBox())!.height).toBeGreaterThanOrEqual(48);
      await page.keyboard.press('Tab');
      await expect(dialog.getByRole('button', { name: 'Close', exact: true })).toBeFocused();
      await page.keyboard.press('Tab');
      await expect(project).toBeFocused();
      expect(await project.evaluate((select) => getComputedStyle(select).outlineStyle)).toBe('solid');
      expect(await project.evaluate((select) => getComputedStyle(select).outlineOffset)).toBe('0px');
      const bounds = (await dialog.locator('.panel-surface').boundingBox())!;
      expect(Math.abs(bounds.y + bounds.height - height)).toBeLessThan(1);
      expect(bounds.width).toBeLessThanOrEqual(width);
      await dialog.getByRole('button', { name: 'Close', exact: true }).tap();
      await expect(page.getByRole('button', { name: 'Open sessions' })).toBeFocused();
    });
  }
});

test('dialog keyboard navigation retains the close-fitting orange focus ring', async ({ page }) => {
  await page.goto('/');
  const settings = page.getByRole('button', { name: 'Settings', exact: true });
  await settings.focus();
  await settings.press('Enter');
  const dialog = page.getByRole('dialog', { name: 'Settings', exact: true });
  await expect(dialog.getByRole('heading', { name: 'Settings', exact: true })).toBeFocused();
  await page.keyboard.press('Tab');
  await expect(dialog.getByRole('button', { name: 'Close', exact: true })).toBeFocused();
  await page.keyboard.press('Tab');
  const host = dialog.locator('.host-select').first();
  await expect(host).toBeFocused();
  expect(await host.evaluate((row) => getComputedStyle(row).outlineStyle)).toBe('solid');
  expect(await host.evaluate((row) => getComputedStyle(row).outlineOffset)).toBe('0px');
  expect(await host.evaluate((row) => getComputedStyle(row).outlineColor)).toBe('rgba(254, 128, 25, 0.7)');
  await page.keyboard.press('Escape');
  await expect(settings).toBeFocused();
});

test('higher contrast and forced colours retain an explicit keyboard focus indicator', async ({ page }) => {
  await page.goto('/');
  const settings = page.getByRole('button', { name: 'Settings', exact: true });
  for (const media of [{ contrast: 'more' as const }, { contrast: 'no-preference' as const, forcedColors: 'active' as const }]) {
    await page.emulateMedia(media);
    await settings.focus();
    expect(await settings.evaluate((button) => getComputedStyle(button).outlineStyle)).toBe('solid');
    expect(await settings.evaluate((button) => getComputedStyle(button).outlineWidth)).toBe('2px');
  }
});

test('navigation controls and sheets use opaque matte surfaces', async ({ page }) => {
  await page.goto('/');
  const font = await page.evaluate(async () => {
    await document.fonts.load('12px "JetBrains Mono"');
    return {
      available: document.fonts.check('12px "JetBrains Mono"'),
      bundled: await fetch('/jetbrains-mono-latin.woff2').then((response) => response.ok),
      precached: (await fetch('/sw.js').then((response) => response.text())).includes('jetbrains-mono-latin.woff2'),
    };
  });
  expect(font).toEqual({ available: true, bundled: true, precached: true });
  const settings = page.getByRole('button', { name: 'Settings', exact: true });
  const buttonStyle = await settings.evaluate((button) => {
    const style = getComputedStyle(button);
    return [style.backgroundColor, style.borderWidth, style.backdropFilter, style.boxShadow];
  });
  expect(buttonStyle).toEqual(['rgb(60, 56, 54)', '1px', 'none', 'none']);
  await settings.click();
  const appearance = await page.locator('.panel-surface').evaluate((surface) => {
    const style = getComputedStyle(surface);
    return {
      backdropFilter: style.backdropFilter,
      backgroundImage: style.backgroundImage,
      backgroundColor: style.backgroundColor,
      boxShadow: style.boxShadow,
    };
  });
  expect(appearance).toEqual({
    backdropFilter: 'none', backgroundImage: 'none',
    backgroundColor: 'rgb(60, 56, 54)', boxShadow: 'none',
  });
});

for (const name of ['Settings', 'Sessions', 'Jobs']) {
  test(`${name} dismisses only when the pointer starts and ends outside`, async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto('/');
    const opener = page.getByRole('button', { name: name === 'Sessions' ? 'Open sessions' : name, exact: true });
    await opener.click();
    const dialog = page.getByRole('dialog', { name, exact: true });
    await expect(dialog).toBeVisible();
    expect(await page.evaluate(() => document.elementFromPoint(8, 8)?.classList.contains('panel-dismiss'))).toBe(true);
    const bounds = (await dialog.locator('.panel-surface').boundingBox())!;
    const inside = { x: bounds.x + 8, y: bounds.y + 50 };
    await page.mouse.click(inside.x, inside.y);
    await expect(dialog).toBeVisible();
    await page.mouse.move(inside.x, inside.y);
    await page.mouse.down();
    await page.mouse.move(8, 8);
    await page.mouse.up();
    await expect(dialog).toBeVisible();
    await page.mouse.move(8, 8);
    await page.mouse.down();
    await page.mouse.move(inside.x, inside.y);
    await page.mouse.up();
    await expect(dialog).toBeVisible();
    await page.mouse.click(8, 8);
    await expect(dialog).not.toBeVisible();
    await expect(opener).toBeFocused();
  });
}

test.describe('touch panel dismissal', () => {
  test.use({ hasTouch: true });

  for (const name of ['Settings', 'Sessions', 'Jobs']) {
    test(`the real backdrop tap target dismisses ${name}`, async ({ page }) => {
      await page.setViewportSize({ width: 390, height: 844 });
      await page.goto('/');
      const opener = page.getByRole('button', { name: name === 'Sessions' ? 'Open sessions' : name, exact: true });
      await opener.tap();
      const dialog = page.getByRole('dialog', { name, exact: true });
      await expect(dialog).toBeVisible();
      const target = page.getByRole('button', { name: 'Dismiss panel', exact: true });
      await expect(target).toHaveAttribute('tabindex', '-1');
      await target.tap({ position: { x: 8, y: 8 } });
      await expect(dialog).not.toBeVisible();
      await expect(opener).toBeFocused();
    });
  }

  test('outside taps dismiss settings without losing an unfinished host form', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto('/');
    const settings = page.getByRole('button', { name: 'Settings', exact: true });
    await settings.tap();
    const dialog = page.getByRole('dialog', { name: 'Settings', exact: true });
    await expect(dialog.getByRole('heading', { name: 'Connection', exact: true })).toBeVisible();
    await expect(dialog.getByRole('heading', { name: 'App', exact: true })).toBeVisible();
    await dialog.getByRole('button', { name: 'Add daemon host', exact: true }).tap();
    await dialog.getByLabel('Name', { exact: true }).fill('Unfinished host');
    await page.touchscreen.tap(8, 8);
    await expect(dialog).not.toBeVisible();
    await settings.tap();
    await expect(dialog.getByLabel('Name', { exact: true })).toHaveValue('Unfinished host');
    await dialog.getByRole('button', { name: 'Cancel', exact: true }).tap();
    await expect(dialog.getByLabel('Name', { exact: true })).not.toBeVisible();
  });
});

test('long session lists scroll independently without moving Jobs or the composer', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 844 });
  await page.goto('/');
  const jobs = page.locator('.job-status');
  const composer = page.locator('.composer');
  const jobsBefore = await jobs.boundingBox();
  const composerBefore = await composer.boundingBox();
  await page.locator('.session-list').evaluate((list) => {
    const row = list.querySelector('button')!;
    for (let i = 0; i < 100; i++) {
      const extra = row.cloneNode(true) as HTMLButtonElement;
      extra.removeAttribute('aria-current');
      extra.querySelector('span')!.textContent = `Session ${i + 4}`;
      list.append(extra);
    }
    list.scrollTop = list.scrollHeight;
  });
  expect(await page.locator('.session-list').evaluate((list) => list.scrollTop)).toBeGreaterThan(1000);
  expect(await jobs.boundingBox()).toEqual(jobsBefore);
  expect(await composer.boundingBox()).toEqual(composerBefore);
  expect(await page.evaluate(() => document.body.scrollHeight > innerHeight + 1)).toBe(false);
  await jobs.click();
  await expect(page.getByRole('dialog', { name: 'Jobs', exact: true })).toBeVisible();
});

test.describe('picker scrolling', () => {
  test.use({ hasTouch: true });

  for (const viewport of [{ width: 390, height: 844 }, { width: 844, height: 390 }]) {
    for (const name of ['Sessions', 'Jobs']) {
      test(`${name} contains touch scrolling at ${viewport.width}px`, async ({ page }) => {
        await page.setViewportSize(viewport);
        await page.goto('/');
        await expect(page.locator('.message')).toHaveCount(3);
        await page.evaluate(() => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))));
        const transcript = page.locator('.transcript');
        await transcript.evaluate((element) => {
          const content = element.querySelector('.transcript-content')!;
          const message = content.querySelector('.message')!;
          for (let i = 0; i < 40; i++) content.append(message.cloneNode(true));
          element.scrollTop = 160;
        });
        const topBefore = await transcript.evaluate((element) => element.scrollTop);
        await page.getByRole('button', { name: name === 'Sessions' ? 'Open sessions' : name, exact: true }).tap();
        const dialog = page.getByRole('dialog', { name, exact: true });
        const list = dialog.locator('.dialog-list');
        await list.evaluate((element) => {
          const row = element.firstElementChild!;
          for (let i = 0; i < 60; i++) element.append(row.cloneNode(true));
        });
        const headingBefore = await dialog.getByRole('heading', { name, exact: true }).boundingBox();
        const bounds = (await list.boundingBox())!;
        expect(bounds.height).toBeGreaterThan(44);
        const x = bounds.x + bounds.width / 2;
        const y = bounds.y + bounds.height - 8;
        const cdp = await page.context().newCDPSession(page);
        expect(await list.evaluate((element) => element.scrollHeight > element.clientHeight)).toBe(true);
        expect(await page.evaluate(({ x, y }) => !!document.elementFromPoint(x, y)?.closest('.dialog-list'), { x, y })).toBe(true);
        await cdp.send('Input.dispatchTouchEvent', { type: 'touchStart', touchPoints: [{ x, y }] });
        for (let step = 1; step <= 8; step++) {
          await cdp.send('Input.dispatchTouchEvent', {
            type: 'touchMove', touchPoints: [{ x, y: y - Math.min(180, bounds.height - 16) * step / 8 }],
          });
          await page.evaluate(() => new Promise(requestAnimationFrame));
        }
        await cdp.send('Input.dispatchTouchEvent', { type: 'touchEnd', touchPoints: [] });
        await expect.poll(() => list.evaluate((element) => element.scrollTop)).toBeGreaterThan(20);
        expect(await transcript.evaluate((element) => element.scrollTop)).toBe(topBefore);
        expect(await dialog.getByRole('heading', { name, exact: true }).boundingBox()).toEqual(headingBefore);
        expect(await transcript.evaluate((element) => getComputedStyle(element).overflowY)).toBe('hidden');

        await list.evaluate((element) => { element.scrollTop = element.scrollHeight; });
        const listEnd = await list.evaluate((element) => element.scrollTop);
        await page.mouse.move(x, y);
        await page.mouse.wheel(0, 600);
        await page.mouse.move(8, 8);
        await page.mouse.wheel(0, 600);
        await expect.poll(async () => Math.abs(await list.evaluate((element) => element.scrollTop) - listEnd)).toBeLessThanOrEqual(1);
        expect(await transcript.evaluate((element) => element.scrollTop)).toBe(topBefore);

        await dialog.getByRole('button', { name: 'Close', exact: true }).tap();
        expect(await transcript.evaluate((element) => element.scrollTop)).toBe(topBefore);
        expect(await transcript.evaluate((element) => getComputedStyle(element).overflowY)).toBe('auto');
        await transcript.hover();
        await page.mouse.wheel(0, 200);
        await expect.poll(() => transcript.evaluate((element) => element.scrollTop)).toBeGreaterThan(topBefore);
      });
    }
  }
});

test('drafts survive session switching and reload', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await composer.fill('First session draft\nSecond line');
  await page.getByRole('button', { name: 'Open sessions' }).click();
  await page.getByRole('dialog').getByRole('button', { name: /Notes on a first draft/ }).click();
  await expect(composer).toHaveValue('');
  await composer.fill('Writing session draft');
  await page.reload();
  await expect(composer).toHaveValue('Writing session draft');
  await page.getByRole('button', { name: 'Open sessions' }).click();
  await page.getByRole('dialog').getByRole('button', { name: /Plan a focused week/ }).click();
  await expect(composer).toHaveValue('First session draft\nSecond line');
});

test('Enter sends, Shift+Enter inserts a newline, and IME Enter does not send', async ({ page }) => {
  await page.goto('/');
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await composer.fill('First line');
  await composer.press('End');
  await composer.press('Shift+Enter');
  await composer.pressSequentially('Second line');
  await expect(composer).toHaveValue('First line\nSecond line');
  await expect(page.locator('.message')).toHaveCount(3);
  await composer.evaluate((element) => {
    element.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', isComposing: true, bubbles: true }));
    element.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', keyCode: 229, bubbles: true }));
  });
  await expect(composer).toHaveValue('First line\nSecond line');
  await expect(page.locator('.message')).toHaveCount(3);
  await composer.press('Enter');
  await expect(composer).toHaveValue('');
  await expect(page.locator('.message')).toHaveCount(5);
  await expect(page.locator('.message').nth(3)).toContainText('First line\nSecond line');
});

test('composer grows with drafts and restores its height after reload', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  const send = page.getByRole('button', { name: 'Send', exact: true });
  await expect(send).toBeDisabled();
  const emptyHeight = (await composer.boundingBox())!.height;
  await composer.fill('A multiline draft\n'.repeat(12));
  await expect(send).toBeEnabled();
  await expect.poll(async () => (await composer.boundingBox())!.height).toBe(180);
  await page.reload();
  await expect(composer).toHaveValue('A multiline draft\n'.repeat(12));
  await expect.poll(async () => (await composer.boundingBox())!.height).toBe(180);
  await composer.fill('');
  await expect.poll(async () => (await composer.boundingBox())!.height).toBe(emptyHeight);
  await expect(send).toBeDisabled();
  await expect(page.locator('.keyboard-hint')).not.toBeVisible();
});

test('tool rows support keyboard disclosure and contain long output', async ({ page }) => {
  await page.setViewportSize({ width: 320, height: 844 });
  await page.goto('/');
  const tool = page.locator('.tool').first();
  const summary = tool.locator('summary');
  const output = tool.locator('.tool-output');
  await expect(output).not.toBeVisible();
  const target = (await summary.boundingBox())!;
  expect(target.height).toBeGreaterThanOrEqual(44);
  await summary.focus();
  await summary.press('Enter');
  await expect(tool).toHaveAttribute('open', '');
  await expect(output).toContainText('Two afternoons are already committed');
  await output.evaluate((element) => {
    element.textContent = ('A long tool result ' + 'x'.repeat(200) + '\n').repeat(50);
  });
  expect((await output.boundingBox())!.height).toBeLessThanOrEqual(240);
  expect(await output.evaluate((element) => element.scrollHeight > element.clientHeight)).toBe(true);
  expect(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)).toBe(false);
  await summary.press('Space');
  await expect(tool).not.toHaveAttribute('open', '');
  await expect(output).not.toBeVisible();
});

test('streaming accepts input without blocking the next draft', async ({ page }) => {
  await page.goto('/');
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await composer.fill('Help me make a plan '.repeat(20));
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  await expect(composer).toHaveValue('');
  await composer.fill('Next draft while the reply streams');
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeEnabled();
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeEnabled({ timeout: 5000 });
  await expect(composer).toHaveValue('Next draft while the reply streams');
  await expect(page.locator('.message').last()).toContainText('I’ll help you work through:');
  await page.reload();
  await expect(composer).toHaveValue('Next draft while the reply streams');
});

test('reading older content is not interrupted by streaming or session switching', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await page.getByRole('textbox', { name: 'Message to ARC' }).fill(
    'A long paragraph to exercise conversation scrolling and streaming.\n'.repeat(45),
  );
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  const transcript = page.locator('.transcript');
  await expect.poll(() => transcript.evaluate((element) => element.scrollTop)).toBeGreaterThan(200);
  await transcript.evaluate((element) => { element.scrollTop = 0; });
  await expect(page.getByRole('button', { name: 'Jump to latest' })).toBeVisible();
  await page.waitForTimeout(120);
  expect(await transcript.evaluate((element) => element.scrollTop)).toBe(0);
  await page.getByRole('button', { name: 'Open sessions' }).click();
  await page.getByRole('dialog').getByRole('button', { name: /Notes on a first draft/ }).click();
  await page.getByRole('button', { name: 'Open sessions' }).click();
  await page.getByRole('dialog').getByRole('button', { name: /Plan a focused week/ }).click();
  await expect(page.getByRole('button', { name: 'Jump to latest' })).toBeVisible();
  expect(await transcript.evaluate((element) => element.scrollTop)).toBe(0);
  const jump = page.getByRole('button', { name: 'Jump to latest' });
  const target = await jump.boundingBox();
  const visibleMark = await jump.locator('span').boundingBox();
  expect(target?.width).toBe(44);
  expect(target?.height).toBe(44);
  expect(visibleMark?.width).toBe(28);
  expect(visibleMark?.height).toBe(28);
  await jump.click();
  await expect(jump).not.toBeVisible();
  await expect.poll(() => transcript.evaluate((element) => element.scrollTop)).toBeGreaterThan(200);
});

test('unreachable daemon host cannot send and preserves its draft', async ({ page }) => {
  await page.goto('/');
  await page.getByRole('button', { name: 'Settings', exact: true }).click();
  const dialog = page.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Add daemon host' }).click();
  await dialog.getByLabel('Name', { exact: true }).fill('Erebor');
  await dialog.getByLabel('Endpoint', { exact: true }).fill('https://erebor.example/ws');
  await dialog.getByRole('button', { name: 'Add host', exact: true }).click();
  await expect(dialog).toContainText('Use wss://');
  await dialog.getByLabel('Endpoint', { exact: true }).fill('wss://erebor.example/ws');
  await dialog.getByRole('button', { name: 'Add host', exact: true }).click();
  await dialog.getByRole('button', { name: 'Close', exact: true }).click();
  await expect(page.locator('.status')).toContainText('Erebor');
  await expect(page.locator('.status')).toContainText('Disconnected');
  await expect(page.locator('.message')).toHaveCount(0);
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
  await expect(composer).toHaveAttribute('placeholder', 'Message ARC…');
  await composer.fill('Offline host draft');
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeDisabled();
  await page.reload();
  await expect(composer).toHaveValue('Offline host draft');
  await expect(page.locator('.message')).toHaveCount(0);
});

test('message text never executes HTML', async ({ page }) => {
  await page.goto('/');
  const payload = '<img src=x onerror="window.arcInjected=true"><script>window.arcInjected=true</script>';
  await page.getByRole('textbox', { name: 'Message to ARC' }).fill(payload);
  await page.getByRole('button', { name: 'Send', exact: true }).click();
  await expect(page.locator('.message').filter({ hasText: payload }).first()).toBeVisible();
  expect(await page.evaluate(() => Reflect.get(window, 'arcInjected'))).toBeUndefined();
  await expect(page.locator('.message img, .message script')).toHaveCount(0);
});

test('production PWA caches its shell and restores drafts offline', async ({ page, context }) => {
  await page.goto('/');
  const manifest = await (await page.request.get('/manifest.webmanifest')).json();
  expect(manifest.display).toBe('standalone');
  expect(manifest.icons.some((icon: { purpose: string }) => icon.purpose === 'maskable')).toBe(true);
  for (const icon of manifest.icons) {
    const response = await page.request.get(icon.src);
    expect(response.ok()).toBe(true);
    expect(response.headers()['content-type']).toContain('image/png');
  }
  await page.getByRole('textbox', { name: 'Message to ARC' }).fill('Keep this offline draft');
  await page.evaluate(() => navigator.serviceWorker.ready.then(() => undefined));
  await page.reload();
  await expect.poll(() => page.evaluate(() => Boolean(navigator.serviceWorker.controller))).toBe(true);
  await context.setOffline(true);
  await page.reload();
  await expect(page.getByRole('textbox', { name: 'Message to ARC' })).toHaveValue('Keep this offline draft');
  await expect(page.locator('.message')).toHaveCount(3);
  await expect(page.locator('.main > .notice')).toHaveCount(0);
});
