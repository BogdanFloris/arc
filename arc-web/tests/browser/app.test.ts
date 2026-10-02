import { expect, test } from '@playwright/test';
import { fixtureDaemon } from '../fixtures/daemon';

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
      const bounds = (await dialog.locator('.panel-surface').boundingBox())!;
      expect(Math.abs(bounds.y + bounds.height - height)).toBeLessThan(1);
      expect(bounds.width).toBeLessThanOrEqual(width);
      await dialog.getByRole('button', { name: 'Close', exact: true }).tap();
      await expect(page.getByRole('button', { name: 'Open sessions' })).toBeFocused();
    });
  }
});

test('dialog keyboard navigation retains visible focus rings', async ({ page }) => {
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
  await page.keyboard.press('Escape');
  await expect(settings).toBeFocused();
});

test('glass has an opaque higher-contrast fallback', async ({ page }) => {
  await page.goto('/');
  const settings = page.getByRole('button', { name: 'Settings', exact: true });
  expect(await settings.evaluate((button) => getComputedStyle(button).backdropFilter)).not.toBe('none');
  await page.emulateMedia({ contrast: 'more' });
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
  await expect(page.getByRole('button', { name: 'Send', exact: true })).toBeDisabled();
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
  await expect(page.locator('.status')).toContainText('Unavailable');
  await expect(page.locator('.message')).toHaveCount(0);
  const composer = page.getByRole('textbox', { name: 'Message to ARC' });
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
