import { expect, test } from '@playwright/test';

test('ARC branding and installed-app metadata are present', async ({ page }) => {
  await page.setViewportSize({ width: 1100, height: 840 });
  await page.goto('/');

  await expect(page).toHaveTitle('ARC');
  await expect(page.locator('.brand')).toHaveText('');
  await expect(page.locator('.brand').getByRole('img', { name: 'ARC', exact: true })).toBeVisible();
  await expect(page.locator('meta[name="apple-mobile-web-app-title"]'))
    .toHaveAttribute('content', 'ARC');

  const manifestHref = await page.locator('link[rel="manifest"]').getAttribute('href');
  expect(manifestHref).toBeTruthy();
  const manifest = await page.evaluate(async (href) => {
    const response = await fetch(href!);
    return response.json();
  }, manifestHref);
  expect(manifest).toMatchObject({
    id: '/',
    name: 'ARC',
    short_name: 'ARC',
    start_url: '/',
    scope: '/',
  });

  const iconHref = await page.locator('link[rel="icon"]').getAttribute('href');
  expect(iconHref).toBeTruthy();
  const artwork = await page.evaluate(async (href) => {
    const response = await fetch(href!);
    return response.text();
  }, iconHref);
  expect(artwork).toContain('<title id="arc-title">ARC</title>');
  expect(artwork).not.toContain('fill="#fff"');
  expect(artwork).toContain('fill="#fe8019"');

  const iconSizes = await page.evaluate(async (mark) => {
    const paths = [
      mark!,
      '/icons/apple-touch-icon.png',
      '/icons/icon-192.png',
      '/icons/icon-512.png',
      '/icons/icon-maskable-512.png',
    ];
    return Promise.all(paths.map((src) => new Promise<{ src: string; width: number; height: number; background: number[]; mark: number[] }>((resolve, reject) => {
      const image = new Image();
      image.onload = () => {
        const canvas = document.createElement('canvas');
        canvas.width = image.naturalWidth;
        canvas.height = image.naturalHeight;
        const context = canvas.getContext('2d')!;
        context.drawImage(image, 0, 0);
        resolve({
          src, width: canvas.width, height: canvas.height,
          background: [...context.getImageData(0, 0, 1, 1).data],
          mark: [...context.getImageData(Math.floor(canvas.width / 2), Math.floor(canvas.height / 2), 1, 1).data],
        });
      };
      image.onerror = () => reject(new Error(`Could not load ${src}`));
      image.src = src;
    })));
  }, iconHref);
  expect(iconSizes.map(({ src, width, height }) => [src, width, height])).toEqual([
    [iconHref, 256, 256],
    ['/icons/apple-touch-icon.png', 180, 180],
    ['/icons/icon-192.png', 192, 192],
    ['/icons/icon-512.png', 512, 512],
    ['/icons/icon-maskable-512.png', 512, 512],
  ]);
  for (const icon of iconSizes) {
    expect(icon.background).toEqual(icon.src === iconHref ? [0, 0, 0, 0] : [255, 255, 255, 255]);
    expect(icon.mark).toEqual([254, 128, 25, 255]);
  }
});

test('the brand stays out of the phone conversation layout', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await expect(page.locator('.brand')).not.toBeVisible();
  await expect(page.locator('.top img')).toHaveCount(0);
});
