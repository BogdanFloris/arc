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

  const marks = await page.evaluate(async (mark) => {
    const load = (src: string) => new Promise<HTMLImageElement>((resolve, reject) => {
      const image = new Image();
      image.onload = () => resolve(image);
      image.onerror = () => reject(new Error(`Could not load ${src}`));
      image.src = src;
    });

    const scan = (image: HTMLImageElement, size: number) => {
      const canvas = document.createElement('canvas');
      canvas.width = size;
      canvas.height = size;
      const context = canvas.getContext('2d')!;
      context.drawImage(image, 0, 0, size, size);
      const { data } = context.getImageData(0, 0, size, size);
      const corner = (x: number, y: number) => [...context.getImageData(x, y, 1, 1).data];
      let minX = size, minY = size, maxX = -1, maxY = -1, maxRadius = 0;
      for (let y = 0; y < size; y++) {
        for (let x = 0; x < size; x++) {
          const pixel = (y * size + x) * 4;
          if (data[pixel] !== 254 || data[pixel + 1] !== 128 || data[pixel + 2] !== 25 || data[pixel + 3] !== 255) continue;
          minX = Math.min(minX, x); maxX = Math.max(maxX, x);
          minY = Math.min(minY, y); maxY = Math.max(maxY, y);
          maxRadius = Math.max(maxRadius, Math.hypot(x + 0.5 - size / 2, y + 0.5 - size / 2));
        }
      }
      return {
        corners: [corner(0, 0), corner(size - 1, 0), corner(0, size - 1), corner(size - 1, size - 1)],
        center: corner(Math.floor(size / 2), Math.floor(size / 2)),
        mark: {
          widthPct: (maxX - minX + 1) / size * 100,
          heightPct: (maxY - minY + 1) / size * 100,
          leftPadPct: minX / size * 100,
          rightPadPct: (size - 1 - maxX) / size * 100,
          topPadPct: minY / size * 100,
          bottomPadPct: (size - 1 - maxY) / size * 100,
          maxRadiusPct: maxRadius / size * 100,
        },
      };
    };

    const paths = [
      mark!,
      '/icons/apple-touch-icon.png',
      '/icons/icon-192.png',
      '/icons/icon-512.png',
      '/icons/icon-maskable-512.png',
    ];
    const icons = await Promise.all(paths.map(async (src) => {
      const image = await load(src);
      return { src, width: image.naturalWidth, height: image.naturalHeight, ...scan(image, image.naturalWidth) };
    }));
    return { icons, sidebar: scan(await load(mark!), 44) };
  }, iconHref);
  expect(marks.icons.map(({ src, width, height }) => [src, width, height])).toEqual([
    [iconHref, 256, 256],
    ['/icons/apple-touch-icon.png', 180, 180],
    ['/icons/icon-192.png', 192, 192],
    ['/icons/icon-512.png', 512, 512],
    ['/icons/icon-maskable-512.png', 512, 512],
  ]);
  const framedMark = marks.icons[0].mark;
  expect(framedMark.widthPct).toBeGreaterThan(70);
  expect(framedMark.widthPct).toBeLessThan(76);
  for (const icon of marks.icons) {
    const container = icon.src === iconHref ? [0, 0, 0, 0] : [255, 255, 255, 255];
    expect(icon.corners).toEqual([container, container, container, container]);
    expect(icon.center).toEqual([254, 128, 25, 255]);
    expect(Math.abs(icon.mark.leftPadPct - icon.mark.rightPadPct)).toBeLessThan(1);
    expect(Math.abs(icon.mark.topPadPct - icon.mark.bottomPadPct)).toBeLessThan(1);
    expect(Math.abs(icon.mark.widthPct - framedMark.widthPct)).toBeLessThan(1.5);
    // Maskable artwork stays inside the central 80% circle.
    expect(icon.mark.maxRadiusPct).toBeLessThan(40);
  }
  expect(marks.sidebar.center).toEqual([254, 128, 25, 255]);
  expect(marks.sidebar.mark.widthPct).toBeGreaterThan(66);
  expect(marks.sidebar.mark.leftPadPct).toBeGreaterThan(5);
  expect(marks.sidebar.mark.rightPadPct).toBeGreaterThan(5);
});

test('the brand stays out of the phone conversation layout', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto('/');
  await expect(page.locator('.brand')).not.toBeVisible();
  await expect(page.locator('.top img')).toHaveCount(0);
});
