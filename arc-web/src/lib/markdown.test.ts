import { describe, expect, it } from 'vitest';
import { renderMarkdown } from './markdown';

describe('renderMarkdown', () => {
  it('renders common structures and preserves soft line breaks', () => {
    const html = renderMarkdown('# Hello\n\none\ntwo\n\n> quote\n\n- item\n\n| a | b |\n| - | - |\n| x | y |');
    expect(html).toContain('<h1>Hello</h1>');
    expect(html).toContain('one<br>\ntwo');
    expect(html).toContain('<blockquote>');
    expect(html).toContain('<ul>');
    expect(html).toContain('class="markdown-table" tabindex="0" role="region" aria-label="Table"');
  });

  it('highlights known languages and escapes unknown fences', () => {
    const html = renderMarkdown('```rs\nfn main() {}\n```\n\n```unknown\n<script>\n```');
    expect(html).toContain('class="hljs language-rust"');
    expect(html).toContain('hljs-keyword');
    expect(html).toContain('&lt;script&gt;');
    expect(html).not.toContain('<script>');
  });

  it('escapes raw HTML and rejects unsafe links, titles, and fence attributes', () => {
    const html = renderMarkdown('<img src=x onerror=alert(1)>\n\n[x](javascript:alert%281%29 " onmouseover=x")\n\n```js something=\" onclick=alert(1)\nconst x = 1;\n```');
    expect(html).toContain('&lt;img');
    expect(html).not.toContain('<img');
    expect(html).not.toContain('href="javascript:');
    expect(html).not.toContain('<a ');
    expect(html).toContain('<pre tabindex="0">');
    expect(html).not.toContain(' onclick=');
  });

  it('does not load images and renders unfinished fences safely', () => {
    const html = renderMarkdown('![useful alt](https://example.com/image.png "title")\n\n```html\n<b>unfinished');
    expect(html).toContain('useful alt');
    expect(html).not.toContain('<img');
    expect(html).not.toContain('https://example.com/image.png');
    expect(html).toContain('&lt;b&gt;unfinished');
  });

  it('opens allowed absolute links safely, including encoded paths', () => {
    const html = renderMarkdown('[path](https://example.com/a%20b)\n\n[web](http://example.com)\n\n[email](mailto:me@example.com)');
    expect(html).toContain('href="https://example.com/a%20b"');
    expect(html).toContain('href="http://example.com"');
    expect(html).toContain('href="mailto:me@example.com"');
    expect(html.match(/target="_blank" rel="noopener noreferrer"/g)).toHaveLength(3);
    for (const url of ['file:///etc/passwd', 'data:text/html,hello', '//example.com', '/relative', 'jav&#x61;script:alert(1)']) {
      expect(renderMarkdown(`[unsafe](${url})`)).not.toContain('<a ');
    }
  });

  it('treats prototype names as unknown fence languages', () => {
    for (const language of ['constructor', '__proto__', 'toString']) {
      const html = renderMarkdown(`\`\`\`${language}\n<b>plain</b>\n\`\`\``);
      expect(html).toContain('&lt;b&gt;plain&lt;/b&gt;');
      expect(html).not.toContain('class="hljs');
    }
  });

  it('preserves and escapes indented code in a focusable block', () => {
    const html = renderMarkdown('    <b>literal</b>\n        four more spaces');
    expect(html).toContain('<pre tabindex="0"><code>&lt;b&gt;literal&lt;/b&gt;\n    four more spaces');
  });
});
