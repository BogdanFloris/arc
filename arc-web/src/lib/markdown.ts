import MarkdownIt from 'markdown-it';
import hljs from 'highlight.js/lib/core';
import rust from 'highlight.js/lib/languages/rust';
import c from 'highlight.js/lib/languages/c';
import cpp from 'highlight.js/lib/languages/cpp';
import python from 'highlight.js/lib/languages/python';
import bash from 'highlight.js/lib/languages/bash';
import javascript from 'highlight.js/lib/languages/javascript';
import typescript from 'highlight.js/lib/languages/typescript';
import kotlin from 'highlight.js/lib/languages/kotlin';
import java from 'highlight.js/lib/languages/java';
import json from 'highlight.js/lib/languages/json';
import ini from 'highlight.js/lib/languages/ini';
import yaml from 'highlight.js/lib/languages/yaml';

for (const [name, language] of Object.entries({
  rust, c, cpp, python, bash, javascript, typescript, kotlin, java, json, ini, yaml,
})) hljs.registerLanguage(name, language);

const aliases = new Map(Object.entries({
  rs: 'rust', cxx: 'cpp', cc: 'cpp', h: 'c', hpp: 'cpp', py: 'python',
  sh: 'bash', shell: 'bash', zsh: 'bash', js: 'javascript', jsx: 'javascript',
  ts: 'typescript', tsx: 'typescript', kt: 'kotlin', kts: 'kotlin',
  properties: 'ini', toml: 'ini', yml: 'yaml',
}));

const md = new MarkdownIt({
  html: false,
  breaks: true,
  linkify: true,
});
md.validateLink = (url: string) => {
  try {
    const parsed = new URL(url);
    return ['http:', 'https:', 'mailto:'].includes(parsed.protocol);
  } catch {
    return false;
  }
};

md.renderer.rules.link_open = (tokens, index, options, _env, self) => {
  tokens[index].attrSet('target', '_blank');
  tokens[index].attrSet('rel', 'noopener noreferrer');
  return self.renderToken(tokens, index, options);
};
md.renderer.rules.image = (tokens, index) => {
  return md.utils.escapeHtml(tokens[index].content);
};
md.renderer.rules.table_open = () => '<div class="markdown-table" tabindex="0" role="region" aria-label="Table"><table>';
md.renderer.rules.table_close = () => '</table></div>';
md.renderer.rules.fence = (tokens, index) => {
  const token = tokens[index];
  const info = token.info.trim().split(/[\s,]+/)[0].toLowerCase();
  const language = aliases.get(info) ?? info;
  const highlighted = !!language && !!hljs.getLanguage(language);
  const code = highlighted
    ? hljs.highlight(token.content, { language, ignoreIllegals: true }).value
    : md.utils.escapeHtml(token.content);
  const className = highlighted ? ` class="hljs language-${md.utils.escapeHtml(language)}"` : '';
  return `<pre tabindex="0"><code${className}>${code}</code></pre>\n`;
};
md.renderer.rules.code_block = (tokens, index) =>
  `<pre tabindex="0"><code>${md.utils.escapeHtml(tokens[index].content)}</code></pre>\n`;

export function renderMarkdown(content: string): string {
  return md.render(content);
}
