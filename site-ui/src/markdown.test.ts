import { describe, expect, it } from 'vitest';
import { renderMarkdown } from './markdown';

describe('renderMarkdown', () => {
  it('renders Markdown', () => {
    expect(renderMarkdown('**Customers** master data')).toContain('<strong>Customers</strong>');
    expect(renderMarkdown('| a |\n|---|\n| 1 |')).toContain('<table>');
  });

  it('strips scripts and event handlers', () => {
    const html = renderMarkdown('<script>alert(1)</script><img src="x" onerror="alert(1)">text');
    expect(html).not.toContain('<script');
    expect(html).not.toContain('onerror');
    expect(html).toContain('text');
  });

  it('strips javascript: links', () => {
    expect(renderMarkdown('<a href="javascript:alert(1)">x</a>')).not.toMatch(/href="javascript:/);
    expect(renderMarkdown('[x](javascript:alert(1))')).not.toMatch(/href="javascript:/);
  });

  it('keeps unresolved doc references visible as text', () => {
    expect(renderMarkdown('See {{ doc("customers") }} here')).toContain('{{ doc("customers") }}');
  });

  it('removes style tags, style attributes and form controls', () => {
    const html = renderMarkdown(
      '<style>body{display:none}</style><p style="color:red">x</p><form><input name="a"><button>b</button></form>',
    );
    expect(html).not.toContain('<style');
    expect(html).not.toContain('style=');
    expect(html).not.toContain('<form');
    expect(html).not.toContain('<input');
    expect(html).not.toContain('<button');
    expect(html).toContain('x');
  });

  it('drops remote images and keeps data: and relative ones', () => {
    expect(renderMarkdown('![a](https://example.com/a.png)')).not.toContain('<img');
    expect(renderMarkdown('<img src="http://example.com/a.png">')).not.toContain('<img');
    expect(renderMarkdown('<img src="//example.com/a.png">')).not.toContain('<img');
    expect(renderMarkdown('![a](data:image/png;base64,iVBORw0KGgo=)')).toContain('src="data:image/png;base64,iVBORw0KGgo="');
    expect(renderMarkdown('![a](img/diagram.png)')).toContain('src="img/diagram.png"');
    expect(renderMarkdown('<img src="a.png" srcset="https://example.com/b.png 2x">')).not.toContain('srcset');
  });

  it('opens external links in a new tab without opener', () => {
    const html = renderMarkdown('[site](https://starlake.ai)');
    expect(html).toContain('target="_blank"');
    expect(html).toContain('rel="noopener noreferrer"');
  });

  it('leaves internal hash links untouched', () => {
    const html = renderMarkdown('[c](#/node/load%3Asales.customers)');
    expect(html).toContain('href="#/node/load%3Asales.customers"');
    expect(html).not.toContain('target=');
    expect(html).not.toContain('rel=');
  });

  it('neutralizes data: links', () => {
    expect(renderMarkdown('<a href="data:text/html,<script>alert(1)</script>">x</a>')).not.toMatch(/href="data:/);
    expect(renderMarkdown('[x](data:text/html;base64,PHNjcmlwdD5hbGVydCgxKTwvc2NyaXB0Pg==)')).not.toMatch(/href="data:/);
  });

  it('removes svg and math subtrees, including remote image references', () => {
    const cases = [
      '<svg><image href="https://e.com/a.png"/></svg>',
      '<svg xmlns:xlink="http://www.w3.org/1999/xlink"><image xlink:href="https://e.com/a.png"/></svg>',
      '<svg><filter><feImage href="https://e.com/a.png"/></filter></svg>',
      '<svg><filter><feImage xlink:href="https://e.com/a.png"/></filter></svg>',
      '<math><mi xlink:href="https://e.com/a">x</mi></math>',
    ];
    for (const c of cases) {
      const html = renderMarkdown(c + 'after');
      expect(html).not.toMatch(/<svg|<image|<feimage|<math|e\.com/i);
      expect(html).toContain('after');
    }
  });

  it('removes remote-loading attributes', () => {
    expect(renderMarkdown('<table background="https://e.com/a.png"><tr><td>x</td></tr></table>')).not.toMatch(/background|e\.com/);
    expect(renderMarkdown('<p poster="https://e.com/a.png">x</p>')).not.toMatch(/poster|e\.com/);
    expect(renderMarkdown('<a href="#a" ping="https://e.com/p">x</a>')).not.toMatch(/ping|e\.com/);
    expect(renderMarkdown('<p formaction="https://e.com/p">x</p>')).not.toMatch(/formaction|e\.com/);
    expect(renderMarkdown('<a xlink:href="https://e.com/p">x</a>')).not.toMatch(/xlink|e\.com/);
  });

  it('still renders ordinary Markdown', () => {
    const html = renderMarkdown(
      '# Title\n\n- one\n- two\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n`code`\n\n```\nblock\n```\n\n[in](#/node/x) [out](https://starlake.ai)\n\n![d](data:image/png;base64,iVBORw0KGgo=) ![r](img/a.png)',
    );
    expect(html).toContain('<h1>Title</h1>');
    expect(html).toContain('<li>one</li>');
    expect(html).toContain('<table>');
    expect(html).toContain('<code>code</code>');
    expect(html).toContain('<pre>');
    expect(html).toContain('href="#/node/x"');
    expect(html).toContain('href="https://starlake.ai"');
    expect(html).toContain('src="data:image/png;base64,iVBORw0KGgo="');
    expect(html).toContain('src="img/a.png"');
  });

  it('returns an empty string for missing input', () => {
    expect(renderMarkdown(null)).toBe('');
    expect(renderMarkdown(undefined)).toBe('');
  });
});
