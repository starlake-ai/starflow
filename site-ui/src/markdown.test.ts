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

  it('returns an empty string for missing input', () => {
    expect(renderMarkdown(null)).toBe('');
    expect(renderMarkdown(undefined)).toBe('');
  });
});
