import { describe, expect, it } from 'vitest';
import { renderToString } from 'react-dom/server';
import { Sidebar } from './Sidebar';
import { model } from '../test/fixture';
import { renderPage } from '../test/render';

describe('Sidebar', () => {
  it('exposes tabs and branch state to assistive technology', () => {
    const html = renderToString(<Sidebar model={model} />).replace(/<!-- -->/g, '');
    expect(html).toContain('role="tablist"');
    expect(html).toMatch(/role="tab"[^>]*aria-selected="true"[^>]*>Project/);
    expect(html).toMatch(/role="tab"[^>]*aria-selected="false"[^>]*>Database/);
    expect(html).toMatch(/aria-expanded="true"[^>]*>[^<]*Load/);
    expect(html).toMatch(/aria-expanded="false"[^>]*>[^<]*DAGs/);
  });

  it('expands the branches leading to the current node', () => {
    localStorage.clear();
    const { container } = renderPage(<Sidebar model={model} currentId="transform:kpi.revenue" />);
    const current = container.querySelector('a.leaf.current');
    expect(current?.textContent).toBe('revenue');
  });
});
