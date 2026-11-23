import { describe, expect, it } from 'vitest';
import { renderPage } from '../test/render';
import { renderToString } from 'react-dom/server';
import { NodePage } from './NodePage';
import { buildGraph } from '../graph/graph';
import { model, stats } from '../test/fixture';
import type { SiteModel, SiteStats } from '../types';

const ID = 'load:sales.customers';
const renamed: SiteModel = structuredClone(model);
renamed.nodes[ID].columns = [{ name: 'cust_id', rename: 'customer_id', type: 'long' } as never];
const graph = buildGraph(renamed);
const render = (s: SiteStats | null) =>
  renderToString(<NodePage model={renamed} stats={s} graph={graph} node={renamed.nodes[ID]} />);

describe('NodePage', () => {
  it('has no Actual column without stats', () => {
    expect(render(null)).not.toContain('<th>Actual</th>');
  });

  it('marks a renamed column whose diff entry is a mismatch', () => {
    const s: SiteStats = structuredClone(stats);
    s.tables![ID].columnDiff = [{ name: 'customer_id', declared: 'BIGINT', actual: 'TEXT', status: 'mismatch' }];
    const html = render(s);
    expect(html).toContain('<th>Actual</th>');
    expect(html).toContain('class="marker"');
  });

  it('notes connections whose stats are unsupported', () => {
    const s: SiteStats = structuredClone(stats);
    s.connections!.duckdb = { engine: 'duckdb', status: 'unsupported', durationMs: 1 };
    expect(render(s)).toContain('stats not supported for this engine');
  });

  it('looks up the connection error under the stats connectionRef, not the node one', () => {
    const s: SiteStats = structuredClone(stats);
    s.tables![ID].connectionRef = 'sinkconn';
    s.connections!.sinkconn = { engine: 'duckdb', status: 'error', error: 'sink boom', durationMs: 1 };
    s.connections![renamed.nodes[ID].connectionRef ?? 'x'] = { engine: 'duckdb', status: 'ok', durationMs: 1 };
    expect(render(s).replace(/<!-- -->/g, '')).toContain('stats unavailable: sink boom');
  });

  it('shows an ignored pill and no diff marker for an ignored column', () => {
    const m: SiteModel = structuredClone(renamed);
    m.nodes[ID].columns = [{ name: 'cust_id', type: 'long', ignore: true } as never];
    const s: SiteStats = structuredClone(stats);
    s.tables![ID].columnDiff = [{ name: 'cust_id', declared: 'BIGINT', actual: 'TEXT', status: 'mismatch' }];
    const html = renderToString(<NodePage model={m} stats={s} graph={buildGraph(m)} node={m.nodes[ID]} />);
    expect(html).toContain('ignored');
    expect(html).not.toContain('class="marker"');
  });

  it('shows the source file once', () => {
    const html = render(null).replace(/<!-- -->/g, '');
    expect(html.split('metadata/load/sales/customers.sl.yml').length - 1).toBe(1);
  });

  it('renders row level policy descriptions as Markdown', () => {
    const m: SiteModel = structuredClone(renamed);
    m.nodes[ID].rls = [{ name: 'eu', predicate: "region = 'EU'", description: 'Only **EU** rows' }];
    const html = renderToString(<NodePage model={m} stats={null} graph={graph} node={m.nodes[ID]} />);
    expect(html).toContain('<strong>EU</strong>');
  });

  it('renders duplicate warnings without a duplicate key error', () => {
    const m: SiteModel = structuredClone(renamed);
    m.nodes[ID].warnings = ['same', 'same'];
    const { container, errors } = renderPage(<NodePage model={m} stats={null} graph={graph} node={m.nodes[ID]} />);
    expect(container.querySelectorAll('.warnings li')).toHaveLength(2);
    expect(errors().filter((e) => e.includes('same key'))).toEqual([]);
  });
});
