import { describe, expect, it } from 'vitest';
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
});
