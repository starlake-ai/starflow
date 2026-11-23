import { describe, expect, it } from 'vitest';
import { buildGraph } from './graph';
import { select, selectWithExclude } from './selectors';
import { model } from '../test/fixture';

const g = buildGraph(model);
const ids = (text: string) => [...select(model, g, text).ids].sort();
const C = 'load:sales.customers';
const O = 'load:sales.orders';
const R = 'transform:kpi.revenue';
const T = 'transform:kpi.top_customers';
const F = 'external:refdata.fx_rates';

describe('select', () => {
  it('selects everything when empty', () => expect(ids('  ')).toEqual([F, C, O, R, T]));

  it('matches by bare table name, domain.table and id', () => {
    expect(ids('revenue')).toEqual([R]);
    expect(ids('KPI.Revenue')).toEqual([R]);
    expect(ids('transform:kpi.revenue')).toEqual([R]);
  });

  it('expands ancestors and descendants', () => {
    expect(ids('+revenue')).toEqual([F, C, O, R]);
    expect(ids('revenue+')).toEqual([R, T]);
    expect(ids('1+top_customers')).toEqual([R, T]);
    expect(ids('2+top_customers')).toEqual([F, C, O, R, T]);
    expect(ids('orders+1')).toEqual([O, R]);
  });

  it('supports @ (descendants and all their ancestors)', () => expect(ids('@orders')).toEqual([F, C, O, R, T]));

  it('ignores fk edges', () => expect(ids('+customers')).toEqual([C]));

  it('supports methods', () => {
    expect(ids('tag:pii')).toEqual([C]);
    expect(ids('domain:sales')).toEqual([C, O]);
    expect(ids('job:kpi')).toEqual([R, T]);
    expect(ids('dag:airflow_transform')).toEqual([R, T]);
    expect(ids('kind:external')).toEqual([F]);
    expect(ids('engine:duckdb')).toEqual([C, O]);
  });

  it('applies graph operators to methods', () => expect(ids('tag:finance+')).toEqual([R, T]));

  it('unions on spaces and intersects on commas', () => {
    expect(ids('tag:pii tag:finance')).toEqual([C, R]);
    expect(ids('domain:sales,tag:pii')).toEqual([C]);
    expect(ids('+revenue,kind:load')).toEqual([C, O]);
  });

  it('reports errors', () => {
    expect(select(model, g, 'foo:bar').errors).toEqual(["Unknown selector method 'foo'"]);
    expect(select(model, g, 'nothing').errors).toEqual(["No node matches 'nothing'"]);
    expect(select(model, g, 'a++b').errors).toEqual(["Invalid selector 'a++b'"]);
  });
});

describe('selectWithExclude', () => {
  it('removes excluded nodes', () => {
    const res = selectWithExclude(model, g, '+revenue', 'kind:external');
    expect([...res.ids].sort()).toEqual([C, O, R]);
  });
  it('ignores an empty exclude', () => {
    expect(selectWithExclude(model, g, 'revenue', ' ').ids.size).toBe(1);
  });
});
