import { describe, expect, it } from 'vitest';
import { graphHref, nodeHref, parseHash } from './router';

describe('router', () => {
  it('defaults to the overview', () => {
    expect(parseHash('')).toEqual({ page: 'overview' });
    expect(parseHash('#/')).toEqual({ page: 'overview' });
    expect(parseHash('#/unknown')).toEqual({ page: 'overview' });
  });

  it('round-trips node routes with an optional column', () => {
    expect(nodeHref('load:sales.orders')).toBe('#/node/load%3Asales.orders');
    expect(parseHash(nodeHref('load:sales.orders'))).toEqual({ page: 'node', id: 'load:sales.orders' });
    expect(parseHash(nodeHref('load:sales.orders', 'email'))).toEqual({
      page: 'node', id: 'load:sales.orders', column: 'email',
    });
  });

  it('round-trips graph selectors containing plus signs and spaces', () => {
    const href = graphHref('2+orders+1 tag:pii', 'kind:external');
    expect(parseHash(href)).toEqual({ page: 'graph', select: '2+orders+1 tag:pii', exclude: 'kind:external' });
    expect(graphHref()).toBe('#/graph');
    expect(parseHash('#/graph')).toEqual({ page: 'graph', select: '', exclude: '' });
  });
});
