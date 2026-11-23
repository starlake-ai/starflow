import { describe, expect, it, vi } from 'vitest';
import { renderToString } from 'react-dom/server';
import { model } from '../test/fixture';

const built = vi.hoisted(() => ({ count: 0 }));
vi.mock('../search/index', async (orig) => {
  const real = await orig<typeof import('../search/index')>();
  return {
    ...real,
    buildSearchIndex: (...args: Parameters<typeof real.buildSearchIndex>) => {
      built.count++;
      return real.buildSearchIndex(...args);
    },
  };
});

describe('App', () => {
  it('does not build the search index at mount', async () => {
    const { App } = await import('./App');
    renderToString(<App data={{ model, stats: null }} />);
    expect(built.count).toBe(0);
  });
});

describe('memo', () => {
  it('computes once on first call', async () => {
    const { memo } = await import('../hooks');
    const f = vi.fn(() => 42);
    const get = memo(f);
    expect(f).not.toHaveBeenCalled();
    expect(get()).toBe(42);
    expect(get()).toBe(42);
    expect(f).toHaveBeenCalledTimes(1);
  });
});
