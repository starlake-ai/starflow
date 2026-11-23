import { beforeEach, describe, expect, it, vi } from 'vitest';
import { loadData, type Fetcher } from './load';
import { model, stats } from '../test/fixture';

const block = (id: string, json: string) => `<script type="application/json" id="${id}">${json}</script>`;
const okFetch = (byUrl: Record<string, unknown>): Fetcher =>
  vi.fn(async (url: string) =>
    url in byUrl ? { ok: true, json: async () => byUrl[url] } : { ok: false, json: async () => null },
  );

describe('loadData', () => {
  beforeEach(() => {
    document.head.innerHTML = '';
  });

  it('reads inline blocks without fetching', async () => {
    document.head.innerHTML = block('sl-model', JSON.stringify(model)) + block('sl-stats', JSON.stringify(stats));
    const fetcher = okFetch({});
    const data = await loadData(document, fetcher);
    expect(data.model.project.name).toBe('fixture');
    expect(data.stats?.connections?.duckdb?.status).toBe('ok');
    expect(fetcher).not.toHaveBeenCalled();
  });

  it('parses blocks where every less-than sign is a JSON unicode escape', async () => {
    const m = { ...model, project: { ...model.project, overview: 'a </script> b' } };
    document.head.innerHTML = block('sl-model', JSON.stringify(m).replace(/</g, '\\u003c'));
    expect(document.getElementById('sl-model')?.textContent).toContain('\\u003c');
    const data = await loadData(document, okFetch({}));
    expect(data.model.project.overview).toBe('a </script> b');
  });

  it('defaults the nodes of a group the serializer left empty', async () => {
    const m = { ...model, groups: { ...model.groups, dags: [{ name: 'empty_dag' }] } };
    document.head.innerHTML = block('sl-model', JSON.stringify(m));
    const data = await loadData(document, okFetch({}));
    expect(data.model.groups.dags?.[0].nodes).toEqual([]);
  });

  it('falls back to fetch when inline blocks are absent', async () => {
    const data = await loadData(document, okFetch({ './sl-model.json': model, './sl-stats.json': stats }));
    expect(Object.keys(data.model.nodes)).toHaveLength(5);
    expect(data.stats).not.toBeNull();
  });

  it('treats missing or failing stats as null', async () => {
    const failing: Fetcher = async (url) => {
      if (url === './sl-model.json') return { ok: true, json: async () => model };
      throw new TypeError('Failed to fetch');
    };
    expect((await loadData(document, failing)).stats).toBeNull();
  });

  it('throws when no model is available', async () => {
    await expect(loadData(document, okFetch({}))).rejects.toThrow('sl-model.json');
  });

  it('defaults optional top-level fields for an empty project', async () => {
    const empty = { schemaVersion: 1, generatedAt: 'x', starlakeVersion: 'x', project: { name: 'p' }, groups: {} };
    const data = await loadData(document, okFetch({ './sl-model.json': empty }));
    expect(data.model.nodes).toEqual({});
    expect(data.model.edges).toEqual([]);
    expect(data.model.columnEdges).toEqual([]);
    expect(data.model.docs).toEqual({});
    expect(data.model.groups).toEqual({});
  });
});
