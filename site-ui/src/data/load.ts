import type { SiteData, SiteModel, SiteStats } from '../types';

export type Fetcher = (url: string) => Promise<{ ok: boolean; json(): Promise<unknown> }>;

function inline<T>(doc: Document, id: string): T | null {
  const text = doc.getElementById(id)?.textContent?.trim();
  return text ? (JSON.parse(text) as T) : null;
}

async function fetched<T>(fetcher: Fetcher, url: string): Promise<T | null> {
  try {
    const res = await fetcher(url);
    return res.ok ? ((await res.json()) as T) : null;
  } catch {
    return null;
  }
}

/** Defaults what the serializer drops when empty (NON_EMPTY), in place, and returns the model. */
export function normalizeModel(model: SiteModel): SiteModel {
  model.nodes ??= {};
  model.edges ??= [];
  model.columnEdges ??= [];
  model.docs ??= {};
  model.groups ??= {};
  // A group with no member node (a DAG that matches nothing) has `nodes` dropped too.
  for (const list of [model.groups.domains, model.groups.jobs, model.groups.dags]) {
    for (const g of list ?? []) g.nodes ??= [];
  }
  return model;
}

export async function loadData(
  doc: Document = document,
  fetcher: Fetcher = (url) => fetch(url),
): Promise<SiteData> {
  const inlineModel = inline<SiteModel>(doc, 'sl-model');
  const model = inlineModel ?? (await fetched<SiteModel>(fetcher, './sl-model.json'));
  if (!model) throw new Error('No project model found: sl-model.json is missing');
  normalizeModel(model);
  // An inline model means a generated site: its stats, if any, are inline too, so never fetch
  // (a file:// page would only log a failed request). A fetched model is the dev server layout.
  const inlineStats = inline<SiteStats>(doc, 'sl-stats');
  const stats = inlineStats ?? (inlineModel ? null : await fetched<SiteStats>(fetcher, './sl-stats.json'));
  return { model, stats };
}
