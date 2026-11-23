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

export async function loadData(
  doc: Document = document,
  fetcher: Fetcher = (url) => fetch(url),
): Promise<SiteData> {
  const model = inline<SiteModel>(doc, 'sl-model') ?? (await fetched<SiteModel>(fetcher, './sl-model.json'));
  if (!model) throw new Error('No project model found: sl-model.json is missing');
  const stats = inline<SiteStats>(doc, 'sl-stats') ?? (await fetched<SiteStats>(fetcher, './sl-stats.json'));
  return { model, stats };
}
