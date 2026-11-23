export type Route =
  | { page: 'overview' }
  | { page: 'node'; id: string; column?: string }
  | { page: 'graph'; select: string; exclude: string };

export function parseHash(hash: string): Route {
  const h = hash.replace(/^#/, '');
  const q = h.indexOf('?');
  const path = q >= 0 ? h.slice(0, q) : h;
  const params = new URLSearchParams(q >= 0 ? h.slice(q + 1) : '');
  if (path.startsWith('/node/')) {
    const id = decodeURIComponent(path.slice('/node/'.length));
    const column = params.get('column');
    return column ? { page: 'node', id, column } : { page: 'node', id };
  }
  if (path === '/graph') {
    return { page: 'graph', select: params.get('select') ?? '', exclude: params.get('exclude') ?? '' };
  }
  return { page: 'overview' };
}

export function nodeHref(id: string, column?: string): string {
  return `#/node/${encodeURIComponent(id)}` + (column ? `?column=${encodeURIComponent(column)}` : '');
}

export function graphHref(select = '', exclude = ''): string {
  const p = new URLSearchParams();
  if (select) p.set('select', select);
  if (exclude) p.set('exclude', exclude);
  const qs = p.toString();
  return '#/graph' + (qs ? `?${qs}` : '');
}
