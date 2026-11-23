import type { SiteModel, SiteNode } from '../types';
import { ancestors, descendants, type Graph } from './graph';

const METHODS = ['tag', 'domain', 'job', 'dag', 'kind', 'engine'];
const KINDS = ['load', 'transform', 'external'];
const ATOM = /^(\d*\+)?([^+]+?)(\+\d*)?$/;

export type SelectResult = { ids: Set<string>; errors: string[] };

function depthOf(op: string | undefined): number {
  if (!op) return 0;
  const digits = op.replace('+', '');
  return digits === '' ? Infinity : parseInt(digits, 10);
}

function matchName(model: SiteModel, value: string): string[] {
  const v = value.toLowerCase();
  if (KINDS.some((k) => v.startsWith(`${k}:`))) return model.nodes[v] ? [v] : [];
  const nodes = Object.values(model.nodes);
  const qualified = nodes.filter((n) => !!n.domain && `${n.domain}.${n.table}`.toLowerCase() === v);
  if (qualified.length > 0) return qualified.map((n) => n.id);
  return nodes.filter((n) => n.table.toLowerCase() === v || n.name.toLowerCase() === v).map((n) => n.id);
}

function matchMethod(model: SiteModel, method: string, value: string): string[] {
  const v = value.toLowerCase();
  const eq = (s: string | null | undefined) => (s ?? '').toLowerCase() === v;
  const pick = (f: (n: SiteNode) => boolean) => Object.values(model.nodes).filter(f).map((n) => n.id);
  switch (method) {
    case 'tag':
      return pick((n) => (n.tags ?? []).some(eq));
    case 'domain':
      return pick((n) => eq(n.domain));
    case 'job': {
      const job = (model.groups.jobs ?? []).find((j) => eq(j.name));
      return job ? job.nodes.filter((id) => id in model.nodes) : [];
    }
    case 'dag':
      return pick((n) => eq(n.dag));
    case 'kind':
      return pick((n) => eq(n.kind));
    default:
      return pick((n) => eq(n.engine));
  }
}

function evalAtom(model: SiteModel, g: Graph, atom: string, errors: string[]): Set<string> {
  if (atom.startsWith('@')) {
    const base = evalAtom(model, g, atom.slice(1), errors);
    const all = new Set([...base, ...descendants(g, base)]);
    for (const a of ancestors(g, all)) all.add(a);
    return all;
  }
  const m = ATOM.exec(atom);
  if (!m) {
    errors.push(`Invalid selector '${atom}'`);
    return new Set();
  }
  const [, up, target, down] = m;
  const colon = target.indexOf(':');
  const prefix = colon > 0 ? target.slice(0, colon).toLowerCase() : '';
  let base: string[];
  if (METHODS.includes(prefix)) {
    if (target.slice(colon + 1).trim() === '') {
      errors.push(`Empty value for '${prefix}:'`);
      return new Set();
    }
    base = matchMethod(model, prefix, target.slice(colon + 1));
  } else if (colon > 0 && !KINDS.includes(prefix)) {
    errors.push(`Unknown selector method '${prefix}'`);
    return new Set();
  } else base = matchName(model, target);
  if (base.length === 0) errors.push(`No node matches '${target}'`);
  const result = new Set(base);
  if (up) for (const a of ancestors(g, base, depthOf(up))) result.add(a);
  if (down) for (const d of descendants(g, base, depthOf(down))) result.add(d);
  return result;
}

export function select(model: SiteModel, g: Graph, text: string): SelectResult {
  const errors: string[] = [];
  const terms = text.trim().split(/\s+/).filter(Boolean);
  if (terms.length === 0) return { ids: new Set(Object.keys(model.nodes)), errors };
  const ids = new Set<string>();
  for (const term of terms) {
    const atoms = term.split(',').filter(Boolean);
    if (atoms.length === 0) {
      errors.push(`Invalid selector '${term}'`);
      continue;
    }
    const sets = atoms.map((atom) => evalAtom(model, g, atom, errors));
    const inter = sets.reduce((acc, s) => new Set([...acc].filter((x) => s.has(x))));
    inter.forEach((id) => ids.add(id));
  }
  return { ids, errors };
}

export function selectWithExclude(
  model: SiteModel,
  g: Graph,
  selectText: string,
  excludeText: string,
): SelectResult {
  const selected = select(model, g, selectText);
  if (!excludeText.trim()) return selected;
  const excluded = select(model, g, excludeText);
  return {
    ids: new Set([...selected.ids].filter((id) => !excluded.ids.has(id))),
    errors: [...selected.errors, ...excluded.errors],
  };
}
