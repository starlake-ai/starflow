import { useMemo } from 'react';
import type { SiteModel, SiteStats } from '../types';
import { computeHealth, type Health } from '../health';
import { graphHref, nodeHref } from '../router';
import { Markdown, Section } from './common';

const DEFAULT_OVERVIEW =
  'Add a `{% docs __overview__ %}` block to a Markdown file under `metadata/docs/` to describe this project.';

function countBy<T>(items: T[], key: (t: T) => string): [string, number][] {
  const m = new Map<string, number>();
  for (const i of items) m.set(key(i), (m.get(key(i)) ?? 0) + 1);
  return [...m.entries()].sort(([a], [b]) => a.localeCompare(b));
}

const Links = ({ ids }: { ids: string[] }) => (
  <ul className="node-list">
    {ids.map((id) => (
      <li key={id}>
        <a href={nodeHref(id)}>{id}</a>
      </li>
    ))}
  </ul>
);

function HealthPanel({ health, hasStats }: { health: Health; hasStats: boolean }) {
  const clean =
    !health.warnings.length && !health.externals.length && !health.mismatches.length && !health.missing.length;
  if (clean) return <p className="muted">No issues found.</p>;
  return (
    <div className="health">
      {health.warnings.length > 0 && (
        <>
          <h3>Nodes with warnings ({health.warnings.length})</h3>
          <ul className="node-list">
            {health.warnings.map((w) => (
              <li key={w.id}>
                <a href={nodeHref(w.id)}>{w.id}</a>: {w.messages.join('; ')}
              </li>
            ))}
          </ul>
        </>
      )}
      {health.externals.length > 0 && (
        <>
          <h3>Undeclared tables referenced in SQL ({health.externals.length})</h3>
          <Links ids={health.externals} />
        </>
      )}
      {hasStats && health.mismatches.length > 0 && (
        <>
          <h3>Column type mismatches ({health.mismatches.length})</h3>
          <ul className="node-list">
            {health.mismatches.map((m) => (
              <li key={`${m.id}#${m.column}`}>
                <a href={nodeHref(m.id, m.column)}>
                  {m.id}.{m.column}
                </a>
                : declared <code>{m.declared}</code>, actual <code>{m.actual}</code>
              </li>
            ))}
          </ul>
        </>
      )}
      {hasStats && health.missing.length > 0 && (
        <>
          <h3>Tables missing in the warehouse ({health.missing.length})</h3>
          <Links ids={health.missing} />
        </>
      )}
    </div>
  );
}

export function Overview({ model, stats }: { model: SiteModel; stats: SiteStats | null }) {
  const nodes = Object.values(model.nodes);
  const health = useMemo(() => computeHealth(model, stats), [model, stats]);
  const engines = countBy(nodes.filter((n) => n.engine), (n) => n.engine ?? '');
  return (
    <div className="page">
      <h1>{model.project.name}</h1>
      <Markdown source={model.project.overview || DEFAULT_OVERVIEW} />
      <section className="cards">
        {countBy(nodes, (n) => n.kind).map(([k, v]) => (
          <a className="card" key={k} href={graphHref(`kind:${k}`)}>
            <div className="num">{v}</div>
            <div>{k}</div>
          </a>
        ))}
      </section>
      {engines.length > 0 && <p className="muted">Engines: {engines.map(([e, c]) => `${e} (${c})`).join(', ')}</p>}
      <Section title="Domains">
        <ul className="node-list groups">
          {(model.groups.domains ?? []).map((d) => (
            <li key={d.name}>
              <a href={graphHref(`domain:${d.name}`)}>{d.name}</a> <span className="muted">{d.nodes.length} tables</span>
              {d.description && <Markdown source={d.description} />}
            </li>
          ))}
        </ul>
      </Section>
      {(model.groups.jobs ?? []).length > 0 && (
        <Section title="Jobs">
          <ul className="node-list groups">
            {(model.groups.jobs ?? []).map((j) => (
              <li key={j.name}>
                <a href={graphHref(`job:${j.name}`)}>{j.name}</a> <span className="muted">{j.nodes.length} tasks</span>
                {j.description && <Markdown source={j.description} />}
              </li>
            ))}
          </ul>
        </Section>
      )}
      {(model.groups.dags ?? []).length > 0 && (
        <Section title="DAGs">
          <ul className="node-list groups">
            {(model.groups.dags ?? []).map((d) => (
              <li key={d.name}>
                <a href={graphHref(`dag:${d.name}`)}>{d.name}</a> <span className="muted">{d.nodes.length} nodes</span>
                {d.template && (
                  <>
                    {' '}
                    <code className="small">{d.template}</code>
                  </>
                )}
                {d.comment && <Markdown source={d.comment} />}
              </li>
            ))}
          </ul>
        </Section>
      )}
      <Section title="Health">
        <HealthPanel health={health} hasStats={stats != null} />
      </Section>
      <p className="muted small">
        Generated {model.generatedAt} by Starflow {model.starlakeVersion}
      </p>
    </div>
  );
}
