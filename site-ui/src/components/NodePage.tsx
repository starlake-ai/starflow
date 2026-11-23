import { Fragment, type ReactNode } from 'react';
import type { ConnectionStats, Json, SiteModel, SiteNode, SiteStats, TableStats } from '../types';
import type { Graph } from '../graph/graph';
import { formatBytes, formatCount } from '../format';
import { graphHref, nodeHref } from '../router';
import { relations } from '../nodeView';
import { KindBadge, Markdown, Section, Tags } from './common';
import { ColumnsTable } from './ColumnsTable';
import { CodeTabs } from './CodeTabs';
import { GraphView } from './GraphView';

type Props = { model: SiteModel; stats: SiteStats | null; graph: Graph; node: SiteNode; column?: string };

const str = (v: unknown): string => (v == null ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v));

function summarize(o: Json): string {
  return Object.entries(o)
    .filter(([, v]) => v != null && !(Array.isArray(v) && v.length === 0))
    .map(([k, v]) => `${k}=${str(v)}`)
    .join(', ');
}

function Details({ node, t, conn, hasStats }: { node: SiteNode; t?: TableStats; conn?: ConnectionStats; hasStats: boolean }) {
  const md = node.metadata ?? {};
  const ws = node.writeStrategy ?? {};
  const wsKey = Array.isArray(ws.key) ? (ws.key as string[]) : [];
  const rows: [string, ReactNode][] = [
    ['Database', node.database],
    ['Pattern', node.pattern && <code>{node.pattern}</code>],
    ['Sample file name', node.patternSample],
    ['Format', str(md.format)],
    ['Separator', str(md.separator)],
    ['Encoding', str(md.encoding)],
    ['Write strategy', [str(ws.type), wsKey.length ? `key: ${wsKey.join(', ')}` : ''].filter(Boolean).join(', ')],
    ['Primary key', node.primaryKey?.join(', ')],
    ['Schedule', node.schedule],
    ['Freshness', node.freshness && summarize(node.freshness)],
    ['Sink', node.sink && summarize(node.sink)],
    ['Source file', node.filePath && <code>metadata/{node.filePath}</code>],
  ];
  if (hasStats) {
    if (conn?.status === 'error') rows.push(['Stats', <span className="warn">stats unavailable: {conn.error}</span>]);
    else if (t)
      rows.push(
        ['In warehouse', t.exists ? 'yes' : <span className="warn">missing</span>],
        ['Rows', formatCount(t.rowCount, t.rowCountEstimated)],
        ['Size', formatBytes(t.bytes)],
        ['Last modified', t.lastModified],
        ['Table type', t.tableType],
        ['Storage format', t.format],
      );
  }
  return (
    <dl className="details">
      {rows
        .filter(([, v]) => v !== undefined && v !== null && v !== '' && v !== false)
        .map(([k, v]) => (
          <Fragment key={k}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </Fragment>
        ))}
    </dl>
  );
}

function NodeList({ model, ids }: { model: SiteModel; ids: string[] }) {
  if (ids.length === 0) return <p className="muted">None</p>;
  return (
    <ul className="node-list">
      {ids.map((id) => {
        const n = model.nodes[id];
        return (
          <li key={id}>
            <KindBadge kind={n?.kind ?? 'external'} /> <a href={nodeHref(id)}>{n ? (n.domain ? `${n.domain}.${n.table}` : n.table) : id}</a>
          </li>
        );
      })}
    </ul>
  );
}

export function NodePage({ model, stats, graph, node, column }: Props) {
  const t = stats?.tables?.[node.id];
  const conn = node.connectionRef ? stats?.connections?.[node.connectionRef] : undefined;
  const hasStats = stats != null;
  const { dependsOn, referencedBy, fkTo, fkFrom } = relations(model, graph, node.id);
  const columns = node.columns ?? [];
  const expectations = node.expectations ?? [];
  const acl = node.acl ?? [];
  const rls = node.rls ?? [];
  const warnings = node.warnings ?? [];
  const hasCode = !!(node.sql?.raw || node.sql?.compiled || node.python || node.presql?.length || node.postsql?.length);
  return (
    <div className="page">
      <header className="node-header">
        <KindBadge kind={node.kind} />
        <h1>
          {node.domain ? `${node.domain}.` : ''}
          {node.table}
        </h1>
        <div className="meta">
          <Tags tags={node.tags} />
          {node.engine && <span className="pill">{node.engine}</span>}
          {node.connectionRef && <span className="pill">{node.connectionRef}</span>}
          {node.dag && (
            <a className="pill" href={graphHref(`dag:${node.dag}`)}>
              dag {node.dag}
            </a>
          )}
        </div>
        {warnings.length > 0 && (
          <ul className="warnings">
            {warnings.map((w) => (
              <li key={w}>{w}</li>
            ))}
          </ul>
        )}
      </header>
      <Section title="Description">
        <Markdown source={node.description || '_No description provided._'} />
      </Section>
      <Section title="Details">
        <Details node={node} t={t} conn={conn} hasStats={hasStats} />
      </Section>
      {columns.length > 0 && (
        <Section title="Columns">
          <ColumnsTable columns={columns} diff={t?.columnDiff} highlight={column} hasStats={hasStats} />
        </Section>
      )}
      {expectations.length > 0 && (
        <Section title="Expectations">
          <table className="simple">
            <thead>
              <tr>
                <th>Expectation</th>
                <th>Fail on error</th>
              </tr>
            </thead>
            <tbody>
              {expectations.map((e) => (
                <tr key={e.expect}>
                  <td>
                    <code>{e.expect}</code>
                  </td>
                  <td>{e.failOnError ? 'yes' : 'no'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Section>
      )}
      {(acl.length > 0 || rls.length > 0) && (
        <Section title="Security">
          {acl.length > 0 && (
            <table className="simple">
              <thead>
                <tr>
                  <th>Role</th>
                  <th>Grants</th>
                </tr>
              </thead>
              <tbody>
                {acl.map((a) => (
                  <tr key={a.role}>
                    <td>{a.role}</td>
                    <td>{(a.grants ?? []).join(', ')}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
          {rls.length > 0 && (
            <table className="simple">
              <thead>
                <tr>
                  <th>Row level policy</th>
                  <th>Predicate</th>
                  <th>Grants</th>
                  <th>Description</th>
                </tr>
              </thead>
              <tbody>
                {rls.map((r) => (
                  <tr key={r.name}>
                    <td>{r.name}</td>
                    <td>
                      <code>{r.predicate}</code>
                    </td>
                    <td>{(r.grants ?? []).join(', ')}</td>
                    <td>{r.description}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Section>
      )}
      {(hasCode || node.filePath) && (
        <Section title="Code">
          <CodeTabs node={node} />
        </Section>
      )}
      <Section title="Depends on">
        <NodeList model={model} ids={dependsOn} />
      </Section>
      <Section title="Referenced by">
        <NodeList model={model} ids={referencedBy} />
      </Section>
      {(fkTo.length > 0 || fkFrom.length > 0) && (
        <Section title="Foreign keys">
          <p className="muted">References</p>
          <NodeList model={model} ids={fkTo} />
          <p className="muted">Referenced from</p>
          <NodeList model={model} ids={fkFrom} />
        </Section>
      )}
      <Section title="Lineage" extra={<a href={graphHref(`+${node.id}+`)}>Open graph</a>}>
        <GraphView model={model} graph={graph} select={`1+${node.id}+1`} exclude="" compact focusId={node.id} />
      </Section>
    </div>
  );
}
