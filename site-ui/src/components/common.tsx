import type { ReactNode } from 'react';
import { renderMarkdown } from '../markdown';

export function KindBadge({ kind }: { kind: string }) {
  return <span className={`badge kind-${kind}`}>{kind}</span>;
}

export function Tags({ tags }: { tags?: string[] }) {
  return (
    <>
      {(tags ?? []).map((t) => (
        <span key={t} className="tag">
          {t}
        </span>
      ))}
    </>
  );
}

export function Markdown({ source }: { source?: string | null }) {
  return <div className="markdown" dangerouslySetInnerHTML={{ __html: renderMarkdown(source) }} />;
}

export function Section({ title, extra, children }: { title: string; extra?: ReactNode; children: ReactNode }) {
  return (
    <section className="section">
      <h2>
        {title}
        {extra && <span className="extra">{extra}</span>}
      </h2>
      {children}
    </section>
  );
}
