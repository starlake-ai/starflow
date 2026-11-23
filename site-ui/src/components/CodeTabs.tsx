import { useState } from 'react';
import hljs from 'highlight.js/lib/core';
import sql from 'highlight.js/lib/languages/sql';
import type { SiteNode } from '../types';
import { codeTabs } from '../nodeView';

hljs.registerLanguage('sql', sql);

export function CodeTabs({ node }: { node: SiteNode }) {
  const tabs = codeTabs(node);
  const [active, setActive] = useState(tabs[0]?.key);
  const tab = tabs.find((t) => t.key === active) ?? tabs[0];
  return (
    <div className="code-tabs">
      {tab && (
        <>
          <div className="tabs">
            {tabs.map((t) => (
              <button key={t.key} type="button" className={t.key === tab.key ? 'active' : ''} onClick={() => setActive(t.key)}>
                {t.label}
              </button>
            ))}
            <button type="button" className="copy" onClick={() => void navigator.clipboard?.writeText(tab.code)}>
              Copy
            </button>
          </div>
          {/* highlight.js escapes the source, so the generated HTML is safe */}
          <pre className="hljs">
            <code dangerouslySetInnerHTML={{ __html: hljs.highlight(tab.code, { language: 'sql' }).value }} />
          </pre>
        </>
      )}
      {node.python && (
        <p>
          Python script: <code>{node.python}</code>
        </p>
      )}
    </div>
  );
}
