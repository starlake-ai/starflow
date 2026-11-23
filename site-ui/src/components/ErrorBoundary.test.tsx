import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ErrorBoundary } from './ErrorBoundary';

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

function Boom({ fail }: { fail: boolean }) {
  if (fail) throw new Error('kaboom in render');
  return <p>all good</p>;
}

describe('ErrorBoundary', () => {
  let container: HTMLDivElement;
  let root: Root;
  beforeEach(() => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    window.location.hash = '#/node/load%3Asales.customers';
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
  });
  afterEach(() => {
    act(() => root.unmount());
    container.remove();
    vi.restoreAllMocks();
  });

  it('shows the error message and the current route instead of a blank page', () => {
    act(() => root.render(<ErrorBoundary resetKey="a"><Boom fail /></ErrorBoundary>));
    expect(container.textContent).toContain('kaboom in render');
    expect(container.textContent).toContain('#/node/load%3Asales.customers');
    expect(container.querySelector('[role="alert"]')).not.toBeNull();
  });

  it('resets when the route key changes', () => {
    act(() => root.render(<ErrorBoundary resetKey="a"><Boom fail /></ErrorBoundary>));
    act(() => root.render(<ErrorBoundary resetKey="b"><Boom fail={false} /></ErrorBoundary>));
    expect(container.textContent).toContain('all good');
  });

  it('resets on hash change when it has no route key (app root)', () => {
    let fail = true;
    const Child = () => <Boom fail={fail} />;
    act(() => root.render(<ErrorBoundary><Child /></ErrorBoundary>));
    expect(container.textContent).toContain('kaboom in render');
    fail = false;
    act(() => {
      window.location.hash = '#/';
      window.dispatchEvent(new HashChangeEvent('hashchange'));
    });
    expect(container.textContent).toContain('all good');
  });
});
