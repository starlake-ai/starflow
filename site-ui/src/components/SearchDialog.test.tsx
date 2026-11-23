import { act } from 'react';
import { describe, expect, it, vi } from 'vitest';
import { SearchDialog } from './SearchDialog';
import { buildSearchIndex } from '../search/index';
import { model } from '../test/fixture';
import { renderPage } from '../test/render';

const index = buildSearchIndex(model);

function type(input: HTMLInputElement, value: string) {
  const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!;
  act(() => {
    setter.call(input, value);
    input.dispatchEvent(new Event('input', { bubbles: true }));
  });
}
const key = (el: Element, k: string) =>
  act(() => {
    el.dispatchEvent(new KeyboardEvent('keydown', { key: k, bubbles: true }));
  });

describe('SearchDialog', () => {
  it('is a labelled modal dialog with a listbox of options', () => {
    const { container } = renderPage(<SearchDialog index={index} onClose={() => {}} />);
    const dialog = container.querySelector('[role="dialog"]')!;
    expect(dialog.getAttribute('aria-modal')).toBe('true');
    expect(dialog.getAttribute('aria-label')).toBeTruthy();
    type(container.querySelector('input')!, 'cust');
    const options = container.querySelectorAll('[role="listbox"] [role="option"]');
    expect(options.length).toBeGreaterThan(1);
    expect(options[0].getAttribute('aria-selected')).toBe('true');
    expect(options[1].getAttribute('aria-selected')).toBe('false');
  });

  it('scrolls the active hit into view', () => {
    const spy = vi.spyOn(Element.prototype, 'scrollIntoView');
    const { container } = renderPage(<SearchDialog index={index} onClose={() => {}} />);
    const input = container.querySelector('input')!;
    type(input, 'cust');
    spy.mockClear();
    key(input, 'ArrowDown');
    expect(spy).toHaveBeenCalled();
    expect(spy.mock.contexts[0]).toBe(container.querySelectorAll('[role="option"]')[1]);
  });

  it('closes on Escape from anywhere inside the dialog', () => {
    const onClose = vi.fn();
    const { container } = renderPage(<SearchDialog index={index} onClose={onClose} />);
    type(container.querySelector('input')!, 'cust');
    key(container.querySelector('[role="option"]')!, 'Escape');
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('restores focus to the previously focused element on close', () => {
    const opener = document.createElement('button');
    document.body.appendChild(opener);
    opener.focus();
    const { container, rerender } = renderPage(<SearchDialog index={index} onClose={() => {}} />);
    expect(document.activeElement).toBe(container.querySelector('input'));
    rerender(null);
    expect(document.activeElement).toBe(opener);
    opener.remove();
  });
});
