import { describe, expect, it } from 'vitest';
import { searchShortcut } from './platform';

describe('searchShortcut', () => {
  it('says Cmd K on macOS and Ctrl K elsewhere', () => {
    expect(searchShortcut({ platform: 'MacIntel' })).toBe('Cmd K');
    expect(searchShortcut({ platform: '', userAgentData: { platform: 'macOS' } })).toBe('Cmd K');
    expect(searchShortcut({ platform: 'Win32' })).toBe('Ctrl K');
    expect(searchShortcut({ platform: 'Linux x86_64', userAgentData: { platform: 'Linux' } })).toBe('Ctrl K');
    expect(searchShortcut(undefined)).toBe('Ctrl K');
  });
});
