import { describe, expect, it } from 'vitest';
import { formatBytes, formatCount } from './format';

describe('formatCount', () => {
  it('prints exact counts with separators', () => expect(formatCount(120345)).toBe('120,345'));
  it('abbreviates estimated counts', () => {
    expect(formatCount(120345, true)).toBe('~120k');
    expect(formatCount(1500, true)).toBe('~1.5k');
    expect(formatCount(2_500_000, true)).toBe('~2.5M');
    expect(formatCount(999, true)).toBe('~999');
  });
  it('returns an empty string for missing values', () => expect(formatCount(null)).toBe(''));
});

describe('formatBytes', () => {
  it('uses binary units', () => {
    expect(formatBytes(512)).toBe('512 B');
    expect(formatBytes(1536)).toBe('1.5 KB');
    expect(formatBytes(9123456)).toBe('8.7 MB');
  });
  it('returns an empty string for missing values', () => expect(formatBytes(undefined)).toBe(''));
});
