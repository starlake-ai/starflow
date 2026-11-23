import DOMPurify from 'dompurify';
import MarkdownIt from 'markdown-it';

const md = new MarkdownIt({ html: true, linkify: true });

export function renderMarkdown(src: string | null | undefined): string {
  if (!src) return '';
  return DOMPurify.sanitize(md.render(src));
}
