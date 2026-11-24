import DOMPurify from 'dompurify';
import MarkdownIt from 'markdown-it';

const md = new MarkdownIt({ html: true, linkify: true });

// A dedicated instance so these hooks never leak into other DOMPurify users.
const purify = DOMPurify(window);

const REMOTE = /^\s*(https?:)?\/\//i;
const EXTERNAL = /^\s*https?:\/\//i;

purify.addHook('afterSanitizeAttributes', (node) => {
  if (node.nodeName === 'IMG') {
    // No network calls at view time: remote images are dropped, data: and relative sources stay.
    if (REMOTE.test(node.getAttribute('src') ?? '')) node.parentNode?.removeChild(node);
  } else if (node.nodeName === 'A' && EXTERNAL.test(node.getAttribute('href') ?? '')) {
    node.setAttribute('target', '_blank');
    node.setAttribute('rel', 'noopener noreferrer');
  }
});

const OPTIONS = {
  // Styles can hide or spoof content; form controls and media elements have no place in docs.
  FORBID_TAGS: ['style', 'form', 'input', 'button', 'textarea', 'select', 'audio', 'video', 'source', 'track', 'picture', 'svg', 'math'],
  // svg and math are never needed in descriptions and can reference remote resources.
  // srcset would let an img load a remote candidate behind a local src.
  FORBID_ATTR: ['style', 'srcset', 'background', 'poster', 'ping', 'formaction', 'xlink:href'],
};

export function renderMarkdown(src: string | null | undefined): string {
  if (!src) return '';
  return purify.sanitize(md.render(src), OPTIONS);
}
