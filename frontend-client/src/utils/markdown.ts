/**
 * Simple markdown parser for chatbot messages.
 * Only allows links to localhost:8081 (your own website)
 * Blocks external links for security
 */

export interface MarkdownNode {
  type: 'text' | 'link' | 'bold' | 'italic';
  content?: string;
  href?: string;
  children?: MarkdownNode[];
  isAllowed?: boolean; // Whether link is allowed (internal only)
}

function isInternalLink(href: string | undefined): boolean {
  if (!href) return false;
  // Only allow localhost:8081 links
  return href.includes('localhost:8081') || href.startsWith('/listing/');
}

export function parseMarkdown(text: string): MarkdownNode[] {
  const nodes: MarkdownNode[] = [];
  let i = 0;

  while (i < text.length) {
    // Check for link: [text](url)
    if (text[i] === '[') {
      const closeIdx = text.indexOf(']', i);
      if (closeIdx !== -1 && text[closeIdx + 1] === '(') {
        const urlClose = text.indexOf(')', closeIdx + 2);
        if (urlClose !== -1) {
          const linkText = text.slice(i + 1, closeIdx);
          const url = text.slice(closeIdx + 2, urlClose);
          
          // Only allow internal links
          const isAllowed = isInternalLink(url);
          
          nodes.push({
            type: 'link',
            content: linkText,
            href: url,
            isAllowed: isAllowed,
          });
          i = urlClose + 1;
          continue;
        }
      }
    }

    // Check for bold: **text**
    if (text[i] === '*' && text[i + 1] === '*') {
      const closeIdx = text.indexOf('**', i + 2);
      if (closeIdx !== -1) {
        const boldText = text.slice(i + 2, closeIdx);
        nodes.push({
          type: 'bold',
          content: boldText,
        });
        i = closeIdx + 2;
        continue;
      }
    }

    // Check for italic: *text* (but not **)
    if (text[i] === '*' && text[i + 1] !== '*' && text[i - 1] !== '*') {
      const closeIdx = text.indexOf('*', i + 1);
      if (closeIdx !== -1 && text[closeIdx - 1] !== '*' && text[closeIdx + 1] !== '*') {
        const italicText = text.slice(i + 1, closeIdx);
        nodes.push({
          type: 'italic',
          content: italicText,
        });
        i = closeIdx + 1;
        continue;
      }
    }

    // Regular text - collect until next special char
    let j = i;
    while (j < text.length && text[j] !== '[' && text[j] !== '*') {
      j++;
    }
    if (j > i) {
      nodes.push({
        type: 'text',
        content: text.slice(i, j),
      });
    }
    i = j;
  }

  return nodes;
}

export function renderMarkdown(text: string) {
  const nodes = parseMarkdown(text);
  return nodes;
}
