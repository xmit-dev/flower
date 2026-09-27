// Static highlighting: no browser dependency or network request, and copying
// still returns exactly the original source. The Eleventy page layout applies it.

const escape = (text) => text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
const decode = (html) => html.replace(/<\/?span\b[^>]*>/g, "").replace(/&(#x[\da-f]+|#\d+|amp|lt|gt|quot|apos);/gi, (_, entity) => {
  if (entity.startsWith("#")) return String.fromCodePoint(entity[1].toLowerCase() === "x" ? parseInt(entity.slice(2), 16) : Number(entity.slice(1)));
  return { amp: "&", lt: "<", gt: ">", quot: '"', apos: "'" }[entity.toLowerCase()];
});
const token = (text, kind) => kind ? `<span class="${kind}">${escape(text)}</span>` : escape(text);

function typescript(source) {
  const keywords = new Set("import from export default const let var function return if else throw new try catch finally for of in while do switch case break continue async await yield class extends interface type implements public private readonly static typeof instanceof void delete true false null undefined this as is satisfies number string boolean never unknown any".split(" "));
  return [...source.matchAll(/\/\/[^\n]*|\/\*[\s\S]*?\*\/|'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"|`(?:\\.|[^`\\])*`|\b(?:0[xX][\da-fA-F]+|\d[\d_]*(?:\.[\d_]+)?(?:[eE][+-]?\d+)?)n?\b|[a-zA-Z_$][\w$]*|[\s\S]/g)]
    .map(([text]) => token(text, text.startsWith("//") || text.startsWith("/*") ? "comment"
      : /^["'`]/.test(text) ? "str" : /^\d/.test(text) ? "num"
      : keywords.has(text) ? "kw" : undefined)).join("");
}

function shell(source) {
  // These examples contain commands, quoted arguments, variables and comments.
  // Consume quoted strings as a unit so '#' inside an argument isn't a comment.
  return [...source.matchAll(/#[^\n]*|'[^']*'|"(?:\\.|[^"\\])*"|\$\{[^}]*\}|\$[\w]+|--?[a-zA-Z][\w-]*|\b(?:export|npm|npx|node|cargo|git|curl|cp|mkdir|env)\b|\b\d+\b|[\s\S]/g)]
    .map(([text]) => token(text, text.startsWith("#") ? "comment"
      : /^["']/.test(text) ? "str" : /^\d+$/.test(text) ? "num"
      : /^(?:\$|--?[a-zA-Z])/.test(text) ? "var"
      : /^(?:export|npm|npx|node|cargo|git|curl|cp|mkdir|env)$/.test(text) ? "kw" : undefined)).join("");
}

// Highlight every <code class="language-ts|sh"> block, verifying the text is unchanged.
export function highlight(html, file) {
  return html.replace(/(<code class="language-(ts|sh)"[^>]*>)([\s\S]*?)(<\/code>)/g, (_, open, language, body, close) => {
    const source = decode(body);
    const highlighted = language === "ts" ? typescript(source) : shell(source);
    if (decode(highlighted) !== source) throw new Error(`Highlighting changed example text in ${file}`);
    return open + highlighted + close;
  });
}
