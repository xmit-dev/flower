// Shared layouts for the handbook's Eleventy templates.
import { existsSync, readFileSync } from "node:fs";
import { posix, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { publishedReports, renderPublishedSummary, replaceSummary } from "../publish-bench-results.mjs";
import { highlight } from "./highlight.mjs";
import { siteFooter, siteHeader } from "./layout.mjs";
import { groups, pages } from "./pages.mjs";

const root = fileURLToPath(new URL("../../", import.meta.url));
const docs = resolve(root, "docs");
const origin = "https://flower.xmit.dev/";
const redirects = JSON.parse(readFileSync(resolve(root, "scripts/docs/redirects.json"), "utf8"));
const START = "<!-- content:start -->";
const END = "<!-- content:end -->";

const escape = (text) => String(text).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
const plain = (html) => html.replace(/<[^>]+>/g, "").replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&#39;/g, "'");
const prefixOf = (path) => "../".repeat(path.split("/").length - 1);
const pretty = (path) => path.replace(/(^|\/)index\.html$/, "$1");
const two = (n) => String(n).padStart(2, "0");

export function prepareContent(file, html) {
  if (file === "index.html" || file === "operate/benchmarks.html") {
    const { report, others } = publishedReports(resolve(docs, "bench"));
    html = replaceSummary(html, renderPublishedSummary(report, { root: prefixOf(file), workload: file === "index.html", others }));
  }
  return highlight(includes(file, html.trim()), file);
}

// Top-level <section id> elements of the content become the on-page contents.
function sections(html) {
  const found = [];
  let depth = 0;
  for (const m of html.matchAll(/<(\/?)([a-zA-Z][\w-]*)\b([^>]*)>/g)) {
    const [, close, name, attributes] = m;
    if (/^(?:img|br|meta|link|input|hr|source|wbr)$/i.test(name)) continue;
    if (!close && depth === 0 && name === "section") {
      const id = attributes.match(/\bid="([^"]+)"/)?.[1];
      const label = attributes.match(/\bdata-toc="([^"]+)"/)?.[1];
      const heading = html.slice(m.index).match(/<h2\b[^>]*>([\s\S]*?)<\/h2>/)?.[1];
      if (!id || !heading) throw new Error(`Top-level section needs an id and an h2: ${attributes}`);
      found.push({ id, label: label ?? escape(plain(heading).replace(/\.$/, "")) });
    }
    depth += close ? -1 : 1;
  }
  return found;
}

// <code class="language-ts" data-src="../file.ts"></code> shows that file, so a
// page can never drift from the example people download.
function includes(file, html) {
  return html.replace(/(<code class="language-(?:ts|sh)" data-src="([^"]+)">)[\s\S]*?(<\/code>)/g, (_, open, src, close) => {
    const source = resolve(docs, posix.dirname(file), src);
    if (!existsSync(source)) throw new Error(`${file}: data-src ${src} does not exist`);
    return open + readFileSync(source, "utf8").replace(/\n$/, "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]) + close;
  });
}

function head({ path, title, description, prefix, styles, scripts, extra = "" }) {
  const url = origin + pretty(path);
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#f8f5ec">
<meta name="description" content="${escape(description)}">
<meta property="og:title" content="${escape(title)}">
<meta property="og:description" content="${escape(description)}">
<meta property="og:type" content="website">
<meta property="og:url" content="${url}">
<link rel="canonical" href="${url}">
<title>${escape(title)}</title>
<link rel="icon" href="${prefix}assets/flower.svg" type="image/svg+xml">
${styles.map((file) => `<link rel="stylesheet" href="${prefix}${file}">`).join("\n")}
${scripts.map((file) => `<script src="${prefix}${file}" defer></script>`).join("\n")}${extra}
</head>`;
}

function sidebar(page, prefix, toc) {
  const nav = groups.map((group) => {
    const items = pages.filter((p) => p.group === group.id).map((p, i) => {
      const here = p === page;
      const href = here ? "#top" : prefix + pretty(p.path) || "./";
      const inner = here && toc.length
        ? `<ol id="section-nav" aria-label="On this page">${toc.map((s) => `<li><a href="#${s.id}">${s.label}</a></li>`).join("")}</ol>` : "";
      return `<li${here ? ' class="current"' : ""}><a href="${href}"${here ? ' aria-current="page"' : ""}><span>${two(i + 1)}</span>${p.label}</a>${inner}</li>`;
    }).join("");
    return `<div class="nav-group"><p class="toc-label">${group.label}</p><ol>${items}</ol></div>`;
  }).join("");
  const note = groups.find((g) => g.id === page.group).note;
  return `<aside class="sidebar"><nav id="site-nav" aria-label="Documentation">${nav}</nav>` +
    `<div class="sidebar-note"><img src="${prefix}assets/flower.svg" width="30" height="30" alt="" aria-hidden="true"><p>${note}</p></div></aside>`;
}

function pager(page, prefix) {
  const i = pages.indexOf(page);
  const link = (p, rel, word) => p ? `<a rel="${rel}" href="${prefix}${pretty(p.path) || ""}"><small>${word} · ${groups.find((g) => g.id === p.group).label}</small>${p.label}</a>` : "<span></span>";
  return `<nav class="pager" aria-label="Previous and next page">${link(pages[i - 1], "prev", "Previous")}${link(pages[i + 1], "next", "Next")}</nav>`;
}

// Group landing pages list their siblings, so the index cannot drift.
function catalogue(page, prefix) {
  if (!page.catalogue) return "";
  const cards = pages.filter((p) => p.group === page.group && p !== page).map((p) =>
    `<a class="card" href="${prefix}${p.path}"><strong>${p.label}</strong><span>${p.lead}</span></a>`).join("");
  return `\n<section id="catalogue" data-toc="Every page"><h2>${page.catalogue}</h2><div class="card-grid">${cards}</div></section>`;
}

export function renderPage(page, body) {
  const prefix = prefixOf(page.path);
  const group = groups.find((g) => g.id === page.group);
  const number = pages.filter((p) => p.group === page.group).indexOf(page) + 1;
  const tail = catalogue(page, prefix);
  const toc = sections(body + tail);
  return `${head({ path: page.path, title: `${plain(page.label)} · Flower`, description: page.description, prefix,
    styles: ["chrome.css", "site.css"], scripts: ["site.js"] })}
<body class="doc-page">
<a class="skip-link" href="#main">Skip to content</a>
${siteHeader(prefix, page.group, { menu: true })}
<div class="documentation">
${sidebar(page, prefix, toc)}
<main id="main" tabindex="-1">
<header class="page-intro" id="top"><p class="eyebrow">${group.label} · ${two(number)}</p><h1>${page.title}</h1><p class="lead">${page.lead}</p></header>
<div class="page-content">
${START}
${body}
${END}${tail}
</div>
${pager(page, prefix)}
</main>
</div>
${siteFooter(prefix)}
<p class="sr-only" id="copy-status" role="status" aria-live="polite"></p>
</body>
</html>
`;
}

export function renderHome(body) {
  return `${head({ path: "index.html", title: "Flower — A little logic. A lot of bloom.",
    description: "Flower is a reactive TypeScript database built on Rust, QuickJS, and Raft. Plant a little TypeScript logic and let the reactive values grow.",
    prefix: "", styles: ["chrome.css", "site.css", "home.css"], scripts: ["site.js", "redirects.js"] })}
<body class="home-page">
<a class="skip-link" href="#main">Skip to the example</a>
${siteHeader("", null)}
<div class="wrap">
${START}
${body}
${END}
</div>
${siteFooter("")}
<p class="sr-only" id="copy-status" role="status" aria-live="polite"></p>
</body>
</html>
`;
}

// Retired single-page URLs forward their fragments, with a visible fallback.
export function renderLegacy(entry) {
  const target = entry.target;
  const group = groups.find((g) => target.startsWith(g.href)) ?? groups[0];
  const list = pages.filter((p) => p.group === group.id).map((p) => `<li><a href="${pretty(p.path)}">${p.label}</a></li>`).join("");
  return `${head({ path: entry.path, title: `${entry.title} has moved — Flower`, description: `The ${entry.title.toLowerCase()} now lives at ${origin}${target}.`,
    prefix: "", styles: ["chrome.css", "site.css"], scripts: [],
    extra: `\n<meta name="robots" content="noindex">\n<script src="redirects.js" data-fallback="${target}"></script>\n<noscript><meta http-equiv="refresh" content="0; url=${target}"></noscript>` })}
<body class="doc-page">
${siteHeader("", group.id)}
<main id="main" class="moved">
<header class="page-intro"><p class="eyebrow">Moved</p><h1>This bed has been replanted.</h1><p class="lead">The ${escape(entry.title.toLowerCase())} is now several shorter pages. You should be forwarded to <a href="${target}">${origin}${target}</a>.</p></header>
<ol class="moved-list">${list}</ol>
</main>
${siteFooter("")}
</body>
</html>
`;
}

export function renderRedirects() {
  return `"use strict";
// Generated by scripts/build-docs.mjs. Forwards fragments of retired single-page
// URLs to the shorter pages that replaced them.
(function () {
  var map = ${JSON.stringify(redirects)};
  var script = document.currentScript;
  var page = location.pathname.split("/").pop() || "index.html";
  var id = decodeURIComponent(location.hash.slice(1));
  var target = (map[page] || {})[id];
  if (page === "index.html" && (!target || document.getElementById(id))) return;
  if (target || script.dataset.fallback) location.replace(target || script.dataset.fallback + location.search);
})();
`;
}
