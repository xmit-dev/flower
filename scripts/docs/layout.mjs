// Site chrome shared by every page under docs/, including generated benchmark
// reports. The header and footer use role attributes rather than <header>,
// <nav> and <footer> so self-contained report styles cannot restyle them.
import { groups } from "./pages.mjs";

export const repository = "https://github.com/xmit-dev/flower";

// `prefix` leads from the page's directory to the site root ("", "../", ...).
export function siteHeader(prefix, currentGroup, { menu = false } = {}) {
  const links = groups.map((group) =>
    `<a href="${prefix}${group.href}"${group.id === currentGroup ? ' aria-current="page"' : ""}>${group.label}</a>`).join("");
  const toggle = menu
    ? '<button class="site-menu" type="button" aria-expanded="false" aria-controls="site-nav">Contents <span aria-hidden="true">＋</span></button>'
    : "";
  return `<div class="site-header" role="banner"><div class="site-header-inner">` +
    `<a class="site-brand" href="${prefix || "./"}" aria-label="Flower home"><img src="${prefix}assets/flower.svg" width="31" height="31" alt=""><span>flower<span class="site-brand-dot">.</span></span></a>` +
    `<div class="site-links" role="navigation" aria-label="Main">${links}<a class="site-repository" href="${repository}">GitHub ↗</a></div>` +
    `${toggle}</div></div>`;
}

export function siteFooter(prefix) {
  return `<div class="site-footer" role="contentinfo"><div class="site-footer-inner">` +
    `<a class="site-brand" href="${prefix || "./"}" aria-label="Flower home"><img src="${prefix}assets/flower.svg" width="25" height="25" alt=""><span>flower<span class="site-brand-dot">.</span></span></a>` +
    `<p>Small seeds. Durable roots.</p>` +
    `<div class="site-footer-links" role="navigation" aria-label="Footer">` +
    groups.map((group) => `<a href="${prefix}${group.href}">${group.label}</a>`).join("") +
    `<a href="${prefix}operate/benchmarks.html">Benchmarks</a><a href="${repository}">Source ↗</a></div></div></div>`;
}
