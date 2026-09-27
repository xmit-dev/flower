import { pages } from "../../scripts/docs/pages.mjs";
import { prepareContent, renderHome, renderPage } from "../../scripts/docs/render.mjs";

export default function ({ content, page }) {
  const file = `${page.filePathStem.slice(1)}.html`;
  const body = prepareContent(file, content);
  if (file === "index.html") return renderHome(body);
  const entry = pages.find((entry) => entry.path === file);
  if (!entry) throw new Error(`${file} is not listed in scripts/docs/pages.mjs`);
  return renderPage(entry, body);
}
