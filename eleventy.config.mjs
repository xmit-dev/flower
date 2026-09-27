import { relative, resolve } from "node:path";
import { assetPatterns } from "./scripts/docs/assets.mjs";
import { validateDocs } from "./scripts/docs/check.mjs";

export default function (config) {
  for (const pattern of assetPatterns) config.addPassthroughCopy(`docs/${pattern}`);
  config.addWatchTarget("scripts/docs/");
  config.addWatchTarget("scripts/publish-bench-results.mjs");
  config.addWatchTarget("bench/*.mjs");
  config.addWatchTarget("docs/*.ts");
  config.addWatchTarget("docs/bench/**/*.json");
  config.on("eleventy.after", ({ directories, results }) => {
    const output = resolve(directories.output);
    validateDocs(new Map(results.map(({ outputPath, content }) => [relative(output, resolve(outputPath)).split("\\").join("/"), content])));
  });
  return {
    dir: { input: "docs", output: "_site" },
    templateFormats: ["html", "md", "11ty.js"],
    // Code examples can contain template delimiters verbatim.
    htmlTemplateEngine: false,
    markdownTemplateEngine: false,
  };
}
