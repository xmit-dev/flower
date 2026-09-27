import { renderAllBenchResults } from "../../scripts/publish-bench-results.mjs";

export default class {
  async data() {
    return {
      layout: false,
      reports: [...(await renderAllBenchResults())].map(([path, content]) => ({ path, content })),
      pagination: { data: "reports", size: 1, alias: "report" },
      permalink: ({ report }) => report.path,
    };
  }

  render({ report }) {
    return report.content;
  }
}
