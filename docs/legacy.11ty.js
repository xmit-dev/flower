import { legacy } from "../scripts/docs/pages.mjs";
import { renderLegacy } from "../scripts/docs/render.mjs";

export default class {
  data() {
    return {
      layout: false,
      legacy,
      pagination: { data: "legacy", size: 1, alias: "entry" },
      permalink: ({ entry }) => entry.path,
    };
  }

  render({ entry }) {
    return renderLegacy(entry);
  }
}
