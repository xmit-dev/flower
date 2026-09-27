import { renderRedirects } from "../scripts/docs/render.mjs";

export default class {
  data() {
    return { layout: false, permalink: "redirects.js" };
  }

  render() {
    return renderRedirects();
  }
}
