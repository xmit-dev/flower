# Publishing the handbook

The handbook is published at [flower.xmit.dev](https://flower.xmit.dev/) from [xmit-dev/flower](https://github.com/xmit-dev/flower). [Build Awesome](https://build.awesome.me/) generates the handbook; its stable release is installed as `@11ty/eleventy`.

## Preview

```sh
npm ci
bin/web-preview
```

The development server generates and serves `_site/` at `http://localhost:8080/`, with live reload as you edit. `bin/web-preview` changes to the repository root and forwards arguments to Eleventy, for example `bin/web-preview --port=8081`.

- Edit handbook content in `docs/`. HTML and Markdown templates use the shared layout in `docs/_includes/`.
- `scripts/docs/pages.mjs` defines the page order, titles and navigation; `scripts/docs/render.mjs` renders the shared page chrome and highlights examples.
- Static assets, downloadable TypeScript examples and measured benchmark data live in `docs/`. Code blocks with `data-src` include the corresponding example at build time.
- Benchmark reports and summaries are rendered from the retained JSON in `docs/bench/`. `bin/bench` measures the stress preset with the TypeScript and Rust guests and publishes both; select other measured runs with `node scripts/publish-bench-results.mjs path/to/latest.json …`, which retains each run under its guest's name (`latest.json`, `latest-wasm.json`). The live preview picks up the updated measurements. `node scripts/publish-bench-results.mjs --check` validates the retained data offline.
- Commit the sources and measurement JSON in `docs/`. Generated HTML, reports, redirects and copied assets go into the Git-ignored `_site/` directory.
- `npm run docs:check` renders and validates the site in memory, including internal links and legacy redirects. `npm run docs:build` produces a fresh `_site/` for deployment. Both work from a clean checkout after `npm ci`.

## Deploy

With Node.js dependencies installed and xmit configured, run:

```sh
bin/web-deploy
```

The script changes to the repository root, builds and validates the site, then runs `xmit flower.xmit.dev _site/`. The development shell (`nix develop`) supplies Node.js and xmit. Canonical URLs and `docs/CNAME` use `flower.xmit.dev`; CSS and JavaScript use plain relative URLs.

The `Publish handbook` GitHub Actions workflow also builds the site before uploading `_site/` to GitHub Pages.
