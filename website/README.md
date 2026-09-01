# Triplox website

Source for the Triplox website. Built with
[Astro](https://astro.build/) +
[Starlight](https://starlight.astro.build/) and the
[`starlight-theme-black`](https://github.com/adrian-ub/starlight-theme-black)
theme. Blog posts are powered by
[`starlight-blog`](https://github.com/HiDeoo/starlight-blog).

## Run the website locally

Requires Node.js 20+.

```sh
# install dependencies
npm install

# start the dev server at http://localhost:4321
npm run dev
```

Other useful scripts:

| Command            | What it does                           |
| ------------------ | -------------------------------------- |
| `npm run build`    | Build the production site to `./dist/` |
| `npm run preview`  | Preview the production build locally   |
| `npm run astro …`  | Run Astro CLI commands                 |

## Project layout

```
src/
  assets/                   logos and other imported images
  content/
    docs/
      index.mdx             landing page (splash hero)
      getting-started/      docs entry section (introduction, quick-start)
      <section>/            other docs sections (one folder per sidebar group)
      blog/                 blog posts (starlight-blog)
  content.config.ts         content collections config
astro.config.mjs            Astro + Starlight config
public/                     static files served as-is
```

## Deploying to Cloudflare Pages

Build, then upload `dist/` directly with Wrangler:

```sh
npm run build
npx wrangler pages deploy dist --project-name=triplox-website
```

The first run opens a browser to authenticate. Subsequent deploys reuse the
saved credentials.
