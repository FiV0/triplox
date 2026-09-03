import starlight from '@astrojs/starlight';
import { defineConfig } from 'astro/config';
import rehypeKatex from 'rehype-katex';
import remarkMath from 'remark-math';
import starlightBlog from 'starlight-blog';
import starlightLlmsTxt from 'starlight-llms-txt';
import starlightThemeBlack from 'starlight-theme-black';

export default defineConfig({
  site: 'https://triplox.xyz',
  markdown: {
    remarkPlugins: [remarkMath],
    rehypePlugins: [rehypeKatex],
  },
  integrations: [
    starlight({
      title: 'Triplox',
      // Pin a monospace stack whose fonts all ship box-drawing glyphs so the
      // ASCII architecture diagrams stay aligned on mobile (no per-glyph
      // fallback to a different-width font). See src/styles/colors.css.
      expressiveCode: {
        styleOverrides: {
          codeFontFamily:
            "'DejaVu Sans Mono', Menlo, Consolas, 'Liberation Mono', 'Courier New', monospace",
        },
      },
      logo: {
        light: './src/assets/blocks_logo.svg',
        dark: './src/assets/blocks_logo_dark.svg',
        replacesTitle: false,
      },
      customCss: ['./src/styles/colors.css', 'katex/dist/katex.min.css'],
      components: {
        Sidebar: './src/components/Sidebar.astro',
        PageTitle: './src/components/PageTitle.astro',
      },
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/FiV0/triplox/',
        },
        {
          icon: 'discord',
          label: 'Discord',
          href: 'https://discord.gg/JSaGCaVre',
        },
      ],
      plugins: [
        starlightThemeBlack({
          navLinks: [
            { label: 'Docs', link: '/getting-started/introduction/' },
            { label: 'Blog', link: '/blog/' },
          ],
          footerText: '',
        }),
        starlightBlog({ navigation: 'none' }),
        starlightLlmsTxt(),
      ],
      sidebar: [
        {
          label: 'Getting Started',
          items: [
            { label: 'Introduction', slug: 'getting-started/introduction' },
            { label: 'Quick Start', slug: 'getting-started/quick-start' },
            { label: 'Architecture', slug: 'getting-started/architecture' },
            { label: 'Concepts', slug: 'getting-started/concepts' },
          ],
        },
        {
          label: 'Data Model',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'data-model/overview' },
          ],
        },
        {
          label: 'Transactions',
          collapsed: true,
          items: [
            { label: 'Life of a transaction', slug: 'transactions/life-of-a-transaction' },
            { label: 'Transaction Data', slug: 'transactions/transaction-data' },
            { label: 'Schema', slug: 'transactions/schema' },
            { label: 'Transaction Model', slug: 'transactions/transaction-model' },
            { label: 'Partitions', slug: 'transactions/partitions' },
          ],
        },
        {
          label: 'Query Language',
          collapsed: true,
          items: [
            { label: 'Datalog', slug: 'query-language/datalog' },
            { label: 'Expression Engine', slug: 'query-language/expression-engine' },
          ],
        },
        {
          label: 'Incremental Queries',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'incremental-queries/overview' },
            { label: 'Tutorial', slug: 'incremental-queries/tutorial' },
          ],
        },
        {
          label: 'Operations',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'operations/overview' },
          ],
        },
        {
          label: 'APIs',
          collapsed: true,
          items: [
            { label: 'Overview', slug: 'apis/overview' },
            { label: 'Clojure', slug: 'apis/clojure' },
            { label: 'Rust', slug: 'apis/rust' },
            { label: 'Java', slug: 'apis/java' },
          ],
        },
        {
          label: 'Roadmap',
          collapsed: true,
          items: [
            { label: 'Roadmap', slug: 'roadmap/roadmap' },
            { label: 'Open questions', slug: 'roadmap/open-questions' },
          ],
        },
      ],
    }),
  ],
});
