import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';
import tailwindcss from '@tailwindcss/vite';

// T040: the page is served by wry's custom protocol handler (origin
// http://app.localhost). WebView2 may block `<script type="module">` over a
// custom scheme (CORS/headers), so the build emits ONE non-module IIFE bundle
// and the transformIndexHtml hook below strips `type="module" crossorigin`
// from the tag Vite injects. base './' keeps every asset URL same-origin.
export default defineConfig({
  base: './',
  plugins: [
    tailwindcss(),
    vue(),
    {
      name: 't040:no-module-output',
      transformIndexHtml(html) {
        return html
          .replace(/<script type="module" crossorigin/g, '<script defer')
          .replace(/<script type="module"/g, '<script defer');
      },
    },
  ],
  build: {
    target: 'es2020',
    modulePreload: false,
    rollupOptions: {
      output: {
        format: 'iife',
        inlineDynamicImports: true,
        name: 'rsKafkaUi',
      },
    },
  },
});
