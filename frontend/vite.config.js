import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
// https://vite.dev/config/
export default defineConfig({
  plugins: [vue()],
  server: {
    port: parseInt(process.env.VITE_PORT || '3000'),
    host: '0.0.0.0',
    strictPort: false,
    allowedHosts: ['localhost', 'optiplex-790'],
  },
})
