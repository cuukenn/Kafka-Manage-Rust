import { createApp } from 'vue';
import './style.css';
// Side effect: drains window.__bridgeQueue and installs the real
// window.__bridgeEvents handler (must run before App mounts).
import './bridge/events';
import App from './App.vue';

createApp(App).mount('#app');
