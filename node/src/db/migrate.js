import { initDb } from './index.js';

await initDb();
console.log('DB OK (sync complete).');
process.exit(0);
