Node.js port scaffold (Express + BullMQ + Sequelize + Telegraf)

1) Install
   npm install

2) Configure
   copy .env.example .env
   edit .env

3) Create tables
   npm run migrate

4) Run services (separate terminals)
   npm run dev      # Express API
   npm run worker   # Scanner scheduler/worker
   npm run bot      # Telegram bot

HTTP:
  GET /health
  GET /api/settings
  PATCH /api/settings
  GET /api/routes

NOTE: Live trade execution is implemented (Binance MARKET orders).
- It is OFF by default.
- To enable live execution: set TRADING_ENABLED=true and provide BINANCE_API_KEY/BINANCE_API_SECRET in .env
- This project uses decimal.js for safer sizing/rounding. If you pulled latest changes run: npm install
