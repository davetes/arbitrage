# Intra-Exchange Triangular Arbitrage Bot (Django + Celery + aiogram)

Steps (Windows):

1) Create virtual env and install deps
   - py -m venv .venv
   - .venv\Scripts\activate
   - pip install -r requirements.txt

2) Copy .env
   - copy .env.example .env
   - Fill Binance keys, Telegram token, Redis URL, thresholds

3) Migrate and run Django
   - py manage.py migrate
   - py manage.py createsuperuser
   - py manage.py runserver 127.0.0.1:8000

4) Start Redis and Celery
   - Ensure Redis running at REDIS_URL
   - celery -A arbbot worker -l info
   - celery -A arbbot beat -l info

5) Start Telegram bot
   - py apps/telegram_bot/bot.py

Buttons in Telegram:
- Start Search / Stop Search — toggles Celery scanning via DB flag
- Check Validity — re-quotes prices and depth
- Execute Trade — manual-confirm full cycle after revalidation

English now; i18n hooks prepared for Russian later.
