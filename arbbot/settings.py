import os
from pathlib import Path
from dotenv import load_dotenv
import dj_database_url

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "dev-secret")
DEBUG = os.getenv("DJANGO_DEBUG", "True") == "True"
ALLOWED_HOSTS = os.getenv("ALLOWED_HOSTS", "*").split(",")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "apps.core",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "arbbot.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    }
]

WSGI_APPLICATION = "arbbot.wsgi.application"

DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL:
    DATABASES = {
        "default": dj_database_url.parse(DATABASE_URL, conn_max_age=600, ssl_require=False)
    }
else:
    POSTGRES_DB = os.getenv("POSTGRES_DB", "arbbot")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))

    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": POSTGRES_DB,
            "USER": POSTGRES_USER,
            "PASSWORD": POSTGRES_PASSWORD,
            "HOST": POSTGRES_HOST,
            "PORT": POSTGRES_PORT,
            "OPTIONS": {},
        }
    }

AUTH_PASSWORD_VALIDATORS = []

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Networking options for Binance connector
BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
# Example: PROXY_URL=http://127.0.0.1:7890 or socks5://127.0.0.1:1080
PROXY_URL = os.getenv("PROXY_URL", "")

# Celery/Redis
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CELERY_BROKER_URL = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL

# Bot settings
BOT_LANGUAGE = os.getenv("BOT_LANGUAGE", "en")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
BASE_ASSET = os.getenv("BASE_ASSET", "USDT")
FEE_RATE_BPS = float(os.getenv("FEE_RATE_BPS", 10))
EXTRA_FEE_BPS = float(os.getenv("EXTRA_FEE_BPS", 0))
MIN_PROFIT_PCT = float(os.getenv("MIN_PROFIT_PCT", 1.0))
MAX_PROFIT_PCT = float(os.getenv("MAX_PROFIT_PCT", 2.5))
SLIPPAGE_BPS = float(os.getenv("SLIPPAGE_BPS", 10))
MIN_NOTIONAL_USD = float(os.getenv("MIN_NOTIONAL_USD", 10))
MAX_NOTIONAL_USD = float(os.getenv("MAX_NOTIONAL_USD", 10000))
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", 3))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_TELEGRAM_ID = os.getenv("ADMIN_TELEGRAM_ID", "")
# Safety flag: require explicit opt-in for live trading
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "False").lower() == "true"
MAX_ASSETS = int(os.getenv("MAX_ASSETS", "30"))  # Optimized for cross-pair coverage
USE_MID_PRICES = os.getenv("USE_MID_PRICES", "false").lower() in ("1", "true", "yes")

# Celery Beat schedule
from celery.schedules import schedule
CELERY_BEAT_SCHEDULE = {
    "scan-triangular": {
        "task": "apps.core.tasks.scan_triangular_routes",
        "schedule": schedule(run_every=SCAN_INTERVAL_SECONDS),
    }
}
