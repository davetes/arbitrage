web: python manage.py migrate && gunicorn arbbot.wsgi:application --bind 0.0.0.0:$PORT
worker: celery -A arbbot worker -l info
beat: celery -A arbbot beat -l info
