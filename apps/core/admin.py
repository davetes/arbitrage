from django.contrib import admin
from .models import BotSettings, Route, Execution

admin.site.register(BotSettings)
admin.site.register(Route)
admin.site.register(Execution)
