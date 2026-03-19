from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0003_botsettings_use_entire_balance"),
    ]

    operations = [
        migrations.AddField(
            model_name="botsettings",
            name="auto_trade_enabled",
            field=models.BooleanField(default=False),
        ),
    ]
