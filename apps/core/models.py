from django.db import models


class BotSettings(models.Model):
    scanning_enabled = models.BooleanField(default=True)
    min_profit_pct = models.FloatField(default=1.0)
    max_profit_pct = models.FloatField(default=2.5)
    fee_bps = models.FloatField(default=10)
    extra_fee_bps = models.FloatField(default=0)
    slippage_bps = models.FloatField(default=10)
    min_notional_usd = models.FloatField(default=10)
    max_notional_usd = models.FloatField(default=10000)
    base_asset = models.CharField(max_length=16, default="USDT")
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Settings(enabled={self.scanning_enabled})"


class Route(models.Model):
    leg_a = models.CharField(max_length=20)
    leg_b = models.CharField(max_length=20)
    leg_c = models.CharField(max_length=20)
    profit_pct = models.FloatField(default=0)
    volume_usd = models.FloatField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    def label(self):
        return f"{self.leg_a} → {self.leg_b} → {self.leg_c}"


class Execution(models.Model):
    route = models.ForeignKey(Route, on_delete=models.CASCADE)
    status = models.CharField(max_length=20, default="pending")
    notional_usd = models.FloatField(default=0)
    pnl_usd = models.FloatField(default=0)
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    details = models.JSONField(default=dict, blank=True)
