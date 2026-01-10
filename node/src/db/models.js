import SequelizePkg from 'sequelize';
const { DataTypes } = SequelizePkg;

export function defineModels(sequelize) {
  const BotSettings = sequelize.define('BotSettings', {
    scanning_enabled: { type: DataTypes.BOOLEAN, defaultValue: true },
    min_profit_pct: { type: DataTypes.FLOAT, defaultValue: 1.0 },
    max_profit_pct: { type: DataTypes.FLOAT, defaultValue: 2.5 },
    fee_bps: { type: DataTypes.FLOAT, defaultValue: 10 },
    extra_fee_bps: { type: DataTypes.FLOAT, defaultValue: 0 },
    slippage_bps: { type: DataTypes.FLOAT, defaultValue: 10 },
    min_notional_usd: { type: DataTypes.FLOAT, defaultValue: 10 },
    max_notional_usd: { type: DataTypes.FLOAT, defaultValue: 10000 },
    use_entire_balance: { type: DataTypes.BOOLEAN, defaultValue: false },
    base_asset: { type: DataTypes.STRING(16), defaultValue: 'USDT' },
    bot_language: { type: DataTypes.STRING(8), defaultValue: 'en' },
  }, {
    tableName: 'core_botsettings',
    timestamps: false,
  });

  // Ensure Django-compatible primary key value (singleton row id=1)
  // Sequelize normally creates 'id' automatically; we set it explicitly when creating.

  const Route = sequelize.define('Route', {
    leg_a: { type: DataTypes.STRING(20), allowNull: false },
    leg_b: { type: DataTypes.STRING(20), allowNull: false },
    leg_c: { type: DataTypes.STRING(20), allowNull: false },
    profit_pct: { type: DataTypes.FLOAT, defaultValue: 0 },
    volume_usd: { type: DataTypes.FLOAT, defaultValue: 0 },
    created_at: { type: DataTypes.DATE, defaultValue: DataTypes.NOW },
  }, {
    tableName: 'core_route',
    timestamps: false,
  });

  const Execution = sequelize.define('Execution', {
    status: { type: DataTypes.STRING(20), defaultValue: 'pending' },
    notional_usd: { type: DataTypes.FLOAT, defaultValue: 0 },
    pnl_usd: { type: DataTypes.FLOAT, defaultValue: 0 },
    started_at: { type: DataTypes.DATE, defaultValue: DataTypes.NOW },
    finished_at: { type: DataTypes.DATE, allowNull: true },
    details: { type: DataTypes.JSONB, defaultValue: {} },
  }, {
    tableName: 'core_execution',
    timestamps: false,
  });

  Execution.belongsTo(Route, { foreignKey: 'route_id' });
  Route.hasMany(Execution, { foreignKey: 'route_id' });

  return { BotSettings, Route, Execution };
}
