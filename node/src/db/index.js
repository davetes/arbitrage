import SequelizePkg from 'sequelize';
const { Sequelize } = SequelizePkg;
import { config } from '../config.js';
import { defineModels } from './models.js';

export const sequelize = new Sequelize(config.databaseUrl, {
  logging: false,
});

export const models = defineModels(sequelize);

export async function initDb() {
  // We do "sync" (create tables) for simplicity.
  // If you prefer migrations, keep sync off and use a migration tool.
  await sequelize.authenticate();
  await sequelize.sync();
}
