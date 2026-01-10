import axios from 'axios';
import { config } from '../config.js';
import { logger } from '../logger.js';

export async function sendTelegramMessage({ text, replyMarkup }) {
  if (!config.telegramBotToken || !config.adminTelegramId) return;
  const url = `https://api.telegram.org/bot${config.telegramBotToken}/sendMessage`;
  const payload = {
    chat_id: config.adminTelegramId,
    text,
    disable_web_page_preview: true,
  };
  if (replyMarkup) payload.reply_markup = replyMarkup;

  try {
    const res = await axios.post(url, payload, { timeout: 10000 });
    if (res.status !== 200) {
      logger.warn({ status: res.status, data: res.data }, 'Telegram send failed');
    }
  } catch (e) {
    logger.warn({ err: e }, 'Telegram send error');
  }
}
