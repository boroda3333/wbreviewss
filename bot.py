import pandas as pd
import requests
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    CommandHandler,
    MessageHandler,
    filters,
    CallbackContext,
    Application,
    CallbackQueryHandler
)
import io
import logging
import asyncio
from collections import defaultdict

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = "8069123763:AAFQDiopJ6fEHkqEGlLHk7A4S5Tpnc2iVBo"

reply_keyboard = [["Проверить отзывы"]]
markup = ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True, one_time_keyboard=False)

user_sessions = {}


class UserSession:
    """Класс для хранения данных сеанса пользователя"""

    def __init__(self, user_id):
        self.user_id = user_id
        self.current_file = None
        self.processing = False

    async def process_file(self, file_bytes):
        """Основная логика обработки файла"""
        self.processing = True
        try:
            df = pd.read_excel(file_bytes)
            df.columns = df.columns.str.lower()

            required_columns = ['артикул', 'отзыв', 'позитивные', 'негативные']
            if not all(col in df.columns for col in required_columns):
                missing = [col for col in required_columns if col not in df.columns]
                return False, f"Ошибка: В файле отсутствуют колонки: {', '.join(missing)}"

            nm_ids = df['артикул'].astype(str).unique()
            feedbacks_by_nm = await self.get_feedbacks_for_nm_ids(nm_ids)

            result_df = pd.DataFrame(columns=[
                'Артикул', 'отзыв', 'Позитивные', 'Негативные',
                'Статус', 'Статус (эмодзи)'
            ])

            for index, row in df.iterrows():
                nm_id = str(row['артикул'])
                status = ""
                status_emoji = ""

                try:
                    feedbacks = feedbacks_by_nm.get(nm_id, [])
                    if not feedbacks:
                        status = "Ошибка получения"
                        status_emoji = "⚠️"
                    else:
                        if await self.check_feedback_match(row, feedbacks):
                            status = "Найден"
                            status_emoji = "✅"
                        else:
                            status = "Не найден"
                            status_emoji = "❌"
                except Exception as e:
                    status = f"Ошибка: {str(e)}"
                    status_emoji = "⛔"

                result_df.loc[len(result_df)] = [
                    nm_id,
                    str(row['отзыв']) if pd.notna(row['отзыв']) else "",
                    str(row['позитивные']) if pd.notna(row['позитивные']) else "",
                    str(row['негативные']) if pd.notna(row['негативные']) else "",
                    status,
                    status_emoji
                ]

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                result_df.to_excel(writer, index=False, sheet_name='Результаты')
                workbook = writer.book
                worksheet = writer.sheets['Результаты']
                worksheet.set_column('A:A', 15)
                worksheet.set_column('B:D', 50)
                worksheet.set_column('E:E', 15)
                worksheet.set_column('F:F', 10)

            output.seek(0)
            return True, output

        except Exception as e:
            logger.error(f"Ошибка обработки для пользователя {self.user_id}: {str(e)}")
            return False, f"Ошибка обработки файла: {str(e)}"
        finally:
            self.processing = False

    async def get_feedbacks_for_nm_ids(self, nm_ids):
        """Получение отзывов для списка артикулов"""
        feedbacks_by_nm = {}

        for nm_id in set(nm_ids):
            try:
                headers = {
                    'accept': '*/*',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 YaBrowser/25.6.0.0 Safari/537.36',
                }

                params = {'appType': '1', 'curr': 'rub', 'dest': '-1255987', 'nm': nm_id}

                product_response = requests.get(
                    'https://card.wb.ru/cards/v4/list',
                    params=params,
                    headers=headers,
                    timeout=10
                )
                product_data = product_response.json()

                if not product_data.get('products'):
                    continue

                root_id = product_data['products'][0]['root']

                feedback_urls = [
                    f'https://feedbacks2.wb.ru/feedbacks/v2/{root_id}',
                    f'https://feedbacks1.wb.ru/feedbacks/v2/{root_id}'
                ]

                for url in feedback_urls:
                    try:
                        response = requests.get(url, headers=headers, timeout=10)
                        data = response.json()
                        if data.get('feedbacks'):
                            feedbacks_by_nm[nm_id] = data['feedbacks']
                            break
                    except:
                        continue

                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"Ошибка получения отзывов для {nm_id}: {str(e)}")
                continue

        return feedbacks_by_nm

    async def check_feedback_match(self, row, feedbacks):
        """Проверка совпадения отзыва"""
        review_text = str(row['отзыв']).lower() if pd.notna(row['отзыв']) else ""
        pros_text = str(row['позитивные']).lower() if pd.notna(row['позитивные']) else ""
        cons_text = str(row['негативные']).lower() if pd.notna(row['негативные']) else ""

        for fb in feedbacks:
            fb_text = fb.get('text', '').lower()
            fb_pros = fb.get('pros', '').lower()
            fb_cons = fb.get('cons', '').lower()

            if review_text:
                if review_text in fb_text:
                    return True
            else:
                if (pros_text and pros_text in fb_pros) or (cons_text and cons_text in fb_cons):
                    return True
        return False


async def start(update: Update, context: CallbackContext) -> None:
    """Обработчик команды /start"""
    user_id = update.effective_user.id
    if user_id not in user_sessions:
        user_sessions[user_id] = UserSession(user_id)

    await update.message.reply_text(
        "Отправьте Excel файл с отзывами для проверки.\n"
        "Файл должен содержать колонки: 'артикул', 'отзыв', 'позитивные', 'негативные'.",
        reply_markup=markup
    )


async def handle_document(update: Update, context: CallbackContext) -> None:
    """Обработчик документов"""
    user_id = update.effective_user.id
    if user_id not in user_sessions:
        user_sessions[user_id] = UserSession(user_id)

    session = user_sessions[user_id]

    if session.processing:
        await update.message.reply_text("Ваш предыдущий файл еще обрабатывается, пожалуйста подождите.")
        return

    try:
        file = await update.message.document.get_file()
        file_bytes = io.BytesIO()
        await file.download_to_memory(out=file_bytes)
        file_bytes.seek(0)

        await update.message.reply_text("⏳ Файл получен, начинаю обработку...", reply_markup=ReplyKeyboardRemove())

        success, result = await session.process_file(file_bytes)

        if success:
            await update.message.reply_document(
                document=result,
                filename=f'Результаты_проверки_{user_id}.xlsx',
                caption='📊 Результаты проверки отзывов',
                reply_markup=markup
            )
        else:
            await update.message.reply_text(result, reply_markup=markup)

    except Exception as e:
        logger.error(f"Ошибка для пользователя {user_id}: {str(e)}")
        await update.message.reply_text(f"Произошла ошибка: {str(e)}", reply_markup=markup)


async def cancel(update: Update, context: CallbackContext) -> None:
    """Обработчик отмены"""
    user_id = update.effective_user.id
    if user_id in user_sessions:
        del user_sessions[user_id]
    await update.message.reply_text("Сеанс сброшен. Нажмите /start для начала новой проверки.")


def main() -> None:
    """Запуск бота"""
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('cancel', cancel))
    application.add_handler(MessageHandler(filters.Regex(r'^Проверить отзывы$'), start))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    application.run_polling()


if __name__ == '__main__':
    main()