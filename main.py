import os
import openai
import sqlalchemy
from clickhouse_sqlalchemy import make_session, get_declarative_base
import pandas as pd
from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import logging
import traceback
import re
import io
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Set up your credentials
DATABASE_URL = os.getenv("DATABASE_URL")
openai.api_key = os.getenv("CHAT_GPT_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("BOT_TOKEN")

# Set up database connection
engine = sqlalchemy.create_engine(DATABASE_URL)
session = make_session(engine)
Base = get_declarative_base()

def fetch_data(query):
    with engine.connect() as connection:
        return pd.read_sql_query(query, connection)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hello! Ask me anything about your data.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_message = update.message.text

    # Check for keywords to determine response format
    request_excel = 'excel' in user_message.lower()
    request_chart = 'chart' in user_message.lower()

    table_definition = """
    CREATE TABLE activity.stream
    (
        `timestamp` DateTime,
        `activity_id` UUID,
        `activity` String,
        `entity` String,
        `attributes` Map(String, String)
    )
    ENGINE = MergeTree
    ORDER BY timestamp
    SETTINGS index_granularity = 8192;
    """

    data_sample = """
    timestamp          |activity_id                         |activity        |entity       |attributes                                      |
    -------------------+------------------------------------+----------------+-------------+------------------------------------------------+
    2024-01-01 00:00:00|00000000-0000-0000-0000-000000000000|INCOMING_TRAFFIC|Hong Kong    |[Direct, 1, 1, 0, 1]                            |
    2024-01-01 00:00:00|00000000-0000-0000-0000-000000000000|INCOMING_TRAFFIC|Kazakhstan   |[Referral, 2, 1, 2, 0]                          |
    2024-01-01 00:00:00|00000000-0000-0000-0000-000000000000|INCOMING_TRAFFIC|Sweden       |[Organic Social, 2, 2, 0, 1]                    |
    2024-01-01 00:00:00|00000000-0000-0000-0000-000000000000|INCOMING_TRAFFIC|Germany      |[Direct, 3, 3, 0, 1]                            |
    2024-01-01 00:00:00|00000000-0000-0000-0000-000000000000|INCOMING_TRAFFIC|United States|[Direct, 5, 5, 0, 1]                            |
    2024-01-01 00:00:00|00000000-0000-0000-0000-000000000000|INCOMING_TRAFFIC|Kazakhstan   |[Direct, 5, 4, 5, 0]                            |
    2024-01-01 00:00:00|00000000-0000-0000-0000-000000000000|INCOMING_TRAFFIC|Kazakhstan   |[Organic Social, 13, 11, 9, 0.30769230769230771]|
    2024-01-02 00:00:00|00000000-0000-0000-0000-000000000000|INCOMING_TRAFFIC|Russia       |[Organic Social, 1, 1, 1, 0]                    |
    2024-01-02 00:00:00|00000000-0000-0000-0000-000000000000|INCOMING_TRAFFIC|United States|[Direct, 4, 4, 0, 1]                            |
    2024-01-02 00:00:00|00000000-0000-0000-0000-000000000000|INCOMING_TRAFFIC|Ireland      |[Organic Social, 1, 1, 0, 1]                    |
    """

    system_prompt = f"""
    You are a system that understands ClickHouse SQL syntax and database schemas. The only output you provide is the SQL query, nothing else. 
    Here is the schema of the table you will be working with:

    {table_definition}

    Remember that users don't know the structure of attributes, and they may reffer to the keys of attribute Map() as if it's a regular column. 
    Never use indexes for selecting attribute keys, use explicit attributes names instead.
    Here is the data sample of the table you will be working with:
    {data_sample}

    Also users would never consider activity_id as the relevant attribute, so you can ignore it while generating SQL query.

    Entity means country
    """

    user_prompt = f"""
    User request: {user_message}

    Generate a ClickHouse SQL query based on the user's request. Output only the SQL query, nothing else. 
    Remember to cast Strings if arythmetic aggregation requested. When filtering periods use the '2023-01-01 00:00:00' format.
    """

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",  # Use GPT-4 model
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=150
        )
        
        interpreted_text = response.choices[0].message['content'].strip()

        logger.info("Response: %s", interpreted_text)  # Log interpreted query
        
        # The model should now output only the SQL query
        sql_query_match = re.search(r'(SELECT[\s\S]+)', interpreted_text, re.IGNORECASE)

        if sql_query_match:
            sql_query = sql_query_match.group().strip()
            logger.info("Extracted SQL query: %s", sql_query)
        else:
            logger.error("Failed to extract SQL query from interpreted text.")
            await update.message.reply_text("An error occurred while processing the request.")
            return
        
        try:
            # Fetch data from the data warehouse based on the interpreted query
            data = fetch_data(sql_query)
            if data.empty:
                await update.message.reply_text("No data found for the given query.")
                return

            if request_excel:
                # Convert DataFrame to Excel and send
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    data.to_excel(writer, index=False)
                excel_buffer.seek(0)
                await update.message.reply_document(document=InputFile(excel_buffer, filename='data.xlsx'))
            elif request_chart:
                # Generate and send chart
                plt.figure(figsize=(10, 6))
                for column in data.columns:
                    if column != 'timestamp':
                        plt.plot(data['timestamp'], data[column], label=column)
                plt.title('Chart')
                plt.xlabel('Date')
                plt.ylabel('Values')
                plt.legend()
                plt.xticks(rotation=45)
                plt.tight_layout()
                chart_buffer = io.BytesIO()
                plt.savefig(chart_buffer, format='png')
                chart_buffer.seek(0)
                await update.message.reply_photo(photo=chart_buffer)
            else:
                # Send DataFrame as text
                await update.message.reply_text("Here is the data you requested:")
                await update.message.reply_text(data.to_string())
        except Exception as e:
            logger.error("Database query error: %s", e)
            await update.message.reply_text(f"An error occurred while fetching data: {str(e)}")

    
    except openai.error.RateLimitError:
        logger.error("OpenAI API rate limit exceeded.")
        await update.message.reply_text("I'm currently experiencing high usage. Please try again later.")
    except openai.error.InvalidRequestError as e:
        logger.error("Invalid request to OpenAI API: %s", e)
        await update.message.reply_text(f"An error occurred with the AI service: {str(e)}")
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        logger.error(traceback.format_exc()) 
        await update.message.reply_text(f"An unexpected error occurred: {str(e)}")

def main():
    # Set up Telegram bot application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler('start', start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Run the bot
    application.run_polling()

if __name__ == "__main__":
    main()
