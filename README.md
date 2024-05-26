# Telechat: AI-Powered Assistant for Self-Service Analytics

Welcome to **Telechat**, an AI-powered assistant designed to enable self-service analytics for small startups. This project leverages natural language processing (NLP) with ChatGPT and integrates it with a Telegram bot, allowing users to query and retrieve data seamlessly.

## Features

- **Automated Data Queries**: Users can request data in natural language, and the bot translates it into SQL queries.
- **Excel Export**: Export queried data to Excel directly from the chat.
- **Data Visualization**: Generate charts and graphs on-the-fly based on user requests.

## Prerequisites

Before running the application, ensure you have the following set up:

- Python 3.7+
- ClickHouse database
- OpenAI API key
- Telegram Bot API token

## Setup

1. **Clone the repository:**

   ```sh
   git clone https://github.com/AkzhanBerdi/telechat.git
   cd telechat

2. **Create and activate a virtual environment:**

   ```sh
   python3 -m venv .venv

3. **Install the required dependencies:**

   ```sh
   pip install -r requirements.txt

4. **Create a .env file in the project root and add your credentials:**

   ```sh
   touch .env

5. **Add the following lines to the .env file:**

   ```sh
   TELEGRAM_BOT_TOKEN=your-telegram-bot-token
   OPENAI_API_KEY=your-openai-api-key
   DATABASE_URL=clickhouse://default:@localhost/activity

6. **Run the application:**

   ```sh
   python3 main.py
