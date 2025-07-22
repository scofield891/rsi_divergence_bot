import os
from dotenv import load_dotenv

load_dotenv()
print("BOT_TOKEN:", os.getenv('BOT_TOKEN'))
print("CHAT_ID:", os.getenv('CHAT_ID'))
print("BINANCE_API_KEY:", os.getenv('BINANCE_API_KEY'))
print("BINANCE_API_SECRET:", os.getenv('BINANCE_API_SECRET'))