import os
import threading
import asyncio
import logging
import traceback

# Import the main bot script
# Ensure your bot_24_7.py file is in the same directory or accessible via Python path
from bot_24_7 import main_loop

from flask import Flask

# Configure logging for the Flask app itself
# This will also output to Render's console logs via the StreamHandler
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# --- Function to run the bot's asyncio loop in a separate thread ---
# This function will be the target of the new thread
def run_bot_loop():
    """
    Sets up a new asyncio event loop and runs the bot's main loop within it.
    This runs in a separate thread to not block the Flask web server.
    """
    logger.info("Starting the bot's asyncio loop in a separate thread...")
    try:
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Run the bot's main_loop until it completes (e.g., disconnected, error, or signal)
        loop.run_until_complete(main_loop())

    except SystemExit as e:
         # Catch SystemExit from the bot and log it, but don't kill this thread
         logger.info(f"Bot thread received SystemExit: {e}")
    except asyncio.CancelledError:
         logger.info("Bot thread was cancelled.")
    except Exception as e:
        logger.error(f"Bot thread crashed with an error: {e}", exc_info=True)
        # If the bot thread crashes, the Flask server remains running,
        # but the bot functionality is lost. You could add logic here to
        # attempt to restart the bot thread, but handle it carefully.

    logger.info("Bot thread finished.")


# --- Flask Lifecycle Hook to Start the Bot Thread ---
# This decorator ensures the function runs once, before the first request is handled by Flask.
# This is a common way to start background tasks in a Flask web service context.
@app.before_first_request
def start_bot_thread():
    """
    Called by Flask before the first request. Creates and starts the bot thread.
    """
    logger.info("Flask before_first_request: Starting bot thread...")
    # Create a new thread targeting the run_bot_loop function
    bot_thread = threading.Thread(target=run_bot_loop)
    # Setting daemon=True allows the main Flask process to exit even if this thread is still running
    # This is generally appropriate for background tasks in web services.
    bot_thread.daemon = True
    bot_thread.start()
    logger.info("Bot thread started.")


# --- Flask Routes (for Health Checks and basic Web Service presence) ---
@app.route('/')
def index():
    """
    A simple route to indicate the web service is running.
    Used by Render for health checks.
    """
    logger.debug("Received request to /")
    # You could add checks here to see if the bot thread is still alive,
    # but for a simple health check, just responding is usually enough.
    return "Telegram Bot is running in the background!"

# --- Main Execution Block (for local testing) ---
# Render's Gunicorn start command does NOT execute this block.
# It imports the 'app' object directly.
if __name__ == '__main__':
    logger.info("Running Flask app locally...")
    # Get the port from the environment variable provided by Render (or default to 5000)
    port = int(os.environ.get('PORT', 5000))

    # When running locally with __main__, we need to manually start the bot thread
    # as @app.before_first_request might not behave consistently depending on how it's run.
    # A simpler way for local testing is to start it here.
    # If using app.run(debug=True), Flask's reloader can cause threads to start multiple times.
    # For robust local testing of the thread, run with debug=False.
    start_bot_thread() # Call the startup function manually for local run

    # Run the Flask app
    # In production on Render, Gunicorn will manage this.
    app.run(host='0.0.0.0', port=port, debug=False) # Set debug=False for thread safety

