import os
import threading
import asyncio
import logging
import traceback

# Import the main bot script
# Ensure your bot_24_7.py file is in the same directory or accessible via Python path
# (It should be if they are in the same repository root)
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
         # Catch SystemExit from the bot and log it, but don't necessarily kill this thread
         # Render manages process restarts based on the main process exit code.
         logger.info(f"Bot thread received SystemExit: {e}")
         # Depending on desired behavior, you might re-raise or handle differently
         # raise # Uncomment to allow SystemExit to potentially stop the thread/process
    except asyncio.CancelledError:
         logger.info("Bot thread was cancelled.")
    except Exception as e:
        logger.error(f"Bot thread crashed with an error: {e}", exc_info=True)
        # If the bot thread crashes, the Flask server remains running,
        # but the bot functionality is lost. You might want monitoring
        # or a more robust way to restart the bot thread here.

    logger.info("Bot thread finished.")


# --- Startup Logic to Start the Bot Thread ---
# The @before_first_request decorator is removed in Flask 2.2+
def start_bot_thread():
    """
    Creates and starts the bot thread. This function is called directly
    when the app module is loaded in the production environment (by Gunicorn).
    """
    logger.info("Starting bot thread on app module load...")
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
    # You could add checks here (e.g., is bot_thread alive?) for a more robust health check
    return "Telegram Bot is running in the background!"

# --- Main Execution Block (for local testing) ---
# This block is NOT executed by Gunicorn on Render.
if __name__ == '__main__':
    logger.info("Running Flask app locally...")
    # Get the port from the environment variable provided by Render (or default to 5000)
    port = int(os.environ.get('PORT', 5000))

    # When running locally with __main__, call the startup function
    # This ensures the bot thread starts when you run app.py directly.
    # Be aware of Flask's reloader if debug=True, it can cause issues with threads.
    start_bot_thread()

    # Run the Flask app
    # In production on Render, Gunicorn will manage this.
    # host='0.0.0.0' is needed to listen on all interfaces for Render.
    # debug=False is recommended when running threads to avoid unexpected behavior.
    app.run(host='0.0.0.0', port=port, debug=False)

# --- Call the startup function directly at module level ---
# This code runs when the app.py module is imported by Gunicorn (production)
# This is the replacement for the deprecated @app.before_first_request
start_bot_thread()
