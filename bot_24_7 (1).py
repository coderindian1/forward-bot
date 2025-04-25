#!/usr/bin/env python3
"""
Standalone 24/7 Telegram Bot
This bot runs without any web server component and is designed to work
on platforms like Render (using a web service or background worker)
with environment variables for sensitive data and an asyncio queue
for controlled message processing.
"""

import asyncio
import logging
import os
import re
import signal
import sys
import time
import traceback
from typing import Dict, List, Optional, Set, Tuple

from telethon import TelegramClient, events
from telethon.errors import (
    ChatAdminRequiredError,
    ChannelPrivateError,
    FloodWaitError,
    TimedOutError # Added TimedOutError as it's common
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

# Configure logging
# Removed file handler as Render's free tier has ephemeral filesystem
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler() # Logs to stdout/stderr, captured by Render
    ]
)
logger = logging.getLogger(__name__)

# --- Constants ---
# Passwords are now from Environment Variables - Removed hardcoded defaults
# Removed PASSWORDS_FILE constant

MESSAGE_LIMIT_PER_USER = 100  # Daily message processing limit per user
MAX_PROCESSED_IDS = 10000  # Maximum number of processed message IDs to keep in memory
DOWNLOAD_CHUNK_SIZE = 1024 * 1024 * 2  # 2MB chunks for faster downloads (increased)
DOWNLOAD_TIMEOUT = 600  # 10 minutes timeout for downloads (doubled)
OPERATION_DELAY = 0.5  # Further reduced delay between operations (seconds)
SUGGESTION_CHANNEL = "https://t.me/Thestudydimension"  # Channel to suggest for invalid inputs

# --- API credentials and Passwords from Environment Variables ---
API_ID = os.environ.get('TELEGRAM_API_ID')
API_HASH = os.environ.get('TELEGRAM_API_HASH')
BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD') # Get from env
OWNER_PASSWORD = os.environ.get('OWNER_PASSWORD') # Get from env


# Check if essential environment variables are set
if not API_ID or not API_HASH or not BOT_TOKEN or not ADMIN_PASSWORD or not OWNER_PASSWORD:
    logger.error("Required environment variables (TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_BOT_TOKEN, ADMIN_PASSWORD, OWNER_PASSWORD) are not set")
    # Exit with a non-zero code so Render knows to mark the service as failed
    sys.exit(1)

# Ensure downloads directory exists (Still ephemeral on Render free tier)
os.makedirs('downloads', exist_ok=True)

# --- Tracked data in memory (Still ephemeral on Render free tier) ---
user_data = {}  # Store user data
processed_message_ids = set()  # Track processed message IDs to avoid duplicates
# active_links = set() # Removed as queue manages concurrency


# --- Queue System Variables ---
process_queue = asyncio.Queue()
MAX_WORKERS = 3 # Configure the number of concurrent message processing tasks

# --- Global Telegram Client (accessible by handlers and workers) ---
# Client must be initialized before handlers use @client.on()

# API credentials and Passwords from Environment Variables (These are already read here)
# API_ID = os.environ.get('TELEGRAM_API_ID') # These lines are already present
# API_HASH = os.environ.get('TELEGRAM_API_HASH') # Keep them as they are
# BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN') # Keep them as they are
# ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD') # Keep them as they are
# OWNER_PASSWORD = os.environ.get('OWNER_PASSWORD') # Keep them as they are

# Check if essential environment variables are set (This check is already present)
# if not API_ID or not API_HASH or not BOT_TOKEN or not ADMIN_PASSWORD or not OWNER_PASSWORD:
#    ... sys.exit(1)

# --- Initialize the Telegram Client Instance Here ---
# Move the client initialization from run_bot to here
try:
    client = TelegramClient(
        'bot_24_7_session',
        API_ID,
        API_HASH,
        connection_retries=10,
        auto_reconnect=True,
        retry_delay=1,
        timeout=DOWNLOAD_TIMEOUT
    )
    logger.info("Telegram client instance created.")
except Exception as e:
    logger.critical(f"Fatal error creating Telegram client instance: {e}", exc_info=True)
    # Exit with non-zero code to signal failure to Render
    sys.exit(1)


# --- Now the handlers can use @client.on() below this line ---
# ... (The handler definitions follow immediately after this in your file) ...


# --- Removed Functions Related to Password File Management ---
# Removed load_passwords, save_passwords, update_password functions


# --- Helper Functions (Keep these) ---
def save_user_data():
    """Save user data to a JSON file (Still ephemeral)"""
    import json
    try:
        with open('user_data.json', 'w') as f:
            json.dump(user_data, f)
    except Exception as e:
        logger.error(f"Error saving user data (ephemeral): {e}")


def load_user_data() -> Dict:
    """Load user data from a JSON file (Still ephemeral)"""
    import json
    try:
        if os.path.exists('user_data.json'):
            with open('user_data.json', 'r') as f:
                data = json.load(f)
                # Ensure user IDs loaded from JSON are int, not str keys if they were saved that way
                # This loop converts string keys back to integers for 'users', 'admins', 'owners', 'blocked'
                if 'users' in data:
                    data['users'] = {int(k): v for k, v in data['users'].items()}
                if 'admins' in data:
                    data['admins'] = [int(i) for i in data['admins']]
                if 'owners' in data:
                    data['owners'] = [int(i) for i in data['owners']]
                if 'blocked' in data:
                    data['blocked'] = [int(i) for i in data['blocked']]

                return data
        return {}
    except Exception as e:
        logger.error(f"Error loading user data (ephemeral): {e}")
        return {}

# Note: extract_message_info and extract_channel_info are kept as is
def extract_message_info(message_link: str) -> Tuple[Optional[str], Optional[int]]:
    """Extract channel username and message ID from a message link"""
    try:
        message_link = message_link.strip().replace("https://t.me/", "").replace("http://t.me/", "")
        if "/c/" in message_link:
            # Private channel format: c/1234567890/123
            parts = message_link.split('/')
            if len(parts) >= 3 and parts[0] == 'c':
                 # Store private channel ID as a string prepended with 'c/' to distinguish from public
                 channel_id_str = f"c/{parts[1]}"
                 message_id = int(parts[2])
                 return channel_id_str, message_id
        else:
            # Public channel format: channel_name/123
            parts = message_link.split('/')
            if len(parts) >= 2:
                channel_name = parts[0]
                message_id = int(parts[1])
                return channel_name, message_id
    except (ValueError, IndexError) as e:
        logger.error(f"Error extracting message info from {message_link}: {e}")

    return None, None

def extract_channel_info(channel_link: str) -> Optional[str]:
    """Extract channel username or invite hash from a channel link"""
    try:
        channel_link = channel_link.strip()
        if "t.me/+" in channel_link or "t.me/joinchat/" in channel_link:
            # Private channel invite format
            parts = channel_link.split('/')
            invite_hash = parts[-1]
            if invite_hash.startswith('+'):
                invite_hash = invite_hash[1:]  # Remove the + prefix
            return invite_hash
        elif "t.me/" in channel_link:
            # Public channel format
            parts = channel_link.split('/')
            channel_name = parts[-1]
            if not channel_name: # Handle URLs ending with a slash
                 channel_name = parts[-2]
            return channel_name
    except (ValueError, IndexError) as e:
        logger.error(f"Error extracting channel info from {channel_link}: {e}")

    return None


def is_authorized(user_id):
    """Check if a user is authorized (admin or owner - based on ephemeral data)"""
    return (user_id in user_data.get("admins", []) or
            user_id in user_data.get("owners", []))


def signal_handler(sig, frame):
    """Handle termination signals"""
    logger.info("Termination signal received, shutting down...")
    # Save user data before exiting (still ephemeral)
    save_user_data()
    sys.exit(0)


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# --- New Async Functions for Queue Worker and Task Processing ---

async def worker():
    """Asyncio worker task to process items from the queue."""
    logger.info("Worker task started...")
    while True:
        # Get an item from the queue. This will wait if the queue is empty.
        item = await process_queue.get()

        if item is None: # Sentinel value to signal shutdown
            logger.info("Worker received shutdown signal.")
            process_queue.task_done()
            break

        chat_username, message_id, user_id, event_object = item

        try:
            logger.info(f"Worker processing link: {chat_username}/{message_id} for user {user_id}")
            # Call the function that does the actual processing
            await process_message_link_task(chat_username, message_id, user_id, event_object)
            logger.info(f"Worker finished processing link: {chat_username}/{message_id}")

        except Exception as e:
            logger.error(f"Error in worker processing {chat_username}/{message_id}: {e}", exc_info=True)
            # Decide how to handle errors - send message to user? Retry?
            try:
                # Sending error message from worker
                await event_object.reply(f"❌ Error processing link {chat_username}/{message_id}: {str(e)[:200]}...")
            except Exception as reply_error:
                 logger.error(f"Error sending error message from worker: {reply_error}")


        finally:
            # Signal that the queue item is done
            process_queue.task_done()


async def process_message_link_task(chat_username: str, message_id: int, user_id: int, event: events.NewMessage.Event):
    """Core logic to fetch and process a single message link (executed by workers)."""

    # Track in processed IDs set (for deduplication, still useful)
    unique_id = f"{chat_username}_{message_id}"

    # Add to processed IDs, enforcing the limit
    processed_message_ids.add(unique_id)
    # Simple way to keep set size limited, removes an arbitrary element when full
    if len(processed_message_ids) > MAX_PROCESSED_IDS:
         # Convert set to list, remove oldest, convert back - sets are unordered
         # More robust way: remove the first added if we track insertion order, but set is simpler
         if processed_message_ids: # Ensure set is not empty before popping
              processed_message_ids.pop()


    try:
        # Increment the user's daily and total message count (Still ephemeral)
        user_id_str = str(user_id)
        if "users" in user_data and user_id_str in user_data["users"]:
            user_data["users"][user_id_str]["messages_processed"] = user_data["users"][user_id_str].get("messages_processed", 0) + 1
            user_data["users"][user_id_str]["daily_count"] = user_data["users"][user_id_str].get("daily_count", 0) + 1
            # Optional: save user data periodically or after a batch (still ephemeral)
            # save_user_data()


        # Get the message
        # 'client' is accessed here from the global scope set in run_bot
        try:
            if chat_username.startswith('c/'):
                 # Handle private channel ID format
                 channel_entity = int(f"-100{chat_username[2:]}")
            else:
                 # Handle public channel username
                 channel_entity = chat_username

            message = await client.get_messages(channel_entity, ids=message_id)

        except (ValueError, TypeError):
             await event.reply("❌ Could not interpret channel/message ID.")
             return
        except ChannelPrivateError:
             await event.reply("❌ This channel is private. The bot needs to be a member to access it.")
             return
        except ChatAdminRequiredError:
             await event.reply("❌ Admin permissions required to access this chat.")
             return
        except TimedOutError:
             logger.warning(f"Timed out getting message {chat_username}/{message_id}")
             await event.reply("⏳ Request timed out. Please try again.")
             return
        except Exception as e:
            logger.error(f"Error getting message in task {chat_username}/{message_id}: {e}", exc_info=True)
            await event.reply("❌ Error fetching message.")
            return


        if not message:
            await event.reply("❌ Message not found. It might have been deleted or the bot doesn't have access.")
            return


        # Process the message content
        if message.media:
            logger.info(f"Task found media content in message {message_id}")

            # Try all forwarding methods in sequence
            try:
                 forwarded = False

                 # Attempt 1: Standard optimized forward
                 try:
                     logger.info(f"Task attempting optimized direct forward of message {message_id}")
                     # Using background=True allows the worker to potentially get the next item
                     # while forwarding happens in the background.
                     result = await client.forward_messages(
                         entity=user_id,
                         messages=message,
                         silent=True,
                         background=True
                     )

                     if result:
                         forwarded = True
                         logger.info(f"Task: Optimized direct forward successful")
                 except Exception as e:
                     logger.warning(f"Task: Optimized forward failed: {e}")

                 # Attempt 2: ID-based forward
                 if not forwarded:
                     try:
                         logger.info(f"Task attempting ID-based forward for message {message_id}")
                         result = await client.forward_messages(
                             entity=user_id,
                             messages=message.id,
                             from_peer=message.peer_id, # Use peer_id from the message
                             silent=True,
                             background=True
                         )
                         if result:
                             forwarded = True
                             logger.info(f"Task: ID-based forward successful")
                     except Exception as e:
                         logger.warning(f"Task: ID-based forward failed: {e}")

                 # Attempt 3: Direct message copy (sends the message object itself)
                 if not forwarded:
                     try:
                         logger.info(f"Task attempting direct message copy for {message_id}")
                         result = await client.send_message(
                             entity=user_id,
                             message=message,
                             background=True
                         )
                         if result:
                             forwarded = True
                             logger.info(f"Task: Direct message copy successful")
                     except Exception as e:
                         logger.warning(f"Task: Message copy failed: {e}")

                 # Attempt 4: Media clone (downloads and re-uploads) - Slower, resource intensive
                 if not forwarded and hasattr(message, 'media') and message.media:
                     logger.info(f"Task attempting media clone for message {message_id}")
                     # This can be resource intensive (RAM/CPU) and might hit ephemeral disk
                     try:
                         result = await client.send_file(
                             entity=user_id,
                             file=message.media, # Pass the media object
                             caption=message.text if message.text else None,
                             # file_size=message.media.size, # Optional, might help progress
                             # chunk_size=DOWNLOAD_CHUNK_SIZE, # Not directly on send_file, part of Telethon's internal download
                             progress_callback=lambda current, total: logger.debug(f"Download progress: {current}/{total} for {message_id}"), # Optional progress logging
                             background=True
                         )

                         if result:
                             forwarded = True
                             logger.info(f"Task: Media clone successful")
                     except TimedOutError:
                         logger.warning(f"Task: Media clone timed out for {message_id}")
                         await event.reply("⏳ File transfer timed out. Please try again.")
                         return # Exit task
                     except Exception as e:
                         logger.warning(f"Task: Media clone failed: {e}")


                 # Inform user if forwarding was successful or failed after attempts
                 if forwarded:
                      # Decide if you want a success message per file, or just silence
                      # await event.reply("✅ Content delivered!") # Optional success message
                      pass
                 else:
                      await event.reply("⚠️ Failed to forward content after multiple attempts.")

            except FloodWaitError as e:
                logger.warning(f"Task hit FloodWaitError during forwarding: {e}. Waiting {e.seconds}s")
                # This FloodWaitError inside the worker pauses THIS worker task
                await event.reply(f"⚠️ Rate limited by Telegram. Please wait {e.seconds} seconds before trying again.")
                await asyncio.sleep(e.seconds) # Wait within the worker task
                # The item was already taken from the queue. If you want to retry,
                # you'd need to put the item back on the queue carefully.
                # For simplicity here, we log and move on after waiting.

            except Exception as e:
                logger.error(f"Error forwarding media in task: {e}", exc_info=True)
                await event.reply(f"❌ Error forwarding media: {str(e)[:200]}...")

        else:
            # Text message - simple forward (can process directly or queue)
            # Processing text messages via the queue simplifies the message_handler
            text_content = message.text or "⚠️ This message has no text content."

            try:
                 # Try forwarding text message first
                 forwarded = await client.forward_messages(
                     user_id,
                     message,
                     silent=True
                 )

                 if forwarded:
                     logger.info(f"Task forwarded text message for {message_id}")
                     # Optional: await event.reply("✅ Message forwarded successfully!")
                     pass
                 else:
                     # Fallback: send as new message if forward fails
                     header = f"📝 Message from {chat_username}/{message_id}:\n\n"
                     await client.send_message(
                         user_id,
                         header + text_content
                     )
                     logger.info(f"Task sent text message as new message for {message_id}")
                     # Optional: await event.reply("✅ Message text sent successfully!")
                     pass

            except Exception as e:
                 logger.error(f"Error with text message in task: {e}", exc_info=True)
                 await event.reply(f"❌ Error forwarding message: {str(e)[:200]}...")

    # Catch any exceptions not handled in the blocks above
    except Exception as e:
        logger.error(f"Unhandled error in process_message_link_task for {chat_username}/{message_id}: {e}", exc_info=True)
        try:
             await event.reply(f"❌ An unexpected error occurred while processing link: {str(e)[:200]}...")
        except Exception as reply_error:
             logger.error(f"Error sending unexpected error message: {reply_error}")


# --- Telegram Event Handlers ---

@client.on(events.NewMessage(pattern=r'(?i)^/start$'))
async def start_handler(event):
    """Handle /start command"""
    user_id = event.sender_id

    # Anti-duplicate protection - ignore if same command processed recently (KEEP THIS)
    current_time = time.time()
    if user_id in last_command_time and (current_time - last_command_time.get(user_id, 0)) < 3:
        logger.info(f"Ignoring duplicate /start command from user {user_id}")
        return
    last_command_time[user_id] = current_time

    logger.info(f"Received /start command from user {user_id}")

    try:
        # Skip if user is blocked (KEEP THIS, based on ephemeral data)
        if user_id in user_data.get("blocked", []):
            logger.info(f"User {user_id} is blocked, ignoring /start command")
            return

        # Check if user is authorized (admin or owner - based on ephemeral data)
        if not is_authorized(user_id):
            logger.info(f"User {user_id} is not authorized, sending auth request")
            # Send ONE authorization request message
            try:
                await event.reply(
                    "🔒 This is a private bot. Please enter the admin or owner password to continue."
                )
            except FloodWaitError as e:
                logger.warning(f"Hit FloodWaitError sending /start auth request: {e}. Sleeping for {e.seconds}s")
                await asyncio.sleep(e.seconds)
            return

        # Get user info
        user = await event.get_sender()
        name = getattr(user, 'first_name', 'there')
        logger.info(f"User {user_id} ({name}) is authorized, sending welcome message")

        # Add user to database if not already present (Still ephemeral)
        user_id_int = user_id # Use integer key for user_data consistency
        if user_id_int not in user_data.get("users", {}):
            if "users" not in user_data:
                user_data["users"] = {}
            user_data["users"][user_id_int] = {
                "name": name,
                "messages_processed": 0,
                "daily_count": 0,
                "last_reset": time.time()
            }
            # save_user_data() # Optional save

        # Send welcome message - ONLY ONE message to avoid spam
        try:
            await event.reply(
                f"👋 Welcome, {name}!\n\n"
                "I'm the **Study Dimension Forward Bot**. I can retrieve content from Telegram channels where forwarding is restricted.\n\n"
                "🔆 **How to use me:**\n"
                "1. Send me a Telegram message link (like `https://t.me/channel/123`)\n"
                "2. I'll retrieve the content and forward it to you using ultra-fast forwarding\n\n"
                "ℹ️ **Available commands:**\n"
                "/start - Start the bot and see this message\n"
                "/help - Get detailed help\n"
                "/status - Check bot status\n\n"
                "🔍 Example channel to try: https://t.me/Thestudydimension\n\n"
                "🔒 You are authorized to use this bot."
            )
        except FloodWaitError as e:
            logger.warning(f"Hit FloodWaitError in /start response: {e}. Sleeping for {e.seconds}s")
            await asyncio.sleep(e.seconds)
            # Try again after waiting with a simpler message
            try:
                await event.reply(f"👋 Welcome, {name}! I'm the Study Dimension Forward Bot. Send me message links to get started. Type /help for more information.")
            except Exception as inner_e:
                logger.error(f"Error sending simplified welcome message: {inner_e}")

    except Exception as e:
        logger.error(f"Error in start_handler: {e}", exc_info=True)
        try:
            await event.reply("There was an error processing your /start command. Please try again.")
        except Exception as reply_error:
            logger.error(f"Error sending error message: {reply_error}")


@client.on(events.NewMessage(pattern=r'(?i)^/help$'))
async def help_handler(event):
    """Handle /help command"""
    user_id = event.sender_id

    # Anti-duplicate protection (KEEP THIS)
    current_time = time.time()
    if user_id in last_command_time and (current_time - last_command_time.get(user_id, 0)) < 3:
        logger.info(f"Ignoring duplicate /help command from user {user_id}")
        return
    last_command_time[user_id] = current_time

    logger.info(f"Received /help command from user {user_id}")

    try:
        # Skip if user is blocked (KEEP THIS)
        if user_id in user_data.get("blocked", []):
            return

        # Check if user is authorized (KEEP THIS)
        if not is_authorized(user_id):
            try:
                await event.reply("🔒 This is a private bot. Please enter the admin or owner password to continue.")
            except FloodWaitError as e:
                logger.warning(f"Hit FloodWaitError in /help auth: {e}. Sleeping for {e.seconds}s")
                await asyncio.sleep(e.seconds)
            return

        # Send help message - ONLY ONE message
        try:
            await event.reply(
                "📚 **Help Guide**\n\n"
                "This bot helps you retrieve content from channels where forwarding is disabled.\n\n"
                "🔷 **Send me message links like:**\n"
                "• `https://t.me/channel_name/123`\n"
                "• `https://t.me/c/1234567890/123` (for private channels)\n\n"
                "🔷 **Commands:**\n"
                "• /start - Start the bot\n"
                "• /help - Show this help message\n"
                "• /status - Check bot status\n\n"
                "🔍 Try example channel: https://t.me/Thestudydimension\n\n"
                "⚠️ Note: The bot needs to have access to the channel to retrieve content."
            )
        except FloodWaitError as e:
            logger.warning(f"Hit FloodWaitError in /help message: {e}. Sleeping for {e.seconds}s")
            await asyncio.sleep(e.seconds)
            # Try a simplified message after waiting
            try:
                await event.reply("📚 Help: Send me message links like https://t.me/channel_name/123 to retrieve content. Type /status to check bot status.")
            except Exception as inner_e:
                logger.error(f"Error sending simplified help message: {inner_e}")

    except Exception as e:
        logger.error(f"Error in help_handler: {e}", exc_info=True)
        try:
            await event.reply("There was an error processing your /help command. Please try again.")
        except Exception as reply_error:
             logger.error(f"Error sending error message: {reply_error}")


@client.on(events.NewMessage(pattern=r'(?i)^/status$'))
async def status_handler(event):
    """Handle /status command"""
    user_id = event.sender_id

    # Anti-duplicate protection (KEEP THIS)
    current_time = time.time()
    if user_id in last_command_time and (current_time - last_command_time.get(user_id, 0)) < 3:
        logger.info(f"Ignoring duplicate /status command from user {user_id}")
        return
    last_command_time[user_id] = current_time

    logger.info(f"Received /status command from user {user_id}")

    try:
        # Skip if user is blocked (KEEP THIS)
        if user_id in user_data.get("blocked", []):
            return

        # Check if user is authorized (KEEP THIS)
        if not is_authorized(user_id):
            try:
                await event.reply("🔒 This is a private bot. Please enter the admin or owner password to continue.")
            except FloodWaitError as e:
                logger.warning(f"Hit FloodWaitError in /status auth: {e}. Sleeping for {e.seconds}s")
                await asyncio.sleep(e.seconds)
            return

        # Calculate statistics (Based on ephemeral user_data)
        total_users = len(user_data.get("users", {}))
        total_admins = len(user_data.get("admins", []))
        total_owners = len(user_data.get("owners", []))

        user_messages = 0
        user_daily_count = 0

        # Use integer keys for user_data access
        user_id_int = user_id
        if "users" in user_data and user_id_int in user_data["users"]:
            user_info = user_data["users"][user_id_int]
            user_messages = user_info.get("messages_processed", 0)
            user_daily_count = user_info.get("daily_count", 0)

        # Get role
        role = "User"
        # Use integer keys for user_data access
        is_owner = user_id_int in user_data.get("owners", [])
        if is_owner:
            role = "Owner"
        elif user_id_int in user_data.get("admins", []):
            role = "Admin"

        # Show owner commands if user is owner (These commands modify ephemeral data)
        owner_commands = ""
        if is_owner:
            # Removed /change_password as it's not applicable with env vars
            owner_commands = "\n\n👑 **Owner Commands (Ephemeral Data):**\n" \
                          "• /users - View all authorized users (Current session)\n" \
                          "• /remove <user_id> - Remove a user's admin access (Current session)"


        # Get queue status
        queue_size = process_queue.qsize()
        queue_status_line = f"📦 Queue size: {queue_size} (Workers: {MAX_WORKERS})"

        # Send ONLY ONE message as a reply
        try:
            await event.reply(
                "🤖 Bot Status: ONLINE ✅\n\n"
                f"👤 Your ID: {user_id}\n"
                f"🛡️ Your role: {role}\n"
                f"📊 Your stats (Current session):\n" # Explicitly mention ephemeral
                f"• Total messages processed: {user_messages}\n"
                f"• Messages today: {user_daily_count}/{MESSAGE_LIMIT_PER_USER}\n\n"
                f"👥 Total users (Current session): {total_users} (Admins: {total_admins}, Owners: {total_owners})\n\n"
                f"{queue_status_line}\n\n" # Add queue status
                f"The bot is running with ultra-fast forwarding enabled!"
                f"{owner_commands}"
            )
        except FloodWaitError as e:
            logger.warning(f"Hit FloodWaitError in /status response: {e}. Sleeping for {e.seconds}s")
            await asyncio.sleep(e.seconds)
            # Send a simplified response after waiting
            try:
                await event.reply(f"Bot status: ONLINE ✅\nYour role: {role}\nTotal users (Ephemeral): {total_users}\nQueue size: {queue_size}")
            except Exception as inner_e:
                logger.error(f"Error sending simplified status: {inner_e}")

    except Exception as e:
        logger.error(f"Error in status_handler: {e}", exc_info=True)
        try:
            await event.reply("There was an error processing your /status command. Please try again.")
        except Exception as reply_error:
            logger.error(f"Error sending error message: {reply_error}")


# --- Removed auth_handler - Auth is now in message_handler ---
# Removed the separate auth_handler function


# --- Removed /change_password handler - Not applicable with env vars ---
# Removed the change_password_handler function


@client.on(events.NewMessage(pattern=r'(?i)^/users$'))
async def users_handler(event):
    """Handle users list request (owner only - based on ephemeral data)"""
    user_id = event.sender_id

    # Anti-duplicate protection (KEEP THIS)
    current_time = time.time()
    if user_id in last_command_time and (current_time - last_command_time.get(user_id, 0)) < 3:
        logger.info(f"Ignoring duplicate users list request from user {user_id}")
        return
    last_command_time[user_id] = current_time

    logger.info(f"Received users list request from user {user_id}")

    # Only owners can see the users list (Based on ephemeral data)
    if user_id not in user_data.get("owners", []):
        await event.reply("❌ Only bot owners can view the users list.")
        return

    try:
        # Compile the users list (Based on ephemeral data)
        admins = user_data.get("admins", [])
        owners = user_data.get("owners", [])

        # Get user info from the users dictionary
        users_info = user_data.get("users", {})

        admin_list = []
        # Use integer IDs for iteration and lookup
        for admin_id_int in admins:
            name = "Unknown"
            if admin_id_int in users_info:
                name = users_info[admin_id_int].get("name", "Unknown")
            admin_list.append(f"• {name} (ID: {admin_id_int})")

        owner_list = []
        # Use integer IDs for iteration and lookup
        for owner_id_int in owners:
            name = "Unknown"
            if owner_id_int in users_info:
                name = users_info[owner_id_int].get("name", "Unknown")
            owner_list.append(f"• {name} (ID: {owner_id_int})")

        # Format the message
        message = "👥 **Users List (Current session - Ephemeral)**\n\n" # Mention ephemeral

        message += "👑 **Owners:**\n"
        if owner_list:
            message += "\n".join(owner_list)
        else:
            message += "No owners found in current session."

        message += "\n\n🛡️ **Admins:**\n"
        if admin_list:
            message += "\n".join(admin_list)
        else:
            message += "No admins found in current session."

        # Send the message
        await event.reply(message)

    except Exception as e:
        logger.error(f"Error in users_handler: {e}", exc_info=True)
        await event.reply("❌ An error occurred while retrieving the users list. Please try again.")


@client.on(events.NewMessage(pattern=r'(?i)^/remove\s+(\d+)$'))
async def remove_user_handler(event):
    """Handle removing a user from admin/member access (owner only - modifies ephemeral data)"""
    user_id = event.sender_id

    # Anti-duplicate protection (KEEP THIS)
    current_time = time.time()
    if user_id in last_command_time and (current_time - last_command_time.get(user_id, 0)) < 3:
        logger.info(f"Ignoring duplicate remove request from user {user_id}")
        return
    last_command_time[user_id] = current_time

    logger.info(f"Received remove user request from user {user_id}")

    # Only owners can remove users (Based on ephemeral data)
    if user_id not in user_data.get("owners", []):
        await event.reply("❌ Only bot owners can remove users.")
        return

    try:
        # Extract user ID to remove
        user_to_remove = int(event.pattern_match.group(1))

        removed = False

        # Check if the user exists in owners list and is not the owner executing the command
        if "owners" in user_data and user_to_remove in user_data["owners"]:
            if user_to_remove == user_id:
                 await event.reply("❌ You cannot remove yourself from the owners list.")
                 return
            user_data["owners"].remove(user_to_remove)
            removed = True
            logger.info(f"User {user_id} removed user {user_to_remove} from owners list (ephemeral)")


        # Check if the user exists in admins list
        if "admins" in user_data and user_to_remove in user_data["admins"]:
            user_data["admins"].remove(user_to_remove)
            removed = True
            logger.info(f"User {user_id} removed user {user_to_remove} from admins list (ephemeral)")

        # Optional: Remove from the main users dictionary if needed
        if "users" in user_data and user_to_remove in user_data["users"]:
             del user_data["users"][user_to_remove]
             removed = True
             logger.info(f"User {user_id} removed user {user_to_remove} from user data (ephemeral)")

        # Save changes (Still ephemeral)
        if removed:
            # save_user_data() # Optional save
            await event.reply(f"✅ Successfully updated ephemeral access for user {user_to_remove}.")
        else:
            await event.reply(f"ℹ️ User {user_to_remove} not found in owners or admins list (current session).")

    except Exception as e:
        logger.error(f"Error removing user: {e}", exc_info=True)
        await event.reply(f"❌ Error removing user: {str(e)[:200]}...")


@client.on(events.NewMessage)
async def message_handler(event):
    """Handle regular messages (links) and authentication."""
    user_id = event.sender_id
    message_text = event.text.strip()

    # EXTREMELY STRICT anti-spam protection to prevent flood waits at all costs (KEEP THIS)
    current_time = time.time()
    # If we've seen a message from this user in the last 20 seconds, silently ignore it
    # This extremely strict limit should prevent all flood wait errors related to receiving messages.
    if user_id in last_command_time and (current_time - last_command_time.get(user_id, 0)) < 20:
        logger.info(f"Strict spam protection: Ignoring message from user {user_id}")
        return
    last_command_time[user_id] = current_time # Update last command time for ANY incoming message

    # Log all incoming messages for debugging
    logger.info(f"Received message from user {user_id}: {message_text[:100]}...") # Log a bit more

    # Skip if user is blocked (KEEP THIS)
    if user_id in user_data.get("blocked", []):
        logger.info(f"User {user_id} is blocked, ignoring message")
        return

    # --- Handle Authentication for New Users ---
    # If the user is NOT authorized based on the ephemeral user_data
    if not is_authorized(user_id):
        logger.info(f"User {user_id} is not authorized, checking for password...")

        # Check if the message text matches the environment variable passwords
        # Use secrets.compare_digest for constant-time comparison to prevent timing attacks
        import secrets
        if secrets.compare_digest(message_text, ADMIN_PASSWORD):
            logger.info(f"User {user_id} entered correct admin password")
            # Add user as admin if not already (this data is still ephemeral)
            user_id_int = user_id
            if "admins" not in user_data:
                user_data["admins"] = []
            if user_id_int not in user_data["admins"]:
                user_data["admins"].append(user_id_int)
                # save_user_data() # Optional save
                logger.info(f"Added user {user_id} to admins list (ephemeral)")

            # Add user to user_data['users'] if they don't exist (for stats)
            if user_id_int not in user_data.get("users", {}):
                 if "users" not in user_data:
                      user_data["users"] = {}
                 # Try to get user info for name, handle errors
                 name = "Unknown"
                 try:
                      user = await event.get_sender()
                      name = getattr(user, 'first_name', 'Unknown')
                 except Exception as e:
                      logger.warning(f"Could not get sender info for {user_id}: {e}")

                 user_data["users"][user_id_int] = {
                     "name": name,
                     "messages_processed": 0,
                     "daily_count": 0,
                     "last_reset": time.time()
                 }
                 # save_user_data() # Optional save


            # Send ONLY ONE message on successful authentication
            try:
                 await event.reply(
                     "✅ You have been authenticated as an **Admin**.\n\n"
                     "You now have access to the bot's features. Use /help for commands.\n"
                     "Send Telegram message links to get content."
                 )
            except FloodWaitError as e:
                 logger.warning(f"Hit FloodWaitError sending admin auth success: {e}. Sleeping.")
                 await asyncio.sleep(e.seconds + 1) # Wait a bit before returning


            return # Stop processing this message after successful auth

        if secrets.compare_digest(message_text, OWNER_PASSWORD):
            logger.info(f"User {user_id} entered correct owner password")
            # Add user as owner if not already (this data is still ephemeral)
            user_id_int = user_id
            if "owners" not in user_data:
                user_data["owners"] = []
            if user_id_int not in user_data["owners"]:
                 user_data["owners"].append(user_id_int)
                 logger.info(f"Added user {user_id} to owners list (ephemeral)")

            # Also add as admin if not already
            if "admins" not in user_data:
                 user_data["admins"] = []
            if user_id_int not in user_data["admins"]:
                 user_data["admins"].append(user_id_int)
                 logger.info(f"Added user {user_id} to admins list (ephemeral)")

            # Add user to user_data['users'] if they don't exist (for stats)
            if user_id_int not in user_data.get("users", {}):
                 if "users" not in user_data:
                      user_data["users"] = {}
                 # Try to get user info for name, handle errors
                 name = "Unknown"
                 try:
                      user = await event.get_sender()
                      name = getattr(user, 'first_name', 'Unknown')
                 except Exception as e:
                      logger.warning(f"Could not get sender info for {user_id}: {e}")

                 user_data["users"][user_id_int] = {
                     "name": name,
                     "messages_processed": 0,
                     "daily_count": 0,
                     "last_reset": time.time()
                 }
                 # save_user_data() # Optional save

            # Send ONLY ONE message on successful authentication
            try:
                 await event.reply(
                     "✅ You have been authenticated as the **Owner**.\n\n"
                     "You now have full access to the bot. Use /help for commands.\n"
                     "Send Telegram message links to get content."
                 )
            except FloodWaitError as e:
                 logger.warning(f"Hit FloodWaitError sending owner auth success: {e}. Sleeping.")
                 await asyncio.sleep(e.seconds + 1) # Wait a bit before returning

            return # Stop processing this message after successful auth

        # If it's not a password and the user isn't authorized
        logger.info(f"User {user_id} is not authorized and did not provide a correct password.")
        # The initial /start handler already prompts for password if unauthorized.
        # Avoid sending repetitive "Enter password" messages here due to strict anti-spam.
        return # Important: stop processing if not authorized and not a password attempt
    # --- END Authentication for New Users ---


    # --- If authorized, continue processing ---

    # Enforce daily message limit (Based on ephemeral user_data)
    user_id_int = user_id
    if "users" in user_data and user_id_int in user_data["users"]:
        user_info = user_data["users"][user_id_int]
        last_reset = user_info.get("last_reset", 0)

        # Reset counter if 24 hours have passed
        if current_time - last_reset > 86400:  # 24 hours in seconds
            user_info["last_reset"] = current_time
            user_info["daily_count"] = 0
            # save_user_data() # Optional save

        # Check if user has reached daily limit (Owners bypass this)
        if user_info.get("daily_count", 0) >= MESSAGE_LIMIT_PER_USER and user_id_int not in user_data.get("owners", []):
            await event.reply(
                f"⚠️ You have reached your daily limit of {MESSAGE_LIMIT_PER_USER} messages.\n"
                "Please try again tomorrow."
            )
            return # Stop processing

    # Check if the message contains a Telegram link
    if "t.me/" in message_text:
        # Extract message info if it's a message link
        chat_username, message_id = extract_message_info(message_text)

        if chat_username and message_id:
            # --- Add the message link processing task to the queue ---

            # Send an initial message acknowledging receipt *before* queueing
            try:
                 # Only send this if the user is authorized and hasn't hit the limit
                 # which is checked above.
                 await event.reply("✅ Link received! Adding to processing queue...")
            except FloodWaitError as e:
                 logger.warning(f"Hit FloodWaitError sending queue message: {e}. Skipping response.")
                 # Consider a small sleep here if needed, but avoid blocking the handler too long
                 # await asyncio.sleep(e.seconds + 1)


            # Add the necessary info to the queue
            # Pass the event object so the worker can respond to the original message
            await process_queue.put((chat_username, message_id, user_id, event))

            logger.info(f"Added link {chat_username}/{message_id} for user {user_id} to queue. Queue size: {process_queue.qsize()}")

            # --- END OF QUEUE ADDITION ---


        else:
            # This might be a channel link or invalid t.me link
            channel_info = extract_channel_info(message_text)

            if channel_info:
                # This is a channel link - Handle channel link suggestion directly
                await event.reply(
                    "📣 This appears to be a channel link, not a message link.\n\n"
                    "To get content from a channel, please send a specific message link like:\n"
                    "• https://t.me/channel_name/123\n"
                    "• https://t.me/c/1234567890/123\n\n"
                    f"For example, try our suggestion: {SUGGESTION_CHANNEL}/123"
                    # Note: Bot does not automatically join channels from links in this version.
                )
            else:
                # Invalid t.me link format
                await event.reply(
                    "❌ Invalid Telegram link. Please send a valid message link like:\n"
                    "• https://t.me/channel_name/123\n"
                    "• https://t.me/c/1234567890/123\n\n"
                    f"Try our example channel: {SUGGESTION_CHANNEL}"
                )

    else:
        # Not a Telegram link
        # Skip if the message was a command that fell through authorization (like /users, /remove if not owner)
        if message_text.startswith('/'):
             logger.info(f"Ignoring unauthorized or invalid command from user {user_id}")
             return # Stop processing commands not explicitly handled or authorized

        # Respond to non-link, non-command messages
        await event.reply(
            "❌ Please send a valid Telegram message link (containing t.me/).\n\n"
            f"Try our example channel: {SUGGESTION_CHANNEL}"
        )


# --- Main Bot Runner ---

async def run_bot():
    """Run the Telegram bot client and worker tasks."""
    logger.info("Starting the 24/7 Telegram bot client...")

    # Load user data (Still ephemeral)
    global user_data
    user_data = load_user_data()
    # Passwords are now from env vars, removed load_passwords()

    # Create the Telegram client with optimized settings
    

    # Connect and start the bot
    try:
        await client.start(bot_token=BOT_TOKEN)
        logger.info("Telegram client started successfully.")
    except Exception as e:
        logger.critical(f"Fatal error starting Telegram client: {e}", exc_info=True)
        # Exit with non-zero code to signal failure to Render
        sys.exit(1)


    # Get the bot's info
    try:
        bot_info = await client.get_me()
        logger.info(f"Bot started: @{bot_info.username} (ID: {bot_info.id})")
    except Exception as e:
         logger.error(f"Could not fetch bot info: {e}")
         # Continue running, but log the error

    # Track the last command time for each user to prevent duplicates
    # (This variable is local to run_bot, needs to be shared if handlers are separate)
    # As handlers are decorated methods, they should share this.
    # For simplicity, let's assume it's accessed via closure or passed if refactored heavily.
    # In this current structure, the decorated handlers are methods of an implicit class managed by telethon,
    # and they access run_bot's scope or globals. Using a global `last_command_time` is safer.
    global last_command_time # Declare as global to be safe
    last_command_time = {}


    # --- Start the worker tasks ---
    workers = []
    for i in range(MAX_WORKERS):
        # asyncio.create_task schedules the coroutine to run soon
        worker_task = asyncio.create_task(worker(), name=f"worker-{i+1}")
        workers.append(worker_task)
        logger.info(f"Started worker task {i+1}/{MAX_WORKERS}")
    # --- END START WORKERS ---


    # Start keep-alive task
    keep_alive_task = asyncio.create_task(keep_alive())


    try:
        # Run the client until disconnected
        # This keeps the bot connected and listening for events
        logger.info("Client running until disconnected...")
        await client.run_until_disconnected()
        logger.info("Client disconnected.")

    except asyncio.CancelledError:
         logger.info("Bot task was cancelled.")
    except Exception as e:
        logger.critical(f"Bot client crashed with an unhandled exception: {e}", exc_info=True)
        # Exit with non-zero code to signal failure to Render
        sys.exit(1)

    finally:
        logger.info("Bot is shutting down...")

        # Cancel the keep-alive task
        if keep_alive_task and not keep_alive_task.done():
             keep_alive_task.cancel()
             logger.info("Keep-alive task cancelled.")
             try:
                  await keep_alive_task # Await cancellation
             except asyncio.CancelledError:
                  pass # Expected

        # --- Add Queue Shutdown Logic ---
        logger.info(f"Shutting down: Waiting for queue to empty ({process_queue.qsize()} items left)...")
        try:
            # Add sentinel values (None) to the queue to signal workers to stop gracefully
            for _ in range(MAX_WORKERS):
                await process_queue.put(None)

            # Wait for the queue to be fully processed (workers finish current tasks and process sentinels)
            # Use a timeout to prevent infinite waiting if something goes wrong
            await asyncio.wait_for(process_queue.join(), timeout=90.0) # Wait up to 90 secs

        except asyncio.TimeoutError:
            logger.warning("Queue did not empty within timeout during shutdown. Some tasks may not have finished.")
        except Exception as e:
            logger.error(f"Error during graceful queue shutdown: {e}", exc_info=True)
        finally:
             # Ensure worker tasks are cancelled regardless
             logger.info("Cancelling worker tasks...")
             for worker_task in workers:
                 if not worker_task.done():
                      worker_task.cancel()
             # Gather tasks to ensure cancellation is processed
             try:
                  await asyncio.gather(*workers, return_exceptions=True)
             except asyncio.CancelledError:
                  pass # Expected when cancelling
             logger.info("All workers stopped.")
        # --- END Queue Shutdown Logic ---


        # Save user data before exiting (Still ephemeral)
        logger.info("Saving user data (ephemeral)...")
        save_user_data()
        logger.info("Shutdown complete.")
        # Let the script exit, Render will handle restarting if configured


async def keep_alive():
    """Send periodic pings to keep the connection alive and save data."""
    while True:
        await asyncio.sleep(300) # Ping every 5 minutes
        try:
            logger.debug("Sending keep-alive ping...")
            # Send a ping by requesting account info
            if client and client.is_connected():
                 await client.get_me(input_peer=True) # Use input_peer=True for a lighter request
                 logger.debug("Keep-alive successful")
                 # Save user data periodically (Still ephemeral)
                 save_user_data()
                 logger.debug("User data auto-saved.")
            else:
                 logger.warning("Keep-alive: Client not connected.")

        except asyncio.CancelledError:
             logger.info("Keep-alive task cancelled.")
             raise # Re-raise to allow task to properly exit
        except Exception as e:
            logger.error(f"Error in keep_alive: {e}", exc_info=True)


# --- Main Entry Point ---
# This block is primarily used for local testing, app.py runs main_loop directly on Render
async def main_loop():
    """Main loop that keeps the bot running forever with automatic restarts (if run standalone)."""
    # In the app.py -> threading -> asyncio.run(main_loop) context,
    # the outer while True loop here isn't strictly needed for Render's restart policy,
    # but it doesn't hurt if you ever run bot_24_7.py standalone.
    # Render manages restarts based on process exit code.
    while True:
        try:
            logger.info("Executing run_bot()...")
            await run_bot()
        except KeyboardInterrupt:
            logger.info("Bot stopped by user (KeyboardInterrupt)")
            break # Exit the loop on Ctrl+C
        except SystemExit:
             logger.info("Bot received SystemExit.")
             raise # Re-raise SystemExit to allow app.py or runner.py to handle it and potentially stop
        except Exception as e:
            logger.critical(f"run_bot() crashed with error: {e}", exc_info=True)
            logger.info("Restarting the bot in 10 seconds...")
            await asyncio.sleep(10) # Use await for sleep in async function

if __name__ == "__main__":
    """Main entry point for standalone execution (e.g., local testing)"""
    # If running via app.py on Render, this __main__ block is NOT executed by Gunicorn.
    # app.py directly imports main_loop and runs it in a thread.
    print("Starting 24/7 Telegram Bot (Standalone Mode)...")
    print("This bot will run continuously.")
    print("Press Ctrl+C to stop the bot")

    try:
        # Run the main loop with automatic restart capability
        # asyncio.run handles the top-level execution
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\nBot stopped by user (KeyboardInterrupt)")
    except SystemExit as e:
         print(f"\nBot exited with SystemExit: {e}")
         sys.exit(e.code) # Propagate the exit code
    except Exception as e:
        print(f"\nFatal unhandled error in __main__: {e}")
        traceback.print_exc()
        sys.exit(1) # Exit with error code for automatic restart systems

