import os
import io
import logging
import httpx
import json
import mysql.connector
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK")
# Database connection parameters from .env
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
# Query params
USER_ID = os.getenv("MM_USER_ID")
CHANNEL_ID = os.getenv("MM_CHANNEL_ID")
LIMIT = 3
# Download url
DOWNLOAD_URL = os.getenv("MM_DOWNLOAD_URL")
# Mattermost login credentials (add these to your .env file)
MM_USERNAME = os.getenv("MM_USERNAME")
MM_PASSWORD = os.getenv("MM_PASSWORD")
MM_BASE_URL = os.getenv("MM_BASE_URL")  # e.g., https://your-mattermost-instance.com

# File to store the last CreateAt value (relative to script location)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LAST_CREATE_AT_FILE = os.path.join(SCRIPT_DIR, "last_create_at.txt")

# Global Selenium driver
driver = None

def load_last_create_at():
    """Load the last CreateAt value from the file."""
    try:
        if os.path.exists(LAST_CREATE_AT_FILE):
            with open(LAST_CREATE_AT_FILE, "r") as f:
                return int(f.read().strip())
        else:
            # If the file doesn't exist, return 0 to fetch all posts initially
            return 0
    except Exception as e:
        logger.error(f"Error loading last CreateAt: {e}")
        return 0

def save_last_create_at(create_at):
    """Save the last CreateAt value to the file."""
    try:
        with open(LAST_CREATE_AT_FILE, "w") as f:
            f.write(str(create_at))
    except Exception as e:
        logger.error(f"Error saving last CreateAt: {e}")

def setup_selenium():
    """Set up Selenium WebDriver and return the driver."""
    chrome_options = Options()
    # Optional: Run headless if you don’t need a GUI
    chrome_options.add_argument("--headless")  # Run headlessly for cron
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": os.path.join(os.getcwd(), "downloads"),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
    })
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def login_to_mattermost(driver):
    """Log in to Mattermost using Selenium and return cookies."""
    try:
        # Navigate to the login page
        login_url = f"{MM_BASE_URL}/login"
        driver.get(login_url)
        print("Initial URL:", driver.current_url)

        # Wait for the loginId field to be present (up to 10 seconds)
        login_id_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "loginId"))
        )
        login_id_field.send_keys(MM_USERNAME)

        # Wait for the password field to be present
        password_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "loginPassword"))
        )
        password_field.send_keys(MM_PASSWORD)

        # Wait for the login button to be clickable
        login_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "loginButton"))
        )
        login_button.click()

        # Debug: Print the current URL after clicking login
        time.sleep(2)  # Small delay to allow initial redirect
        print("URL after clicking login button:", driver.current_url)

        # Check for login error messages
        try:
            error_message = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CLASS_NAME, "error-message"))  # Adjust based on actual error element
            )
            print("Login error detected:", error_message.text)
            raise Exception(f"Login failed: {error_message.text}")
        except:
            print("No login error message detected.")

        # Wait for login to complete (check for redirect)
        try:
            # Wait for the URL to contain "steamroom"
            WebDriverWait(driver, 20).until(
                EC.url_contains("steamroom")
            )
            print("Successfully left the login page. Current URL:", driver.current_url)
        except:
            print("Timeout waiting for redirect. Current URL:", driver.current_url)
            print("Page source after timeout:")
            print(driver.page_source)
            raise

        # Navigate to DOWNLOAD_URL to ensure all cookies are captured
        driver.get(DOWNLOAD_URL)
        time.sleep(2)  # Wait for cookies to be set
        cookies = driver.get_cookies()
        print("Cookies after navigating to DOWNLOAD_URL:", cookies)
        logger.info("Cookies after login: %s", cookies)

        # Convert Selenium cookies to a dictionary for httpx
        cookies_dict = {cookie["name"]: cookie["value"] for cookie in cookies}
        return cookies_dict

    except Exception as e:
        logger.exception(f"Error during login: {e}")
        raise

def get_recent_posts_with_files(user_id, channel_id, last_create_at, limit=100):
    """
    Retrieve new posts from Mattermost database with CreateAt > last_create_at

    Args:
        user_id: The user ID to filter posts
        channel_id: The channel ID to filter posts
        last_create_at: The last processed CreateAt timestamp
        limit: Maximum number of posts to fetch (default 100)

    Returns:
    - tuple: (list of posts, highest CreateAt value)
    """
    try:
        # Connect to the database
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor(dictionary=True)

        query = """
        SELECT Message, Hashtags, FileIds, CreateAt
        FROM mattermost_test.posts
        WHERE UserId = %s
        AND ChannelId = %s
        AND CreateAt > %s
        ORDER BY CreateAt DESC
        LIMIT %s;
        """

        cursor.execute(query, (user_id, channel_id, last_create_at, limit))
        results = cursor.fetchall()

        # Get the highest CreateAt value from the results
        highest_create_at = last_create_at
        if results:
            highest_create_at = max(post['CreateAt'] for post in results)

        cursor.close()
        conn.close()

        return results, highest_create_at

    except Exception as e:
        print(f"Database error: {e}")
        raise

def download_file_from_mattermost(file_id, cookies):
    """
    Download a file from Mattermost API using cookies obtained from Selenium

    Args:
        file_id: The ID of the file to download
        cookies: Dictionary of cookies obtained after login

    Returns:
    - tuple containing (file_data, file_name)
    """
    try:
        # Ensure no double slashes in the URL
        download_url = f"{DOWNLOAD_URL.rstrip('/')}/api/v4/files/{file_id}?download=1"
        logger.info(f"Downloading file from Mattermost API: {download_url}")

        with httpx.Client() as client:
            # Add CSRF token to headers if present
            headers = {}
            if "MMCSRF" in cookies:
                headers["X-CSRF-Token"] = cookies["MMCSRF"]
            elif "MM_CSRF_TOKEN" in cookies:
                headers["X-CSRF-Token"] = cookies["MM_CSRF_TOKEN"]

            # Debug: Print cookies and headers being sent
            print("Cookies sent with request:", cookies)
            print("Headers sent with request:", headers)

            response = client.get(
                download_url,
                cookies=cookies,
                headers=headers,
                follow_redirects=True
            )
            logger.info(f"Got file from Mattermost API: {response.status_code}")

            if response.status_code != 200:
                print("Response text:", response.text)
                raise Exception(f"Failed to download file. Status code: {response.status_code}")

            # Get filename from Content-Disposition header if available
            content_disposition = response.headers.get("Content-Disposition", "")
            file_name = file_id

            if content_disposition:
                # Parse the Content-Disposition header manually
                if 'filename="' in content_disposition:
                    # Use the filename field
                    file_name = content_disposition.split('filename="')[1].split('"')[0]
                elif "filename*=" in content_disposition:
                    # Fallback to filename* if filename is not present
                    file_name = content_disposition.split("filename*=")[1].split("'")[-1]
                    # Remove any URL-encoded characters
                    file_name = file_name.replace("%22", "")

            # Clean up the filename to remove invalid characters
            file_name = file_name.replace(";", "").strip()

            return response.content, file_name

    except Exception as e:
        logger.exception(f"Error downloading file: {e}")
        raise

def send_to_discord_webhook(message, file_data, file_name):
    """
    Send a message with a file to Discord webhook
    """
    try:
        with httpx.Client() as client:
            # Create a file-like object in memory
            file_obj = io.BytesIO(file_data)

            # Prepare the files parameter
            files = {
                "file": (file_name, file_obj)
            }

            # Send the message with the file, explicitly setting wait=true
            response = client.post(
                WEBHOOK_URL,
                data={"content": message, "wait": "true"},
                files=files
            )

            # Check if the request was successful
            if response.status_code in (200, 204):
                logger.info(f"Successfully sent {file_name} to Discord")
                return True
            else:
                logger.error(f"Failed to send file to Discord. Status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return False

    except Exception as e:
        logger.exception(f"Error sending to Discord: {e}")
        return False

def process_mattermost_posts(user_id, channel_id):
    """
    Process new posts from a specific user and channel,
    download any attached files, and send them to Discord
    """
    global driver
    try:
        # Load the last CreateAt value
        last_create_at = load_last_create_at()
        logger.info(f"Last CreateAt: {last_create_at}")

        # Set up Selenium and log in
        driver = setup_selenium()
        cookies = login_to_mattermost(driver)

        # Get new posts since the last CreateAt
        posts, highest_create_at = get_recent_posts_with_files(user_id, channel_id, last_create_at)
        logger.info(f"Found {len(posts)} new posts. Highest CreateAt: {highest_create_at}")

        if not posts:
            logger.info("No new posts to process.")
            return

        for post in posts:
            message = post['Message']
            hashtags = post['Hashtags']
            file_ids_json = post['FileIds']

            # Skip if no file IDs
            if not file_ids_json or file_ids_json == "[]":
                continue

            # Parse file IDs from JSON string
            try:
                file_ids = json.loads(file_ids_json)
            except json.JSONDecodeError:
                print(f"Invalid FileIds JSON: {file_ids_json}")
                continue

            # Process each file ID
            for file_id in file_ids:
                logger.info(f'Files: {file_id}')
                try:
                    # Download file using cookies from Selenium
                    file_data, file_name = download_file_from_mattermost(file_id, cookies)

                    # Prepare Discord message
                    discord_message = f"**Message:** {message}"
                    if hashtags:
                        discord_message += f"\n**Hashtags:** {hashtags}"

                    # Send to Discord
                    send_to_discord_webhook(discord_message, file_data, file_name)

                except Exception as e:
                    print(f"Error processing file {file_id}: {e}")

        # Save the highest CreateAt value for the next run
        if highest_create_at > last_create_at:
            save_last_create_at(highest_create_at)
            logger.info(f"Updated last CreateAt to: {highest_create_at}")

    except Exception as e:
        print(f"Error processing posts: {e}")
    finally:
        # Clean up: Close the browser
        if driver:
            driver.quit()

# Example usage
if __name__ == "__main__":
    process_mattermost_posts(USER_ID, CHANNEL_ID)