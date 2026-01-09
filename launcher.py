#!/usr/bin/env python3
"""
Fischer Data Processing App Launcher
====================================

Professional launcher for Fischer Data Processing App with:
- Environment validation
- Port availability checking with conflict resolution
- Streamlit process lifecycle management
- Browser auto-launch with retry logic
- Comprehensive logging and error handling

Author: Fischer Energy Partners
Version: 1.0
Date: 2025-12-19
"""

import os
import sys
import subprocess
import socket
import webbrowser
import time
import ctypes
import logging
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler

# ============================================================================
# CONFIGURATION
# ============================================================================

# Get project root directory (where this script is located)
PROJECT_DIR = Path(__file__).parent.absolute()

# Virtual environment paths
VENV_PYTHON = PROJECT_DIR / ".venv311" / "Scripts" / "python.exe"
STREAMLIT_EXE = PROJECT_DIR / ".venv311" / "Scripts" / "streamlit.exe"

# Application configuration
APP_FILE = PROJECT_DIR / "src" / "app_v12.py"
PORT = 5000
URL = f"http://localhost:{PORT}"

# Logging configuration
LOGS_DIR = PROJECT_DIR / "logs"
LOG_FILE = LOGS_DIR / f"launcher_{datetime.now().strftime('%Y%m%d')}.log"

# Required packages for validation
REQUIRED_PACKAGES = ["pandas", "streamlit", "anthropic", "openpyxl"]


# ============================================================================
# LOGGER SETUP
# ============================================================================

def setup_logging():
    """Configure logging to file and console with rotation."""
    LOGS_DIR.mkdir(exist_ok=True)

    logger = logging.getLogger("FischerAppLauncher")
    logger.setLevel(logging.INFO)

    # Clear existing handlers
    logger.handlers.clear()

    # File handler - rotating at 5MB
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5*1024*1024,
        backupCount=7,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = setup_logging()


# ============================================================================
# ERROR DIALOGS
# ============================================================================

def show_error_dialog(title, message, log_path=None):
    """Display Windows error dialog with message box."""
    if log_path:
        message += f"\n\nLog file: {log_path}"

    try:
        # MB_ICONERROR = 0x10
        ctypes.windll.user32.MessageBoxW(0, message, title, 0x10)
    except Exception as e:
        logger.error(f"Failed to show error dialog: {e}")
        print(f"\n{title}\n{message}")


def show_success_dialog(message):
    """Display Windows success dialog."""
    try:
        # MB_ICONINFORMATION = 0x40
        ctypes.windll.user32.MessageBoxW(
            0, message, "Fischer Data Processing App", 0x40
        )
    except Exception as e:
        logger.error(f"Failed to show success dialog: {e}")
        print(f"\n{message}")


# ============================================================================
# ENVIRONMENT VALIDATION
# ============================================================================

def validate_environment():
    """Validate virtual environment and Streamlit executable."""
    logger.info("Validating environment...")

    # Check Python executable
    if not VENV_PYTHON.exists():
        error = f"Python executable not found at:\n{VENV_PYTHON}"
        logger.error(error)
        return False, error

    logger.info(f"[OK] Python executable found: {VENV_PYTHON}")

    # Check Streamlit executable
    if not STREAMLIT_EXE.exists():
        error = f"Streamlit executable not found at:\n{STREAMLIT_EXE}"
        logger.error(error)
        return False, error

    logger.info(f"[OK] Streamlit executable found: {STREAMLIT_EXE}")

    # Check app file
    if not APP_FILE.exists():
        error = f"Application file not found at:\n{APP_FILE}"
        logger.error(error)
        return False, error

    logger.info(f"[OK] App file found: {APP_FILE}")

    return True, ""


def validate_dependencies():
    """Check if required packages are installed."""
    logger.info("Validating dependencies...")

    missing_packages = []

    for package in REQUIRED_PACKAGES:
        try:
            result = subprocess.run(
                [str(VENV_PYTHON), "-c", f"import {package}"],
                capture_output=True,
                timeout=5
            )
            if result.returncode != 0:
                missing_packages.append(package)
            else:
                logger.info(f"[OK] {package} is installed")
        except Exception as e:
            logger.warning(f"Failed to check {package}: {e}")
            missing_packages.append(package)

    if missing_packages:
        error = f"Missing required packages: {', '.join(missing_packages)}"
        logger.error(error)
        return False, missing_packages

    logger.info("[OK] All dependencies validated")
    return True, []


def check_api_key():
    """Validate .env file and CLAUDE_API_KEY presence."""
    env_path = PROJECT_DIR / ".env"

    if not env_path.exists():
        logger.warning(".env file not found - AI features may not work")
        return False, ".env file not found"

    try:
        with open(env_path, 'r') as f:
            content = f.read()
            if "CLAUDE_API_KEY" not in content:
                logger.warning("CLAUDE_API_KEY not found in .env - AI features may not work")
                return False, "CLAUDE_API_KEY not found"

        logger.info("[OK] API key found")
        return True, ""
    except Exception as e:
        logger.warning(f"Failed to read .env: {e}")
        return False, str(e)


# ============================================================================
# PORT MANAGEMENT
# ============================================================================

def is_port_in_use(port):
    """Check if port is already in use using socket."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        result = sock.connect_ex(('localhost', port))
        return result == 0
    except Exception as e:
        logger.warning(f"Failed to check port {port}: {e}")
        return False
    finally:
        sock.close()


def find_process_on_port(port):
    """Find process using specified port."""
    try:
        result = subprocess.run(
            f'netstat -ano | findstr :{port}',
            shell=True,
            capture_output=True,
            text=True,
            timeout=5
        )

        if result.stdout:
            # Parse netstat output
            for line in result.stdout.split('\n'):
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 5:
                        try:
                            pid = int(parts[-1])
                            # Get process name
                            tasklist = subprocess.run(
                                f'tasklist /FI "PID eq {pid}" /FO LIST',
                                shell=True,
                                capture_output=True,
                                text=True,
                                timeout=5
                            )
                            for tl_line in tasklist.stdout.split('\n'):
                                if "Image Name:" in tl_line:
                                    process_name = tl_line.split(':')[1].strip()
                                    return pid, process_name
                        except Exception as e:
                            logger.debug(f"Failed to parse netstat: {e}")

        return None, None
    except Exception as e:
        logger.warning(f"Failed to find process on port {port}: {e}")
        return None, None


def kill_process(pid):
    """Terminate process by PID."""
    try:
        subprocess.run(
            f'taskkill /PID {pid} /F',
            shell=True,
            capture_output=True,
            timeout=5
        )
        time.sleep(1)  # Give OS time to release port
        return not is_port_in_use(PORT)
    except Exception as e:
        logger.error(f"Failed to kill process {pid}: {e}")
        return False


def find_available_port(start=5001, max_attempts=10):
    """Find next available port starting from start."""
    for port in range(start, start + max_attempts):
        if not is_port_in_use(port):
            return port
    return None


def offer_port_conflict_resolution(port, pid, process_name):
    """Interactive prompt for port conflict resolution."""
    print(f"\n{'='*60}")
    print(f"WARNING: Port {port} is already in use!")
    print(f"Process: {process_name} (PID: {pid})")
    print(f"{'='*60}")

    # Offer appropriate options based on process type
    if pid and ("python" in process_name.lower() or "streamlit" in process_name.lower()):
        print("\nThis might be a previous instance of Fischer App.")
        print("\nOptions:")
        print("  1. Stop the existing process and restart (recommended)")
        print("  2. Use a different port")
        print("  3. Exit launcher")

        choice = input("\nYour choice (1/2/3): ").strip()

        if choice == "1":
            logger.info(f"User chose to kill existing process {pid}")
            return ("kill", port)
        elif choice == "2":
            new_port = find_available_port(start=5001)
            if new_port:
                logger.info(f"User chose to use alternative port {new_port}")
                return ("change_port", new_port)
            else:
                print("\nError: Could not find available port")
                logger.error("No available ports found")
                return ("exit", None)
        else:
            logger.info("User chose to exit due to port conflict")
            return ("exit", None)
    else:
        print(f"\nAnother application is using port {port}.")
        print("\nOptions:")
        print("  1. Use a different port")
        print("  2. Exit launcher")

        choice = input("\nYour choice (1/2): ").strip()

        if choice == "1":
            new_port = find_available_port(start=5001)
            if new_port:
                logger.info(f"User chose to use alternative port {new_port}")
                return ("change_port", new_port)
            else:
                print("\nError: Could not find available port")
                logger.error("No available ports found")
                return ("exit", None)
        else:
            logger.info("User chose to exit due to port conflict")
            return ("exit", None)


# ============================================================================
# STREAMLIT MANAGEMENT
# ============================================================================

def launch_streamlit(port):
    """Launch Streamlit server subprocess."""
    logger.info(f"Launching Streamlit on port {port}...")

    try:
        # Build command
        cmd = [
            str(STREAMLIT_EXE),
            "run",
            str(APP_FILE),
            "--server.port", str(port),
            "--server.headless", "false",
            "--server.address", "localhost"
        ]

        # Create process with hidden console
        CREATE_NO_WINDOW = 0x08000000
        process = subprocess.Popen(
            cmd,
            creationflags=CREATE_NO_WINDOW if sys.platform == 'win32' else 0,
            cwd=str(PROJECT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        logger.info(f"[OK] Streamlit process started (PID: {process.pid})")
        return process, True
    except Exception as e:
        error = f"Failed to start Streamlit: {e}"
        logger.error(error)
        return None, False


def monitor_process(process):
    """Check if process is still running."""
    return process.poll() is None


def wait_for_server_ready(url, timeout=30, initial_delay=0.5):
    """Poll server until it responds or timeout."""
    logger.info(f"Waiting for server ready (timeout: {timeout}s)...")

    try:
        import requests
    except ImportError:
        logger.warning("requests not installed - skipping server health check")
        time.sleep(3)
        return True

    start_time = time.time()
    current_delay = initial_delay

    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                logger.info("[OK] Server is ready")
                return True
        except requests.exceptions.RequestException:
            pass  # Server not ready yet

        time.sleep(current_delay)
        # Exponential backoff (0.5s → 1s → 2s → 2s...)
        current_delay = min(current_delay * 2, 2.0)

    logger.error(f"Server failed to become ready within {timeout} seconds")
    return False


# ============================================================================
# BROWSER MANAGEMENT
# ============================================================================

def open_browser(url, max_retries=3):
    """Open default browser to application URL."""
    logger.info(f"Opening browser to {url}...")

    for attempt in range(max_retries):
        try:
            webbrowser.open_new_tab(url)
            logger.info("[OK] Browser opened successfully")
            return True
        except Exception as e:
            logger.warning(f"Browser open attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(1)

    logger.warning("Failed to auto-open browser")
    return False


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main launcher orchestration."""

    # Phase 1: Setup & Logging
    logger.info("=" * 70)
    logger.info("Fischer Data Processing App Launcher Started")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info(f"Project Directory: {PROJECT_DIR}")
    logger.info(f"Log File: {LOG_FILE}")
    logger.info("=" * 70)

    # Phase 2: Environment Validation
    logger.info("\nPhase 1: Environment Validation")
    logger.info("-" * 70)

    success, error = validate_environment()
    if not success:
        logger.error(f"Environment validation failed: {error}")
        show_error_dialog(
            "Fischer App - Environment Error",
            f"Environment validation failed:\n\n{error}\n\n"
            f"Please ensure Python 3.11+ and Streamlit are properly installed.",
            log_path=str(LOG_FILE)
        )
        return 1

    # Validate dependencies
    success, missing = validate_dependencies()
    if not success:
        logger.error(f"Missing dependencies: {', '.join(missing)}")
        show_error_dialog(
            "Fischer App - Dependency Error",
            f"Missing required packages:\n\n{', '.join(missing)}\n\n"
            f"Please run: pip install -r requirements.txt",
            log_path=str(LOG_FILE)
        )
        return 1

    # Check API key (non-fatal)
    success, error = check_api_key()
    if not success:
        logger.warning(f"API key check: {error}")

    # Phase 3: Port Management
    logger.info("\nPhase 2: Port Availability Check")
    logger.info("-" * 70)

    current_port = PORT

    if is_port_in_use(current_port):
        pid, process_name = find_process_on_port(current_port)
        logger.warning(f"Port {current_port} in use by {process_name} (PID: {pid})")

        action, new_port = offer_port_conflict_resolution(current_port, pid, process_name)

        if action == "kill":
            logger.info(f"Attempting to kill process {pid}...")
            if kill_process(pid):
                logger.info(f"[OK] Process {pid} terminated")
                time.sleep(1)
            else:
                logger.error(f"Failed to terminate process {pid}")
                return 1
        elif action == "change_port":
            current_port = new_port
            logger.info(f"[OK] Using alternative port: {current_port}")
        elif action == "exit":
            logger.info("User chose to exit")
            return 0
    else:
        logger.info(f"[OK] Port {current_port} available")

    # Phase 4: Launch Streamlit
    logger.info("\nPhase 3: Launching Streamlit Server")
    logger.info("-" * 70)

    process, success = launch_streamlit(current_port)
    if not success or process is None:
        logger.error("Failed to start Streamlit process")
        show_error_dialog(
            "Fischer App - Launch Error",
            "Failed to start Streamlit server.\n\n"
            "Check the log file for details.",
            log_path=str(LOG_FILE)
        )
        return 1

    # Phase 5: Wait for Server Ready
    logger.info("\nPhase 4: Waiting for Server Ready")
    logger.info("-" * 70)

    app_url = f"http://localhost:{current_port}"
    if not wait_for_server_ready(app_url, timeout=30):
        logger.error("Server failed to become ready")
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()

        show_error_dialog(
            "Fischer App - Startup Timeout",
            "Server failed to start within 30 seconds.\n\n"
            "Check the log file for details.",
            log_path=str(LOG_FILE)
        )
        return 1

    # Phase 6: Open Browser
    logger.info("\nPhase 5: Opening Browser")
    logger.info("-" * 70)

    browser_opened = open_browser(app_url)

    if not browser_opened:
        show_success_dialog(
            f"Fischer Data Processing App is running!\n\n"
            f"Please manually open:\n{app_url}\n\n"
            f"Keep this console window open while using the app."
        )
    else:
        show_success_dialog(
            f"Fischer Data Processing App is running!\n\n"
            f"URL: {app_url}\n\n"
            f"Keep this console window open while using the app.\n"
            f"Press Ctrl+C in console to stop the application."
        )

    # Phase 7: Monitor Process
    logger.info("\nPhase 6: Monitoring Application")
    logger.info("-" * 70)
    logger.info("Application is running. Press Ctrl+C to stop.")

    try:
        while monitor_process(process):
            time.sleep(5)

        logger.warning("Streamlit process terminated unexpectedly")
        show_error_dialog(
            "Fischer App - Unexpected Shutdown",
            "The application stopped unexpectedly.\n\n"
            "Check the log file for details.",
            log_path=str(LOG_FILE)
        )
        return 1
    except KeyboardInterrupt:
        logger.info("\nShutdown requested by user (Ctrl+C)")
        print("\n\nShutting down Fischer Data Processing App...")

        try:
            process.terminate()
            process.wait(timeout=5)
            logger.info("[OK] Application stopped cleanly")
        except subprocess.TimeoutExpired:
            logger.warning("Force-killing process (graceful termination timed out)")
            process.kill()
            process.wait()

        logger.info("=" * 70)
        logger.info("Fischer Data Processing App Launcher Stopped")
        logger.info("=" * 70)
        return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        show_error_dialog(
            "Fischer App - Unexpected Error",
            f"An unexpected error occurred:\n\n{e}\n\n"
            f"Check the log file for details.",
            log_path=str(LOG_FILE)
        )
        sys.exit(1)
