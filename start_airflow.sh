#!/usr/bin/env bash

# Simplified Airflow Startup Script (Tailored for your environment)
# -----------------------------------------------------------------

# Configuration
AIRFLOW_HOME="/home/jgupdogg/airflow"
CONDA_ENV_NAME="rapids-24.12"
WEBSERVER_PORT=8080

export AIRFLOW_HOME="/home/jgupdogg/airflow"

echo "=== Starting Airflow with Conda environment: $CONDA_ENV_NAME ==="

# Deactivate any active virtual environment
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "Deactivating existing virtual environment: $VIRTUAL_ENV"
    deactivate
fi

# Kill existing processes
echo "Stopping any running Airflow processes..."
pkill -f "airflow scheduler" 2>/dev/null || true
pkill -f "airflow webserver" 2>/dev/null || true
sleep 2

# Add Chrome zombie process cleanup
echo "Cleaning up any Chrome zombie processes..."
CHROME_PROCESSES=$(ps aux | grep -i chrome | grep -v grep | awk '{print $2}')
if [ -n "$CHROME_PROCESSES" ]; then
    echo "$CHROME_PROCESSES" | xargs -r kill -9
    echo "Chrome processes killed."
fi

# Clean up nodriver user_data_dir if it exists
USER_DATA_DIRS=$(find /tmp -name "nodriver*" -type d 2>/dev/null)
if [ -n "$USER_DATA_DIRS" ]; then
    echo "Cleaning up temporary nodriver directories..."
    echo "$USER_DATA_DIRS" | xargs -r rm -rf
fi

# Activate Conda environment
echo "Activating Conda environment..."
source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda activate "$CONDA_ENV_NAME"

# Verify Python and environment
PYTHON_VERSION=$(python --version)
echo "Using Python version: $PYTHON_VERSION"

# Generate and set Fernet key if needed
if ! grep -q "^fernet_key =" "$AIRFLOW_HOME/airflow.cfg" || grep -q "^fernet_key =$" "$AIRFLOW_HOME/airflow.cfg"; then
    echo "Generating a new Fernet key for encryption..."
    # Install cryptography if needed
    if ! python -c "import cryptography" 2>/dev/null; then
        pip install cryptography
    fi
    
    # Generate a Fernet key
    FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
    
    # Update the configuration
    if grep -q "^fernet_key =" "$AIRFLOW_HOME/airflow.cfg"; then
        sed -i "s|^fernet_key =.*|fernet_key = $FERNET_KEY|" "$AIRFLOW_HOME/airflow.cfg"
    else
        echo "fernet_key = $FERNET_KEY" >> "$AIRFLOW_HOME/airflow.cfg"
    fi
    echo "Fernet key set successfully."
else
    echo "Fernet key already set."
fi

# Set up all required connections
echo "Setting up required connections..."

# 1. Pipeline DB connection
airflow connections delete pipeline_db 2>/dev/null || true
airflow connections add 'pipeline_db' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-login 'your_username' \
    --conn-password 'your_password' \
    --conn-port '5432' \
    --conn-schema 'your_database'

# 2. Birdseye API connection (needed by db_manager.py)
airflow connections delete birdseye_api 2>/dev/null || true
airflow connections add 'birdseye_api' \
    --conn-type 'http' \
    --conn-host 'api.birdeye.so' \
    --conn-port '443' \
    --conn-extra '{"key": "your_api_key_here"}'

# 3. Any other connections your application needs
# Example:
# airflow connections add 'another_connection' \
#    --conn-type 'http' \
#    --conn-host 'api.example.com' \
#    --conn-port '443'

# Set up a periodic Chrome cleanup process
echo "Setting up periodic Chrome process cleanup..."
(
    while true; do
        sleep 3600  # Run cleanup every hour
        echo "Running scheduled cleanup of Chrome processes - $(date)" >> "$AIRFLOW_HOME/chrome_cleanup.log"
        CHROME_PROCESSES=$(ps aux | grep -i chrome | grep -v grep | awk '{print $2}')
        if [ -n "$CHROME_PROCESSES" ]; then
            echo "$CHROME_PROCESSES" | xargs -r kill -9
            echo "$(date): Killed zombie Chrome processes" >> "$AIRFLOW_HOME/chrome_cleanup.log"
        fi
        
        # Clean up nodriver directories if any
        USER_DATA_DIRS=$(find /tmp -name "nodriver*" -type d 2>/dev/null)
        if [ -n "$USER_DATA_DIRS" ]; then
            echo "$USER_DATA_DIRS" | xargs -r rm -rf
            echo "$(date): Cleaned up temporary nodriver directories" >> "$AIRFLOW_HOME/chrome_cleanup.log"
        fi
    done
) &
CLEANUP_PID=$!
echo "Cleanup process started with PID: $CLEANUP_PID"

# Start scheduler
echo "Starting Airflow Scheduler..."
airflow scheduler > "$AIRFLOW_HOME/scheduler.log" 2>&1 &
echo "Scheduler started with PID: $!"

# Start Helius listener if the file exists
if [ -f "$AIRFLOW_HOME/listeners/helius_listener.py" ]; then
    echo "Starting Helius Listener..."
    python $AIRFLOW_HOME/listeners/helius_listener.py > "$AIRFLOW_HOME/helius_listener.log" 2>&1 &
    echo "Helius Listener started with PID: $!"
else
    echo "Warning: Helius listener script not found at $AIRFLOW_HOME/listeners/helius_listener.py"
fi

# Start webserver
echo "Starting Airflow Webserver on port $WEBSERVER_PORT..."
airflow webserver --port $WEBSERVER_PORT

# When webserver exits, clean up other processes
echo "Airflow Webserver has stopped. Cleaning up other processes..."
kill $CLEANUP_PID 2>/dev/null || true
pkill -f "airflow scheduler" 2>/dev/null || true

echo "Airflow services have been stopped."