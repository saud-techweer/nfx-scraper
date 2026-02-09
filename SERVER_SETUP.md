# NFX Scraper - Simple Server Setup

## Step 1: Install (Run This Once)

```bash
# Update & install everything
sudo apt update
sudo apt install -y python3 python3-pip python3-venv xvfb screen wget

# Install Chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo apt --fix-broken install -y
```

## Step 2: Upload Your Files

Create folder and upload your scraper files:
```bash
mkdir -p ~/scraper/data/profiles
cd ~/scraper
```

Upload these files to `~/scraper/`:
- nfx_scraper.py
- profile_scraper.py
- requirements.txt

## Step 3: Setup Python

```bash
cd ~/scraper
python3 -m venv venv
source venv/bin/activate
pip install selenium
```

## Step 4: Login to NFX (One Time Only)

```bash
# Install VNC to see browser
sudo apt install -y tightvncserver xfce4
vncserver :1   # Set a password when asked

# Connect with VNC Viewer app to: YOUR_SERVER_IP:5901
# In VNC desktop, open terminal and run:
google-chrome --remote-debugging-port=9222 --user-data-dir=~/chrome-profile

# Login to signal.nfx.com in Chrome
# Close Chrome after login
# Close VNC
```

## Step 5: Run Scraper

```bash
# Start screen (keeps running after you disconnect)
screen -S scraper

# Start Chrome headless
Xvfb :99 -screen 0 1920x1080x24 &
export DISPLAY=:99
google-chrome --remote-debugging-port=9222 --user-data-dir=~/chrome-profile --headless=new &
sleep 5

# Run scraper
cd ~/scraper
source venv/bin/activate
python3 nfx_scraper.py

# Press Ctrl+A then D to detach (scraper keeps running)
```

## Useful Commands

```bash
# Come back to scraper
screen -r scraper

# Check Chrome running
curl http://localhost:9222/json/version

# Restart Chrome
pkill chrome
Xvfb :99 & export DISPLAY=:99
google-chrome --remote-debugging-port=9222 --user-data-dir=~/chrome-profile --headless=new &
```
