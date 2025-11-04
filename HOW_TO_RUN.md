# How to Run the App and See Changes

## The Problem
You're viewing an old version of the app. Changes to the code won't appear until you restart Streamlit.

## Solution: Restart Streamlit

### Step 1: Stop the Current App
In your terminal/command prompt where Streamlit is running:
- Press **Ctrl+C** to stop it
- Or close that terminal window entirely

### Step 2: Start Fresh
Open a new terminal/command prompt and run:

```bash
cd "C:\Users\minke\OneDrive\Desktop\Fischer Data App V1"
streamlit run src/app_v5.py
```

### Step 3: Open in Browser
Streamlit will show you a URL like:
```
Local URL: http://localhost:8501
```

Open that URL in your browser (or it may open automatically).

## To See the Debug Window

Once the app is running:
1. Scroll down the page
2. You should immediately see (even with no files uploaded):

```
─────────────────────────────────────────
🔍 AI Debug Window - View API Requests & Responses ▼

  AI Detection Log
  Total API calls: 0

  ℹ️ No AI API calls yet. Click 'Auto-Detect' on a file to see debug information.
─────────────────────────────────────────
```

It will be **expanded** (not collapsed) and visible right away.

## Quick Test
If you can't find the debug window even after restarting, run this test:

```bash
cd "C:\Users\minke\OneDrive\Desktop\Fischer Data App V1"
python -c "print('AI DEBUG WINDOW' in open('src/app_v5.py').read())"
```

Should print: `True`

## Common Issues

### "I restarted but still don't see it"
- Make sure you're viewing http://localhost:8501 (or whatever port Streamlit shows)
- Hard refresh the browser: Ctrl+Shift+R (Windows) or Cmd+Shift+R (Mac)
- Check you're running app_v5.py not app.py or another version

### "Port is already in use"
Kill the old process:
1. Open Task Manager (Ctrl+Shift+Esc)
2. Find "python.exe" processes
3. End the one running Streamlit
4. Try again

### "Still shows old version"
Clear Streamlit cache:
```bash
streamlit cache clear
streamlit run src/app_v5.py
```

## Current Running Processes (as of last check)
- Port 8501: Running (PID 36768) - **This is what you're viewing**
- Port 8502: Running (PID 31236)

You need to restart the app on 8501 to see changes!