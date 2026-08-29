"""
=============================================================================
📱 UDISE+ 24/7 AUTONOMOUS CLOUD CRAWLER & MANAGER (RENDER ENGINE)
=============================================================================
Features:
- 24/7 Self-Ping Keep-Alive Daemon (Prevents Sleep on Render Free Tier)
- Start / Stop Cloud Crawler on Demand
- Live Real-Time Extraction Statistics
- Built-in Files Browser (View any school JSON live in UI)
- 1-Click "Download All JSONs (ZIP Archive)"
- Single School JSON Downloader
=============================================================================
"""

import os
import sys
import time
import json
import io
import zipfile
import threading
import requests
from flask import Flask, render_template, jsonify, send_file, request, Response

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(BASE_DIR, "json_schools")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "crawler_checkpoint.json")
LOG_FILE = os.path.join(BASE_DIR, "crawler_activity.log")
CLUSTERS_CSV = os.path.join(BASE_DIR, "udise_clusters.csv")
if not os.path.exists(CLUSTERS_CSV):
    CLUSTERS_CSV = os.path.join(BASE_DIR, "all_india_village_ward_school_counts.csv")

os.makedirs(JSON_DIR, exist_ok=True)

# Crawler Lifecycle State
crawler_thread = None
is_crawler_running = False
crawler_stop_signal = threading.Event()

def run_crawler_process():
    global is_crawler_running
    try:
        from cluster_crawler import run_distributed_crawler
        is_crawler_running = True
        run_distributed_crawler()
    except Exception as e:
        print(f"Crawler error: {e}")
    finally:
        is_crawler_running = False

# Auto-start on cloud boot
try:
    crawler_thread = threading.Thread(target=run_crawler_process, daemon=True)
    crawler_thread.start()
    is_crawler_running = True
    print("🚀 Cloud Crawler background thread launched successfully!")
except Exception as e:
    print(f"Auto-start exception: {e}")

# =============================================================================
# 💓 24/7 KEEP-ALIVE DAEMON (PINGS ITSELF EVERY 3 MINUTES SO IT NEVER SLEEPS)
# =============================================================================
def keep_alive_worker():
    time.sleep(30)
    my_url = os.environ.get("RENDER_EXTERNAL_URL", "https://udise-cloud-crawler.onrender.com")
    while True:
        try:
            time.sleep(180) # Every 3 minutes
            res = requests.get(f"{my_url}/api/stats", timeout=15)
            if res.status_code == 200:
                print("💓 [Keep-Alive] Ping OK! Render cloud container staying 24/7 awake.")
        except Exception as e:
            print(f"Keep-Alive ping notice: {e}")

threading.Thread(target=keep_alive_worker, daemon=True).start()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/stats")
def get_stats():
    json_files = os.listdir(JSON_DIR) if os.path.exists(JSON_DIR) else []
    total_json_files = len([f for f in json_files if f.endswith(".json")])
    
    ckpt_data = {}
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                ckpt_data = json.load(f)
        except Exception:
            pass
            
    total_clusters = ckpt_data.get("total_clusters", 581128)
    workers = ckpt_data.get("workers", {})
    
    completed_clusters = 0
    active_workers = 0
    for w in workers.values():
        completed_clusters += (w.get("current", 0) - w.get("start", 0))
        if not w.get("done", False):
            active_workers += 1
            
    progress_pct = (completed_clusters / total_clusters * 100) if total_clusters else 0
    
    logs = []
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                logs = [line.strip() for line in f.readlines()[-12:]]
        except Exception:
            pass
            
    return jsonify({
        "status": "running" if is_crawler_running else "stopped",
        "total_schools_saved": total_json_files,
        "completed_clusters": completed_clusters,
        "total_clusters": total_clusters,
        "progress_pct": round(progress_pct, 2),
        "active_workers": active_workers if is_crawler_running else 0,
        "total_workers": len(workers) or 100,
        "timestamp": ckpt_data.get("timestamp", time.strftime("%Y-%m-%d %H:%M:%S")),
        "recent_logs": logs
    })

@app.route("/api/crawler/start", methods=["POST"])
def start_crawler():
    global crawler_thread, is_crawler_running
    if is_crawler_running:
        return jsonify({"success": True, "message": "Crawler is already running!"})
        
    crawler_stop_signal.clear()
    crawler_thread = threading.Thread(target=run_crawler_process, daemon=True)
    crawler_thread.start()
    is_crawler_running = True
    return jsonify({"success": True, "message": "Crawler started successfully!"})

@app.route("/api/crawler/stop", methods=["POST"])
def stop_crawler():
    global is_crawler_running
    is_crawler_running = False
    crawler_stop_signal.set()
    return jsonify({"success": True, "message": "Crawler stopped!"})

@app.route("/api/files")
def list_files():
    """Returns list of recent extracted school files for UI file browser"""
    if not os.path.exists(JSON_DIR):
        return jsonify({"total_files": 0, "recent_files": []})
        
    all_files = [f for f in os.listdir(JSON_DIR) if f.endswith(".json")]
    all_files.sort(key=lambda f: os.path.getmtime(os.path.join(JSON_DIR, f)), reverse=True)
    
    recent = all_files[:50]
    file_list = []
    for fname in recent:
        fpath = os.path.join(JSON_DIR, fname)
        try:
            sz = os.path.getsize(fpath)
            code = fname.replace("school_", "").replace(".json", "")
            file_list.append({
                "filename": fname,
                "udise_code": code,
                "size_kb": round(sz / 1024, 1),
                "modified": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(fpath)))
            })
        except Exception:
            continue
            
    return jsonify({
        "total_files": len(all_files),
        "recent_files": file_list
    })

@app.route("/api/file/<filename>")
def view_single_file(filename):
    safe_name = os.path.basename(filename)
    fpath = os.path.join(JSON_DIR, safe_name)
    if os.path.exists(fpath):
        with open(fpath, "r", encoding="utf-8") as f:
            return jsonify(json.load(f))
    return jsonify({"error": "File not found"}), 404

@app.route("/download/json/<filename>")
def download_single_json(filename):
    safe_name = os.path.basename(filename)
    fpath = os.path.join(JSON_DIR, safe_name)
    if os.path.exists(fpath):
        return send_file(fpath, as_attachment=True, download_name=safe_name)
    return "File not found", 404

@app.route("/download/all-json-zip")
def download_all_zip():
    if not os.path.exists(JSON_DIR):
        return "No files found", 404
        
    files = [f for f in os.listdir(JSON_DIR) if f.endswith(".json")]
    if not files:
        return "No files extracted yet", 404
        
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fname in files:
            fpath = os.path.join(JSON_DIR, fname)
            zf.write(fpath, arcname=fname)
            
    memory_file.seek(0)
    zip_name = f"udise_all_schools_{len(files)}_json_files.zip"
    return send_file(
        memory_file,
        mimetype="application/zip",
        as_attachment=True,
        download_name=zip_name
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
