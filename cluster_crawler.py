"""
=============================================================================
🏛️ GCP OPTIMIZED UDISE+ CLUSTER CRAWLER (1GB RAM SAFE)
=============================================================================
- Optimized for Google Cloud e2-micro (1 vCPU, 1GB RAM, 30GB SSD)
- Memory-Safe (Keeps RAM usage under 150MB)
- 30 Concurrent Threads (Best throughput for 1 vCPU without throttling)
- Full 5-Endpoint JSON Extraction per School
- Auto-Checkpoint & Exact Resume
- Network Auto-Recovery
=============================================================================
"""

import os
import sys
import time
import json
import csv
import threading
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

NUM_WORKERS = 30
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLUSTERS_CSV = os.path.join(BASE_DIR, "udise_clusters.csv")
JSON_DIR = os.path.join(BASE_DIR, "json_schools")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "crawler_checkpoint.json")
LOG_FILE = os.path.join(BASE_DIR, "crawler_activity.log")

HTTP_HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36',
    'x-app-signature': '9f2c7a4b8e1d6c3f5a9b0e2d4f6a7c8b',
    'Referer': 'https://kys.udiseplus.gov.in/',
    'Origin': 'https://kys.udiseplus.gov.in'
}

session = requests.Session()
adapter = HTTPAdapter(pool_connections=60, pool_maxsize=100, max_retries=Retry(total=2, backoff_factor=0.2))
session.mount("https://", adapter)
session.mount("http://", adapter)

checkpoint_lock = threading.Lock()
log_lock = threading.Lock()
network_lock = threading.Lock()
is_network_down = False

def log(msg):
    t = time.strftime("[%Y-%m-%d %H:%M:%S]")
    line = f"{t} {msg}"
    print(line)
    with log_lock:
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

def check_and_wait_for_internet():
    global is_network_down
    with network_lock:
        while True:
            try:
                r = requests.get("https://1.1.1.1", timeout=3)
                if r.status_code:
                    if is_network_down:
                        log("🌐 Network reconnected! Resuming crawler...")
                        is_network_down = False
                    return
            except Exception:
                is_network_down = True
                log("⚠️ Network disconnected! Waiting 10s...")
                time.sleep(10)

def safe_api_get(url, timeout=5):
    while True:
        try:
            return session.get(url, headers=HTTP_HEADERS, timeout=timeout)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            check_and_wait_for_internet()
        except Exception:
            return None

def init_environment():
    os.makedirs(JSON_DIR, exist_ok=True)

def load_or_create_checkpoint(total_clusters):
    chunk_size = math.ceil(total_clusters / NUM_WORKERS)
    
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data.get("num_workers") == NUM_WORKERS and "workers" in data:
                    return data
        except Exception:
            pass
            
    workers = {}
    for i in range(NUM_WORKERS):
        start = i * chunk_size
        end = min((i + 1) * chunk_size, total_clusters)
        workers[str(i)] = {
            "start": start,
            "end": end,
            "current": start,
            "done": start >= total_clusters
        }
        
    initial_ckpt = {
        "total_clusters": total_clusters,
        "num_workers": NUM_WORKERS,
        "chunk_size": chunk_size,
        "total_schools_found": 0,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "workers": workers
    }
    
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(initial_ckpt, f, indent=2)
        
    return initial_ckpt

def update_worker_checkpoint(ckpt_data, worker_id, current_idx, is_done, new_schools=0):
    with checkpoint_lock:
        try:
            w = ckpt_data["workers"][str(worker_id)]
            w["current"] = current_idx
            w["done"] = is_done
            ckpt_data["total_schools_found"] += new_schools
            ckpt_data["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
            
            tmp_file = CHECKPOINT_FILE + ".tmp"
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(ckpt_data, f, indent=2)
            os.replace(tmp_file, CHECKPOINT_FILE)
        except Exception:
            pass

def fetch_full_school_data(code):
    base_url = "https://kys.udiseplus.gov.in/web-app/api"
    try:
        r1 = safe_api_get(f"{base_url}/school/by-year?udiseSchCode={code}&action=1", timeout=6)
        if not r1 or r1.status_code != 200:
            return None
        j1 = r1.json()
        if not j1.get("status"):
            return None
        d1 = j1.get("data", {})
        if not d1 or not d1.get("schoolName"):
            return None

        d2 = {}
        r2 = safe_api_get(f"{base_url}/school/profile?udiseSchCode={code}", timeout=5)
        if r2 and r2.status_code == 200:
            d2 = r2.json().get("data", {}) or {}

        d3 = {}
        r3 = safe_api_get(f"{base_url}/school/facility?udiseSchCode={code}", timeout=5)
        if r3 and r3.status_code == 200:
            d3 = r3.json().get("data", {}) or {}

        d4 = {}
        r4 = safe_api_get(f"{base_url}/school-statistics/enrolment-teacher?udiseSchCode={code}", timeout=5)
        if r4 and r4.status_code == 200:
            d4 = r4.json().get("data", {}) or {}

        d5 = {}
        r5 = safe_api_get(f"{base_url}/school/report-card?udiseSchCode={code}", timeout=5)
        if r5 and r5.status_code == 200:
            d5 = r5.json().get("data", {}) or {}

        full_json = {
            "udise_code": code,
            "school_id": d1.get("schoolId"),
            "school_name": d1.get("schoolName"),
            "basic_info": d1,
            "profile": d2,
            "facilities": d3,
            "enrolment_and_teachers": d4,
            "report_card": d5,
            "extracted_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        return full_json
    except Exception:
        return None

def save_json_file(full_json):
    code = full_json["udise_code"]
    json_path = os.path.join(JSON_DIR, f"school_{code}.json")
    try:
        with open(json_path, "w", encoding="utf-8") as jf:
            json.dump(full_json, jf, indent=2)
        return True
    except Exception:
        return False

def process_single_cluster(cluster_info, worker_id):
    cluster_code = str(cluster_info["cluster_code"]).zfill(9)
    try:
        max_known_id = int(cluster_info.get("max_school_id", 1))
    except (ValueError, TypeError):
        max_known_id = 1
        
    school_id = 1
    consecutive_misses = 0
    found_count = 0
    
    while consecutive_misses < 3 and school_id <= 99:
        code = f"{cluster_code}{school_id:02d}"
        full_json = fetch_full_school_data(code)
        
        if full_json:
            found_count += 1
            consecutive_misses = 0
            save_json_file(full_json)
        else:
            if school_id >= max_known_id:
                consecutive_misses += 1
                
        school_id += 1
        time.sleep(0.02)
        
    return found_count

def worker_thread_task(worker_id, clusters, ckpt_data):
    w_info = ckpt_data["workers"][str(worker_id)]
    start_idx = w_info["current"]
    end_idx = w_info["end"]
    
    if w_info["done"] or start_idx >= end_idx:
        return
        
    log(f"[Worker #{worker_id:02d}] 🚀 Started partition [{start_idx} to {end_idx}]")
    
    for idx in range(start_idx, end_idx):
        cluster_info = clusters[idx]
        schools_found = process_single_cluster(cluster_info, worker_id)
        
        is_done = (idx + 1) >= end_idx
        update_worker_checkpoint(ckpt_data, worker_id, idx + 1, is_done, new_schools=schools_found)
        
        if (idx - start_idx + 1) % 50 == 0 or is_done:
            progress_pct = ((idx + 1 - w_info['start']) / (end_idx - w_info['start'])) * 100
            log(f"[Worker #{worker_id:02d}] Progress: {idx + 1}/{end_idx} ({progress_pct:.1f}%) | Code: {cluster_info.get('cluster_code')}")
            
    log(f"[Worker #{worker_id:02d}] 🏁 Finished its chunk [{w_info['start']}-{end_idx}].")

def run_distributed_crawler():
    init_environment()
    
    with open(CLUSTERS_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        clusters = list(reader)
        
    total_clusters = len(clusters)
    ckpt_data = load_or_create_checkpoint(total_clusters)
    
    active_workers = sum(1 for w in ckpt_data["workers"].values() if not w["done"])
    log(f"==================================================================")
    log(f"🚀 GCP UDISE CRAWLER INITIALIZED")
    log(f"   Total Clusters: {total_clusters}")
    log(f"   Workers:        {NUM_WORKERS} Threads (~{ckpt_data['chunk_size']} clusters each)")
    log(f"   Active Workers: {active_workers}/{NUM_WORKERS}")
    log(f"   Schools Saved:  {ckpt_data.get('total_schools_found', 0)}")
    log(f"==================================================================")
    
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = []
        for i in range(NUM_WORKERS):
            futures.append(executor.submit(worker_thread_task, i, clusters, ckpt_data))
            
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log(f"⚠️ Worker error: {e}")
                
    log("🎉 ALL WORKERS COMPLETED ALL CLUSTERS! Full extraction finished.")

if __name__ == "__main__":
    while True:
        try:
            run_distributed_crawler()
            break
        except Exception as e:
            log(f"⚠️ Master process exception: {e}. Retrying in 5s...")
            time.sleep(5)
