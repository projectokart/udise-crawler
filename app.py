"""
=============================================================================
📱 MOBILE-FRIENDLY WEB DASHBOARD & DATA EXPORTER FOR GCP CRAWLER
=============================================================================
"""

import os
import json
import csv
import io
import zipfile
from flask import Flask, render_template, jsonify, send_file, request

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_DIR = os.path.join(BASE_DIR, "json_schools")
CHECKPOINT_FILE = os.path.join(BASE_DIR, "crawler_checkpoint.json")
LOG_FILE = os.path.join(BASE_DIR, "crawler_activity.log")

CSV_HEADERS = [
    "udise_code", "school_name", "school_id", "status", "year_desc",
    "state_name", "district_name", "block_name", "cluster_name", "village_ward",
    "panchayat_name", "panchayat_id",
    "pincode", "address", "latitude", "longitude", "assembly_constituency", "urban_local_body",
    "school_category", "category_desc", "management_type", "management_desc_state",
    "class_from", "class_to", "school_type", "rural_urban", "pm_shri",
    "headmaster_principal_name", "respondent_name", "phone", "email", "website",
    "board_secondary_10th", "board_higher_secondary_12th", "established_year",
    "recog_year_pri", "recog_year_sec", "recog_year_hsec", "medium_of_instruction_1",
    "annual_instructional_days", "residential_school", "minority_school", "pre_primary_section",
    "total_students", "total_boys", "total_girls",
    "total_teachers", "regular_teachers", "contract_teachers",
    "male_teachers", "female_teachers", "teachers_post_graduate_above",
    "teachers_graduate", "teachers_above_55_age", "teachers_in_service_trained",
    "teachers_non_teaching_assign",
    "building_status", "total_building_blocks", "classrooms_total", "other_rooms",
    "classrooms_good_condition", "classrooms_minor_repair", "classrooms_major_repair",
    "boundary_wall_type", "students_with_furniture",
    "drinking_water", "electricity", "solar_panel", "rainwater_harvesting",
    "medical_checkup", "ramps_accessible", "handrails", "library", "playground",
    "integrated_science_lab", "tinkering_lab_atl", "ict_lab", "dth_tv_access",
    "desktop_computers_working", "laptops_working", "tablets_working",
    "projectors_working", "printers_total", "digital_boards_working", "internet_available",
    "boys_toilets_functional", "girls_toilets_functional",
    "boys_urinals", "girls_urinals", "cwsn_special_toilets_boys", "cwsn_special_toilets_girls",
    "handwash_available", "meal_handwash_available"
]

def is_private_management(mgmt_str, mgmt_id):
    if not mgmt_str:
        mgmt_str = ""
    mgmt_lower = mgmt_str.lower()
    if any(k in mgmt_lower for k in ["private", "unaided", "aided", "unrecognized", "madarsa", "trust", "society"]):
        return True
    if mgmt_id in [4, 5, 8, 9, 10, 11, 12, 13, 14, 15]:
        return True
    return False

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/stats")
def get_stats():
    total_json_files = len(os.listdir(JSON_DIR)) if os.path.exists(JSON_DIR) else 0
    
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
    
    # Recent logs
    logs = []
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                logs = [line.strip() for line in f.readlines()[-10:]]
        except Exception:
            pass
            
    return jsonify({
        "total_schools_saved": total_json_files,
        "completed_clusters": completed_clusters,
        "total_clusters": total_clusters,
        "progress_pct": round(progress_pct, 2),
        "active_workers": active_workers,
        "total_workers": len(workers) or 30,
        "timestamp": ckpt_data.get("timestamp", ""),
        "recent_logs": logs
    })

@app.route("/export/csv/<category>")
def export_csv(category):
    """Generates on-the-fly streaming CSV download for mobile"""
    if not os.path.exists(JSON_DIR):
        return "No data found", 404
        
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(CSV_HEADERS)
    
    count = 0
    files = os.listdir(JSON_DIR)
    
    for fname in files:
        if not fname.endswith(".json"):
            continue
        try:
            with open(os.path.join(JSON_DIR, fname), "r", encoding="utf-8") as f:
                d = json.load(f)
                
            d1 = d.get("basic_info", {})
            d2 = d.get("profile", {})
            d3 = d.get("facilities", {})
            d4 = d.get("enrolment_and_teachers", {})
            d5 = d.get("report_card", {})
            
            mgmt_str = f"{d1.get('schCategoryType', '')} {d1.get('schMgmtType', '')} {d1.get('schMgmtDesc', '')} {d1.get('schMgmtDescSt', '')}"
            mgmt_id = d1.get("schMgmtId")
            is_pvt = is_private_management(mgmt_str, mgmt_id)
            
            if category == "private" and not is_pvt:
                continue
            if category == "govt" and is_pvt:
                continue
                
            code = d.get("udise_code", "")
            row = [
                d1.get("udiseschCode") or code, d1.get("schoolName", ""), d1.get("schoolId", ""),
                d1.get("schoolStatusName", ""), d1.get("yearDesc", ""), d1.get("stateName", ""),
                d1.get("districtName", ""), d1.get("blockName", ""), d1.get("clusterName", ""),
                d1.get("villageName") or d1.get("lgdwardName", ""),
                d1.get("lgdvillpanchayatName") or d5.get("panDesc") or "",
                d1.get("lgdpanchayatId") or "",
                d1.get("pincode", ""), d1.get("address", ""), d1.get("latitude", ""), d1.get("longitude", ""),
                d5.get("assemblyCdDesc", ""), d1.get("lgdurbanlocalbodyName", ""),
                d1.get("schCategoryType", ""), d1.get("schCatDesc", ""), d1.get("schMgmtType", ""),
                d1.get("schMgmtDescSt", ""), d1.get("classFrm", ""), d1.get("classTo", ""),
                d1.get("schTypeDesc", ""), d1.get("schLocDesc", ""), d1.get("pmShriYn", ""),
                d2.get("headMasterName", ""), d2.get("respName", ""), d2.get("schPhone", ""),
                (d2.get("email") or "").replace("[at]", "@").replace("[dot]", "."),
                d2.get("website", ""), d2.get("boardSecName", ""), d2.get("boardHighSecName", ""),
                d2.get("estdYear", ""), d2.get("recogYearPri", ""), d2.get("recogYearSec", ""),
                d2.get("recogYearHsec", ""), d2.get("mediumOfInstrName1", ""), d2.get("instructionalDays", ""),
                d2.get("resiSchDesc", ""), d2.get("minorityYnDesc", ""), d2.get("ppSecDesc", ""),
                d4.get("totalCount", ""), d4.get("totalBoy", ""), d4.get("totalGirl", ""),
                (d4.get("totalTeacherReg", 0) or 0) + (d4.get("totalTeacherCon", 0) or 0),
                d4.get("totalTeacherReg", ""), d4.get("totalTeacherCon", ""),
                d4.get("totalTeacherMale", ""), d4.get("totalTeacherFemale", ""),
                d5.get("totTchPgraduateAbove", ""), d5.get("totTchGraduateAbove", ""),
                d5.get("tchAbove55", ""), d5.get("tchRecvdServiceTrng", ""), d5.get("tchInvlovedNonTchAssign", ""),
                d3.get("bldStatus", ""), d3.get("bldBlkTot") or d3.get("bldBlk", ""),
                d3.get("clsrmsInst", ""), d3.get("othrooms", ""), d3.get("clsrmsGd", ""),
                d3.get("clsrmsMin", ""), d3.get("clsrmsMaj", ""), d3.get("bndrywallType", ""),
                d3.get("stusHvFurnt", ""), d3.get("drinkWaterYnDesc", ""), d3.get("electricityYnDesc", ""),
                d3.get("solarpanelYnDesc", ""), d3.get("rainHarvestYnDesc", ""), d3.get("medchkYnDesc", ""),
                d3.get("rampsYnDesc", ""), d3.get("handrailsYnDesc", ""), d3.get("libraryYnDesc", ""),
                d3.get("playgroundYnDesc", ""),
                "1-Yes" if d3.get("integratedLabYn") == 1 else "2-No",
                "1-Yes" if d3.get("tinkeringLabYn") == 1 else "2-No",
                d3.get("ictLabYnDesc", ""), d3.get("accessDthYnDesc", ""),
                d3.get("desktopFun", ""), d3.get("laptopFun", ""), d3.get("tabletsFun", ""),
                d3.get("projectorFun", ""), d3.get("printerTot", ""), d3.get("digiBoardFun", ""),
                d3.get("internetYnDesc", ""), d3.get("toiletbFun", ""), d3.get("toiletgFun", ""),
                d3.get("urinalsb", ""), d3.get("urinalsg", ""), d3.get("toiletbCwsnFun", ""),
                d3.get("toiletgCwsnFun", ""), d3.get("handwashYnDesc", ""), d3.get("handwashMealYnDesc", "")
            ]
            writer.writerow(row)
            count += 1
        except Exception:
            continue
            
    output.seek(0)
    filename = f"udise_{category}_schools_{count}_records.csv"
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8-sig')),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename
    )

import threading
from cluster_crawler import run_distributed_crawler

try:
    threading.Thread(target=run_distributed_crawler, daemon=True).start()
    print("🚀 Cloud Background Crawler thread started successfully!")
except Exception as e:
    print(f"⚠️ Crawler startup error: {e}")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
