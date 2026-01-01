from flask import Flask, render_template, request, jsonify
import pandas as pd
import os
import sys
from datetime import datetime

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data import employees, parts
import storage

app = Flask(__name__, template_folder='../templates', static_folder='../static')

# --- Configuration ---
FILES = {
    'standard_times': 'wp_data.csv'
}

# --- Helper: Load Standard Times ---
def get_wp_data_path():
    """Get path to wp_data.csv."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, FILES['standard_times'])

try:
    wp_data = pd.read_csv(get_wp_data_path())
    header = wp_data.columns.tolist() 
except:
    wp_data = pd.DataFrame()
    header = []

@app.route("/")
def index():
    return render_template("index.html", employees=employees, parts=parts, header=header[1:])

# --- ATTENDANCE ---

@app.route("/mark_attendance", methods=["POST"])
def mark_attendance():
    data = request.get_json()
    date = data.pop('date')
    shift = data.pop('shift')
    
    # Convert remaining data to attendance dict
    attendance_dict = {}
    present_count = 0
    for emp_id, present in data.items():
        attendance_dict[emp_id] = present
        if present:
            present_count += 1
    
    # Save using storage module
    storage.save_attendance(date, shift, attendance_dict)
    
    # Calculate stats
    total_employees = len(employees)
    attendance_pct = (present_count / total_employees * 100) if total_employees > 0 else 0

    return jsonify({
        "status": "success",
        "count": int(present_count),
        "total": int(total_employees),
        "attendance_pct": round(float(attendance_pct), 1),
        "date": date,
        "shift": shift
    })

@app.route("/get_attendance", methods=["GET"])
def get_attendance():
    date = request.args.get('date')
    shift = request.args.get('shift')
    
    attendance = storage.get_attendance(date, shift)
    return jsonify(attendance)

# --- PRODUCTION PLAN & SAVING ---
@app.route("/plan_production", methods=["POST"])
def plan_production():
    data = request.get_json()
    selected_parts = data["parts"]
    date = data['date']
    shift = data['shift']
    
    # Get present employee IDs using storage module
    present_ids = storage.get_present_employees(date, shift)

    present_employees = [
        {"id": eid, "name": emp["name"], "efficiency": emp["efficiency"], "trained_skills": emp["trained_skills"]}
        for eid, emp in employees.items() if eid in present_ids
    ]
    
    # Fallback: if no attendance data, use all employees
    if not present_employees:
        present_employees = [
            {"id": eid, "name": emp["name"], "efficiency": emp["efficiency"], "trained_skills": emp["trained_skills"]}
            for eid, emp in employees.items()
        ]

    total_tasks = []
    for item in selected_parts:
        part_id = item["part_id"]
        qty = int(item["quantity"])
        area = item["work_area"]
        time_per_unit_min = wp_data.loc[wp_data[header[0]] == part_id, area].values
        part = parts[part_id]
        total_minutes = time_per_unit_min[0] * qty if len(time_per_unit_min) > 0 else 0
        total_tasks.append({
            "part_name": part["name"],
            "part_id": part_id,
            "quantity": qty,
            "work_area": area,
            "total_minutes": total_minutes,
            "time_per_unit": time_per_unit_min[0] if len(time_per_unit_min) > 0 else 0
        })

    assignments = []
    log_entries = []
    total_tasks.sort(key=lambda x: x["total_minutes"], reverse=True)
    assignment_employees = present_employees.copy()
    
    # Helper to normalize skill names
    def normalize_skill(skill):
        return skill.strip().lower().replace('_', ' ').replace('  ', ' ')
    
    def has_skill(emp, work_area):
        normalized_area = normalize_skill(work_area)
        skills = [normalize_skill(s) for s in emp["trained_skills"].split(",") if s.strip()]
        return normalized_area in skills

    for task in total_tasks:
        task_assignments = []
        best_operator = None
        support_operator = None
        
        sorted_employees = sorted(assignment_employees, key=lambda x: x["efficiency"], reverse=True)
        skilled_employees = [emp for emp in sorted_employees if has_skill(emp, task['work_area'])]
        
        if skilled_employees:
            best_operator = skilled_employees[0]
        elif sorted_employees:
            best_operator = sorted_employees[0]

        if best_operator:
            assignment_employees.remove(best_operator)
            sorted_employees = sorted(assignment_employees, key=lambda x: x["efficiency"], reverse=True)
            skilled_employees = [emp for emp in sorted_employees if has_skill(emp, task['work_area'])]

        if skilled_employees:
            support_operator = skilled_employees[-1]
        elif sorted_employees:
            support_operator = sorted_employees[-1]

        if support_operator:
            assignment_employees.remove(support_operator)

        task_assignments.append({
            "best_operator": best_operator["name"] if best_operator else "Unassigned",
            "support_operator": support_operator["name"] if support_operator else "Unassigned"
        })

        assignments.append({
            "part": task["part_name"],
            "part_id": task["part_id"],
            "quantity": task["quantity"],
            "work_area": task["work_area"],
            "operators": task_assignments
        })

        log_entries.append({
            'date': date,
            'shift': shift,
            'part_id': task["part_id"],
            'work_area': task["work_area"],
            'plan_qty': task["quantity"],
            'actual_qty': 0,
            'efficiency': 0
        })

    # Save using storage module
    storage.save_production_plan(log_entries)

    return jsonify({"assignments": assignments, "present_count": len(present_employees)})


@app.route("/update_production_actual", methods=["POST"])
def update_production_actual():
    data = request.get_json()
    date = data['date']
    shift = data['shift']
    part_id = data['part_id']
    area = data['work_area']
    actual = float(data['actual'])
    plan = float(data['plan'])
    
    efficiency = (actual / plan * 100) if plan > 0 else 0

    # Update using storage module
    updated = storage.update_production_actual(date, shift, part_id, area, actual, efficiency)
    
    if updated:
        return jsonify({"status": "updated", "efficiency": efficiency})
    return jsonify({"status": "not found"})

# --- MATERIAL ---
@app.route("/save_material", methods=["POST"])
def save_material():
    data = request.get_json()
    date_str = data.get("date")
    materials = data.get("materials", [])
    
    rows = []
    area_efficiencies = {}
    
    for item in materials:
        eff_value = float(item['efficiency'].replace('%',''))
        work_area = item['work_area']
        
        rows.append({
            'date': date_str,
            'program': item['program'],
            'part_id': item['part_id'],
            'work_area': work_area,
            'qty': item['qty'],
            'req': item['req'],
            'actual': item['actual'],
            'efficiency': eff_value
        })
        
        if work_area not in area_efficiencies:
            area_efficiencies[work_area] = []
        area_efficiencies[work_area].append(eff_value)
    
    # Save using storage module
    storage.save_materials(rows)
    
    work_area_avg = {}
    for area, effs in area_efficiencies.items():
        work_area_avg[area] = sum(effs) / len(effs) if effs else 0
            
    return jsonify({"status": "success", "count": len(rows), "efficiencies": work_area_avg})    

# --- DASHBOARD DATA AGGREGATION ---
@app.route("/get_dashboard_data")
def get_dashboard_data():
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    shift = request.args.get('shift')
    
    response_data = {}
    work_areas = ['Autoclave', 'CCA', 'PAA', 'Paint_Booth', 'Prefit']

    # Load data using storage module
    prod_records = storage.get_production(date, shift)
    mat_records = storage.get_materials(date)

    for area in work_areas:
        area_clean = area.replace('_', ' ').lower()
        
        prod_effs = []
        mat_effs = []

        for r in prod_records:
            r_area = str(r.get('work_area', '')).lower().replace('_', ' ')
            if r_area == area_clean:
                prod_effs.append(r.get('efficiency', 0))

        for r in mat_records:
            r_area = str(r.get('work_area', '')).lower().replace('_', ' ')
            if r_area == area_clean:
                mat_effs.append(r.get('efficiency', 0))

        all_effs = prod_effs + mat_effs
        avg_eff = sum(all_effs) / len(all_effs) if all_effs else 0
        
        response_data[area] = avg_eff

    # Attendance
    total_employees = len(employees)
    present_count = storage.count_present(date, shift) if shift else 0
    
    # If no shift specified, count for both shifts
    if not shift:
        present_count = max(
            storage.count_present(date, 'Day'),
            storage.count_present(date, 'Night')
        )

    attendance_pct = (present_count / total_employees * 100) if total_employees > 0 else 0
    response_data['attendance_present'] = int(present_count)
    response_data['attendance_total'] = int(total_employees)
    response_data['attendance_pct'] = round(float(attendance_pct), 1)

    return jsonify(response_data)

# For local development
if __name__ == "__main__":
    app.run(debug=True)

# For Vercel serverless
app = app
