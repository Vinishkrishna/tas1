from flask import Flask, render_template, request, jsonify
import pandas as pd
import os
import sys
from datetime import datetime

# Add parent directory to path to import data module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data import employees, parts

app = Flask(__name__, template_folder='../templates', static_folder='../static')

# --- Configuration ---
# Use /tmp for writable files on Vercel (serverless), fallback to current dir for local dev
def get_file_path(filename):
    """Get the appropriate file path based on environment."""
    if os.environ.get('VERCEL'):
        return f'/tmp/{filename}'
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), filename)

FILES = {
    'attendance': 'attendance_log.csv',
    'production': 'production_log.csv',
    'material': 'material_log.csv',
    'standard_times': 'wp_data.csv'
}

# --- Helper: Init CSVs ---
def init_csvs():
    att_path = get_file_path(FILES['attendance'])
    prod_path = get_file_path(FILES['production'])
    mat_path = get_file_path(FILES['material'])
    
    if not os.path.exists(att_path):
        pd.DataFrame(columns=['date', 'shift', 'emp_id', 'present']).to_csv(att_path, index=False)
    if not os.path.exists(prod_path):
        pd.DataFrame(columns=['date', 'shift', 'part_id', 'work_area', 'plan_qty', 'actual_qty', 'efficiency']).to_csv(prod_path, index=False)
    if not os.path.exists(mat_path):
        pd.DataFrame(columns=['date', 'program', 'part_id', 'work_area', 'qty', 'req', 'actual', 'efficiency']).to_csv(mat_path, index=False)

init_csvs()

# --- Helper: Load Standard Times ---
def get_wp_data_path():
    """Get path to wp_data.csv - check both locations."""
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
    
    new_rows = []
    present_count = 0
    for emp_id, present in data.items():
        if present:
            present_count += 1
        new_rows.append({'date': date, 'shift': shift, 'emp_id': emp_id, 'present': present})
    
    df = pd.DataFrame(new_rows)
    
    att_path = get_file_path(FILES['attendance'])
    
    # Remove existing entries for this date/shift before adding new ones
    if os.path.exists(att_path):
        existing_df = pd.read_csv(att_path)
        existing_df = existing_df[~((existing_df['date'] == date) & (existing_df['shift'] == shift))]
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df.to_csv(att_path, index=False)
    else:
        df.to_csv(att_path, index=False)

    # Return count based on persisted data (guards against duplicates/multiple submits)
    saved_df = pd.read_csv(att_path)
    saved_mask = (saved_df['date'] == date) & (saved_df['shift'] == shift)
    present_mask = saved_df['present'].astype(str).str.lower().eq('true')
    saved_present_count = saved_df[saved_mask & present_mask]['emp_id'].nunique()

    total_employees = len(employees)
    attendance_pct = (saved_present_count / total_employees * 100) if total_employees > 0 else 0

    return jsonify({
        "status": "success",
        "count": int(saved_present_count),
        "total": int(total_employees),
        "attendance_pct": round(float(attendance_pct), 1),
        "date": date,
        "shift": shift
    })

@app.route("/get_attendance", methods=["GET"])
def get_attendance():
    date = request.args.get('date')
    shift = request.args.get('shift')
    
    att_path = get_file_path(FILES['attendance'])
    if not os.path.exists(att_path):
        return jsonify({})

    df = pd.read_csv(att_path)
    mask = (df['date'] == date) & (df['shift'] == shift)
    filtered = df[mask]
    
    attendance_dict = dict(zip(filtered.emp_id, filtered.present))
    return jsonify(attendance_dict)

# --- PRODUCTION PLAN & SAVING ---
@app.route("/plan_production", methods=["POST"])
def plan_production():
    data = request.get_json()
    selected_parts = data["parts"]
    date = data['date']
    shift = data['shift']
    
    att_path = get_file_path(FILES['attendance'])
    att_df = pd.read_csv(att_path)
    present_ids = att_df[(att_df['date'] == date) & (att_df['shift'] == shift) & (att_df['present'] == True)]['emp_id'].tolist()

    present_employees = [
        {"id": eid, "name": emp["name"], "efficiency": emp["efficiency"], "trained_skills": emp["trained_skills"]}
        for eid, emp in employees.items() if eid in present_ids
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

    for task in total_tasks:
        task_assignments = []
        best_operator = None
        support_operator = None
        last_best_efficiency = -float('inf')
        last_support_efficiency = float('inf')
        for emp_assign in assignment_employees[:]:
            skills = [s.strip() for s in emp_assign["trained_skills"].split(",")]
            if task['work_area'] in skills and emp_assign["efficiency"] > last_best_efficiency:
                best_operator = emp_assign
                last_best_efficiency = emp_assign["efficiency"]

        if best_operator:
            assignment_employees.remove(best_operator)

        for emp_assign in assignment_employees[:]:
            skills = [s.strip() for s in emp_assign["trained_skills"].split(",")]
            if task['work_area'] in skills and emp_assign["efficiency"] < last_support_efficiency:
                support_operator = emp_assign
                last_support_efficiency = emp_assign["efficiency"]

        if support_operator:
            assignment_employees.remove(support_operator)

        if best_operator:
            task_assignments.append({
                "best_operator": best_operator["name"],
                "support_operator": support_operator["name"] if support_operator else "None"
            })

        assignments.append({
            "part": task["part_name"],
            "quantity": task["quantity"],
            "work_area": task["work_area"],
            "operators": task_assignments
        })

        log_entries.append({
            'date': date,
            'shift': shift,
            'part_id': part_id,
            'work_area': area,
            'plan_qty': qty,
            'actual_qty': 0,
            'efficiency': 0
        })

    prod_path = get_file_path(FILES['production'])
    log_df = pd.DataFrame(log_entries)
    log_df.to_csv(prod_path, mode='a', header=not os.path.exists(prod_path), index=False)

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

    prod_path = get_file_path(FILES['production'])
    df = pd.read_csv(prod_path)
    
    mask = (df['date'] == date) & (df['shift'] == shift) & (df['part_id'] == part_id) & (df['work_area'] == area)
    
    if mask.any():
        df.loc[mask, 'actual_qty'] = actual
        df.loc[mask, 'efficiency'] = efficiency
        df.to_csv(prod_path, index=False)
        return jsonify({"status": "updated", "efficiency": efficiency})
    
    return jsonify({"status": "not found"})

# --- MATERIAL ---
@app.route("/save_material", methods=["POST"])
def save_material():
    data = request.get_json()
    date_str = data.get("date")
    materials = data.get("materials", [])
    
    rows = []
    for item in materials:
        rows.append({
            'date': date_str,
            'program': item['program'],
            'part_id': item['part_id'],
            'work_area': item['work_area'],
            'qty': item['qty'],
            'req': item['req'],
            'actual': item['actual'],
            'efficiency': float(item['efficiency'].replace('%',''))
        })
    
    mat_path = get_file_path(FILES['material'])
    df = pd.DataFrame(rows)
    df.to_csv(mat_path, mode='a', header=not os.path.exists(mat_path), index=False)
            
    return jsonify({"status": "success", "count": len(rows)})    

# --- DASHBOARD DATA AGGREGATION ---
@app.route("/get_dashboard_data")
def get_dashboard_data():
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    shift = request.args.get('shift')
    
    response_data = {}
    work_areas = ['Autoclave', 'CCA', 'PAA', 'Paint_Booth', 'Prefit']

    prod_path = get_file_path(FILES['production'])
    mat_path = get_file_path(FILES['material'])
    
    prod_df = pd.read_csv(prod_path) if os.path.exists(prod_path) else pd.DataFrame()
    mat_df = pd.read_csv(mat_path) if os.path.exists(mat_path) else pd.DataFrame()

    if not prod_df.empty:
        prod_df = prod_df[prod_df['date'] == date]
        if shift:
            prod_df = prod_df[prod_df['shift'] == shift]
    if not mat_df.empty:
        mat_df = mat_df[mat_df['date'] == date]

    for area in work_areas:
        area_clean = area.replace('_', ' ').lower()
        
        prod_effs = []
        mat_effs = []

        if not prod_df.empty:
            p_rows = prod_df[prod_df['work_area'].str.lower().str.replace('_', ' ') == area_clean]
            prod_effs = p_rows['efficiency'].tolist()

        if not mat_df.empty:
            m_rows = mat_df[mat_df['work_area'].str.lower().str.replace('_', ' ') == area_clean]
            mat_effs = m_rows['efficiency'].tolist()

        all_effs = prod_effs + mat_effs
        avg_eff = sum(all_effs) / len(all_effs) if all_effs else 0
        
        response_data[area] = avg_eff

    # Attendance (for the selected date/shift)
    att_path = get_file_path(FILES['attendance'])
    total_employees = len(employees)
    present_count = 0
    if os.path.exists(att_path):
        att_df = pd.read_csv(att_path)
        if not att_df.empty:
            mask = (att_df['date'] == date)
            if shift:
                mask = mask & (att_df['shift'] == shift)
            present_mask = att_df['present'].astype(str).str.lower().eq('true')
            present_count = att_df[mask & present_mask]['emp_id'].nunique()

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
