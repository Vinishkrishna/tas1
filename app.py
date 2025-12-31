from flask import Flask, render_template, request, jsonify
from data import employees, parts
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__)

# --- Configuration ---
FILES = {
    'attendance': 'attendance_log.csv',
    'production': 'production_log.csv',
    'material': 'material_log.csv',
    'standard_times': 'wp_data.csv'
}
# --- Helper: Init CSVs ---
def init_csvs():
    if not os.path.exists(FILES['attendance']):
        pd.DataFrame(columns=['date', 'shift', 'emp_id', 'present']).to_csv(FILES['attendance'], index=False)
    if not os.path.exists(FILES['production']):
        # plan_qty is Target, actual_qty is what they achieved
        pd.DataFrame(columns=['date', 'shift', 'part_id', 'work_area', 'plan_qty', 'actual_qty', 'efficiency']).to_csv(FILES['production'], index=False)
    if not os.path.exists(FILES['material']):
        pd.DataFrame(columns=['date', 'program', 'part_id', 'work_area', 'qty', 'req', 'actual', 'efficiency']).to_csv(FILES['material'], index=False)

init_csvs()

# --- Helper: Load Standard Times ---
# Assuming wp_data.csv has columns: [Part_ID, prefit, CCA, PAA, Paint_Booth, Autoclave]
try:
    wp_data = pd.read_csv(FILES['standard_times'])
    header = wp_data.columns.tolist() 
except:
    wp_data = pd.DataFrame()
    header = []

@app.route("/")
def index():
    return render_template("index.html", employees=employees, parts=parts, header=header[1:])

# --- ATTENDANCE ---

def safe_read_csv(filepath, columns):
    """Safely read a CSV file, returning an empty DataFrame with specified columns if file is empty or missing."""
    try:
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            return pd.read_csv(filepath)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        pass
    return pd.DataFrame(columns=columns)

@app.route("/mark_attendance", methods=["POST"])
def mark_attendance():
    MAX_EMPLOYEES = 23  # Maximum cap for employees
    
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
    
    # Remove existing entries for this date/shift before adding new ones
    existing_df = safe_read_csv(FILES['attendance'], ['date', 'shift', 'emp_id', 'present'])
    if not existing_df.empty:
        existing_df = existing_df[~((existing_df['date'] == date) & (existing_df['shift'] == shift))]
    combined_df = pd.concat([existing_df, df], ignore_index=True)
    combined_df.to_csv(FILES['attendance'], index=False)

    # Cap the count at MAX_EMPLOYEES (23) - only count current session checkboxes
    capped_count = min(present_count, MAX_EMPLOYEES)
    attendance_pct = (capped_count / MAX_EMPLOYEES * 100) if MAX_EMPLOYEES > 0 else 0

    return jsonify({
        "status": "success",
        "count": int(capped_count),
        "total": int(MAX_EMPLOYEES),
        "attendance_pct": round(float(attendance_pct), 1),
        "date": date,
        "shift": shift
    })

@app.route("/get_attendance", methods=["GET"])
def get_attendance():
    date = request.args.get('date')
    shift = request.args.get('shift')

    df = safe_read_csv(FILES['attendance'], ['date', 'shift', 'emp_id', 'present'])
    if df.empty:
        return jsonify({})
    
    # Filter for specific date/shift
    mask = (df['date'] == date) & (df['shift'] == shift)
    filtered = df[mask]
    
    # Convert to dict {emp_id: true/false}
    attendance_dict = dict(zip(filtered.emp_id, filtered.present))
    return jsonify(attendance_dict)

# --- PRODUCTION PLAN & SAVING ---
@app.route("/plan_production", methods=["POST"])
def plan_production():
    # assignments = []
    # log_entries = []
    
    #     ops = []
    #     if present_employees:
    #         ops.append({"best_operator": present_employees[0]['name'], "support_operator": "None"})

    #     assignments.append({
    #         "part": part_name,
    #         "part_id": part_id,
    #         "quantity": qty,
    #         "work_area": area,
    #         "operators": ops
    #     })

    #     # Prepare data to save to DB
    #     log_entries.append({
    #         'date': date,
    #         'shift': shift,
    #         'part_id': part_id,
    #         'work_area': area,
    #         'plan_qty': qty,
    #         'actual_qty': 0, # Starts at 0
    #         'efficiency': 0
    #     })

    # # 2. Save Plan to CSV
    # log_df = pd.DataFrame(log_entries)
    # log_df.to_csv(FILES['production'], mode='a', header=not os.path.exists(FILES['production']), index=False)

    data = request.get_json()
    selected_parts = data["parts"]
    date = data['date']
    shift = data['shift']
    att_df = pd.read_csv(FILES['attendance'])
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
        for emp_assign in assignment_employees[:]:  # copy to avoid modification during iteration
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
                "support_operator": support_operator["name"] 
            })

        assignments.append({
            "part": task["part_name"],
            "quantity": task["quantity"],
            "work_area": task["work_area"],
            "operators": task_assignments
        })


        # Prepare data to save to DB
        log_entries.append({
            'date': date,
            'shift': shift,
            'part_id': part_id,
            'work_area': area,
            'plan_qty': qty,
            'actual_qty': 0, # Starts at 0
            'efficiency': 0
        })

    # 2. Save Plan to CSV
    log_df = pd.DataFrame(log_entries)
    log_df.to_csv(FILES['production'], mode='a', header=not os.path.exists(FILES['production']), index=False)

    return jsonify({"assignments": assignments, "present_count": len(present_employees)})




@app.route("/update_production_actual", methods=["POST"])
def update_production_actual():
    # Called when user types in "Actual Quantity" box
    data = request.get_json()
    date = data['date']
    shift = data['shift']
    part_id = data['part_id']
    area = data['work_area']
    actual = float(data['actual'])
    plan = float(data['plan'])
    
    efficiency = (actual / plan * 100) if plan > 0 else 0

    # Update CSV using Pandas
    df = pd.read_csv(FILES['production'])
    
    # Find the row and update
    mask = (df['date'] == date) & (df['shift'] == shift) & (df['part_id'] == part_id) & (df['work_area'] == area)
    
    if mask.any():
        df.loc[mask, 'actual_qty'] = actual
        df.loc[mask, 'efficiency'] = efficiency
        df.to_csv(FILES['production'], index=False)
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
    
    df = pd.DataFrame(rows)
    df.to_csv(FILES['material'], mode='a', header=not os.path.exists(FILES['material']), index=False)
            
    return jsonify({"status": "success", "count": len(rows)})    

# --- DASHBOARD DATA AGGREGATION ---
@app.route("/get_dashboard_data")
def get_dashboard_data():
    # Default to today if no date provided, or filter by specific date
    date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
    shift = request.args.get('shift')
    
    response_data = {}
    work_areas = ['Autoclave', 'CCA', 'PAA', 'Paint_Booth', 'Prefit']

    # 1. Load Data (using safe_read_csv to handle empty files)
    prod_df = safe_read_csv(FILES['production'], ['date', 'shift', 'part_id', 'work_area', 'plan_qty', 'actual_qty', 'efficiency'])
    mat_df = safe_read_csv(FILES['material'], ['date', 'program', 'part_id', 'work_area', 'qty', 'req', 'actual', 'efficiency'])

    # 2. Filter by Date/Shift
    if not prod_df.empty:
        prod_df = prod_df[prod_df['date'] == date]
        if shift:
            prod_df = prod_df[prod_df['shift'] == shift]
    if not mat_df.empty:
        mat_df = mat_df[mat_df['date'] == date]

    # 3. Calculate Stats per Area
    for area in work_areas:
        # Handle naming mismatches (CSV might save 'Paint Booth' vs 'Paint_Booth')
        area_clean = area.replace('_', ' ').lower()
        
        prod_effs = []
        mat_effs = []

        # Get Production Efficiency for this area
        if not prod_df.empty:
            # Case insensitive match
            p_rows = prod_df[prod_df['work_area'].str.lower().str.replace('_', ' ') == area_clean]
            prod_effs = p_rows['efficiency'].tolist()

        # Get Material Efficiency for this area
        if not mat_df.empty:
            m_rows = mat_df[mat_df['work_area'].str.lower().str.replace('_', ' ') == area_clean]
            mat_effs = m_rows['efficiency'].tolist()

        # Combine
        all_effs = prod_effs + mat_effs
        avg_eff = sum(all_effs) / len(all_effs) if all_effs else 0
        
        response_data[area] = avg_eff


    # Attendance (for the selected date/shift)
    MAX_EMPLOYEES = 23
    present_count = 0
    att_df = safe_read_csv(FILES['attendance'], ['date', 'shift', 'emp_id', 'present'])
    if not att_df.empty:
        mask = (att_df['date'] == date)
        if shift:
            mask = mask & (att_df['shift'] == shift)
        present_mask = att_df['present'].astype(str).str.lower().eq('true')
        present_count = min(att_df[mask & present_mask]['emp_id'].nunique(), MAX_EMPLOYEES)

    attendance_pct = (present_count / MAX_EMPLOYEES * 100) if MAX_EMPLOYEES > 0 else 0
    response_data['attendance_present'] = int(present_count)
    response_data['attendance_total'] = int(MAX_EMPLOYEES)
    response_data['attendance_pct'] = round(float(attendance_pct), 1)

    return jsonify(response_data)

if __name__ == "__main__":
    app.run(debug=True)