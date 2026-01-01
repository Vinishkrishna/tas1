"""
Persistent Storage Module for Vercel KV (Upstash Redis)
Falls back to local CSV files when not running on Vercel.
"""
import os
import json
import pandas as pd

# Check if we're running on Vercel with KV configured
def is_vercel_kv_available():
    return os.environ.get('KV_REST_API_URL') and os.environ.get('KV_REST_API_TOKEN')

# Initialize Redis client only if on Vercel with KV configured
redis_client = None
if is_vercel_kv_available():
    try:
        from upstash_redis import Redis
        redis_client = Redis(
            url=os.environ.get('KV_REST_API_URL'),
            token=os.environ.get('KV_REST_API_TOKEN')
        )
    except Exception as e:
        print(f"Failed to initialize Redis client: {e}")
        redis_client = None

# Storage keys
ATTENDANCE_KEY = "attendance_data"
PRODUCTION_KEY = "production_data"
MATERIAL_KEY = "material_data"

# Local file paths (for local development)
LOCAL_FILES = {
    'attendance': 'attendance_log.csv',
    'production': 'production_log.csv',
    'material': 'material_log.csv'
}

def get_base_path():
    """Get base path for local files."""
    return os.path.dirname(os.path.abspath(__file__))

# ============== GENERIC STORAGE FUNCTIONS ==============

def load_data(data_type):
    """
    Load data from storage (KV on Vercel, CSV locally).
    Returns a list of dicts (records).
    """
    if redis_client:
        # Load from Vercel KV
        key = _get_key(data_type)
        try:
            data = redis_client.get(key)
            if data:
                if isinstance(data, str):
                    return json.loads(data)
                return data  # Already parsed
            return []
        except Exception as e:
            print(f"Error loading from KV: {e}")
            return []
    else:
        # Load from local CSV
        filepath = os.path.join(get_base_path(), LOCAL_FILES[data_type])
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            try:
                df = pd.read_csv(filepath)
                return df.to_dict('records')
            except Exception:
                return []
        return []

def save_data(data_type, records):
    """
    Save data to storage (KV on Vercel, CSV locally).
    records: list of dicts
    """
    if redis_client:
        # Save to Vercel KV
        key = _get_key(data_type)
        try:
            redis_client.set(key, json.dumps(records))
            return True
        except Exception as e:
            print(f"Error saving to KV: {e}")
            return False
    else:
        # Save to local CSV
        filepath = os.path.join(get_base_path(), LOCAL_FILES[data_type])
        df = pd.DataFrame(records)
        df.to_csv(filepath, index=False)
        return True

def append_data(data_type, new_records):
    """
    Append new records to existing data.
    """
    existing = load_data(data_type)
    existing.extend(new_records)
    return save_data(data_type, existing)

def _get_key(data_type):
    """Get Redis key for data type."""
    keys = {
        'attendance': ATTENDANCE_KEY,
        'production': PRODUCTION_KEY,
        'material': MATERIAL_KEY
    }
    return keys.get(data_type, data_type)

# ============== ATTENDANCE FUNCTIONS ==============

def get_attendance(date, shift):
    """Get attendance for specific date/shift as dict {emp_id: present}."""
    records = load_data('attendance')
    result = {}
    for r in records:
        if r.get('date') == date and r.get('shift') == shift:
            # Handle both string and boolean values
            present = r.get('present')
            if isinstance(present, str):
                present = present.lower() == 'true'
            result[r.get('emp_id')] = present
    return result

def save_attendance(date, shift, attendance_dict):
    """
    Save attendance for a date/shift.
    attendance_dict: {emp_id: True/False}
    """
    # Load existing data
    records = load_data('attendance')
    
    # Remove existing entries for this date/shift
    records = [r for r in records if not (r.get('date') == date and r.get('shift') == shift)]
    
    # Add new entries
    for emp_id, present in attendance_dict.items():
        records.append({
            'date': date,
            'shift': shift,
            'emp_id': emp_id,
            'present': present
        })
    
    return save_data('attendance', records)

def get_present_employees(date, shift):
    """Get list of present employee IDs for date/shift."""
    attendance = get_attendance(date, shift)
    return [emp_id for emp_id, present in attendance.items() if present]

def count_present(date, shift):
    """Count present employees for date/shift."""
    return len(get_present_employees(date, shift))

# ============== PRODUCTION FUNCTIONS ==============

def get_production(date, shift=None):
    """Get production records for date (and optionally shift)."""
    records = load_data('production')
    result = []
    for r in records:
        if r.get('date') == date:
            if shift is None or r.get('shift') == shift:
                result.append(r)
    return result

def save_production_plan(entries):
    """Append production plan entries."""
    return append_data('production', entries)

def update_production_actual(date, shift, part_id, work_area, actual_qty, efficiency):
    """Update actual quantity and efficiency for a production entry."""
    records = load_data('production')
    updated = False
    
    for r in records:
        if (r.get('date') == date and 
            r.get('shift') == shift and 
            r.get('part_id') == part_id and 
            r.get('work_area') == work_area):
            r['actual_qty'] = actual_qty
            r['efficiency'] = efficiency
            updated = True
            break
    
    if updated:
        save_data('production', records)
    
    return updated

# ============== MATERIAL FUNCTIONS ==============

def get_materials(date):
    """Get material records for date."""
    records = load_data('material')
    return [r for r in records if r.get('date') == date]

def save_materials(entries):
    """Append material entries."""
    return append_data('material', entries)
