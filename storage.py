"""
Persistent Storage Module for Vercel KV (Upstash Redis)
Falls back to local CSV files when not running on Vercel.
Uses date-based keys for ALL operations - instant read/write.
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

# Local file paths (for local development)
LOCAL_FILES = {
    'attendance': 'attendance_log.csv',
    'production': 'production_log.csv',
    'material': 'material_log.csv'
}

def get_base_path():
    """Get base path for local files."""
    return os.path.dirname(os.path.abspath(__file__))

# ============== ATTENDANCE FUNCTIONS (OPTIMIZED) ==============

def _get_attendance_key(date, shift):
    """Key: att:2026-01-01:Day"""
    return f"att:{date}:{shift}"

def get_attendance(date, shift):
    """Get attendance for specific date/shift as dict {emp_id: present}."""
    if redis_client:
        key = _get_attendance_key(date, shift)
        try:
            data = redis_client.get(key)
            if data:
                return json.loads(data) if isinstance(data, str) else data
            return {}
        except:
            return {}
    else:
        filepath = os.path.join(get_base_path(), LOCAL_FILES['attendance'])
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            try:
                df = pd.read_csv(filepath)
                mask = (df['date'] == date) & (df['shift'] == shift)
                filtered = df[mask]
                result = {}
                for _, row in filtered.iterrows():
                    present = row['present']
                    if isinstance(present, str):
                        present = present.lower() == 'true'
                    result[row['emp_id']] = present
                return result
            except:
                return {}
        return {}

def save_attendance(date, shift, attendance_dict):
    """Save attendance - instant write to specific key."""
    if redis_client:
        key = _get_attendance_key(date, shift)
        try:
            redis_client.set(key, json.dumps(attendance_dict))
            return True
        except:
            return False
    else:
        filepath = os.path.join(get_base_path(), LOCAL_FILES['attendance'])
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            try:
                existing_df = pd.read_csv(filepath)
                existing_df = existing_df[~((existing_df['date'] == date) & (existing_df['shift'] == shift))]
            except:
                existing_df = pd.DataFrame(columns=['date', 'shift', 'emp_id', 'present'])
        else:
            existing_df = pd.DataFrame(columns=['date', 'shift', 'emp_id', 'present'])
        
        new_rows = [{'date': date, 'shift': shift, 'emp_id': emp_id, 'present': present} 
                    for emp_id, present in attendance_dict.items()]
        new_df = pd.DataFrame(new_rows)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df.to_csv(filepath, index=False)
        return True

def get_present_employees(date, shift):
    """Get list of present employee IDs for date/shift."""
    attendance = get_attendance(date, shift)
    return [emp_id for emp_id, present in attendance.items() if present]

def count_present(date, shift):
    """Count present employees for date/shift."""
    return len(get_present_employees(date, shift))

# ============== PRODUCTION FUNCTIONS (OPTIMIZED) ==============

def _get_production_key(date, shift):
    """Key: prod:2026-01-01:Day"""
    return f"prod:{date}:{shift}"

def get_production(date, shift=None):
    """Get production records for date/shift."""
    if redis_client:
        if shift:
            key = _get_production_key(date, shift)
            try:
                data = redis_client.get(key)
                if data:
                    return json.loads(data) if isinstance(data, str) else data
                return []
            except:
                return []
        else:
            # Get both shifts
            results = []
            for s in ['Day', 'Night']:
                key = _get_production_key(date, s)
                try:
                    data = redis_client.get(key)
                    if data:
                        records = json.loads(data) if isinstance(data, str) else data
                        results.extend(records)
                except:
                    pass
            return results
    else:
        filepath = os.path.join(get_base_path(), LOCAL_FILES['production'])
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            try:
                df = pd.read_csv(filepath)
                mask = (df['date'] == date)
                if shift:
                    mask = mask & (df['shift'] == shift)
                return df[mask].to_dict('records')
            except:
                return []
        return []

def save_production_plan(date, shift, entries):
    """Save production plan - instant write to specific key."""
    if redis_client:
        key = _get_production_key(date, shift)
        try:
            # Get existing and append
            existing = []
            data = redis_client.get(key)
            if data:
                existing = json.loads(data) if isinstance(data, str) else data
            existing.extend(entries)
            redis_client.set(key, json.dumps(existing))
            return True
        except:
            return False
    else:
        filepath = os.path.join(get_base_path(), LOCAL_FILES['production'])
        df = pd.DataFrame(entries)
        df.to_csv(filepath, mode='a', header=not os.path.exists(filepath), index=False)
        return True

def update_production_actual(date, shift, part_id, work_area, actual_qty, efficiency):
    """Update actual quantity and efficiency for a production entry."""
    if redis_client:
        key = _get_production_key(date, shift)
        try:
            data = redis_client.get(key)
            if not data:
                return False
            records = json.loads(data) if isinstance(data, str) else data
            updated = False
            for r in records:
                if r.get('part_id') == part_id and r.get('work_area') == work_area:
                    r['actual_qty'] = actual_qty
                    r['efficiency'] = efficiency
                    updated = True
                    break
            if updated:
                redis_client.set(key, json.dumps(records))
            return updated
        except:
            return False
    else:
        filepath = os.path.join(get_base_path(), LOCAL_FILES['production'])
        if not os.path.exists(filepath):
            return False
        df = pd.read_csv(filepath)
        mask = (df['date'] == date) & (df['shift'] == shift) & (df['part_id'] == part_id) & (df['work_area'] == work_area)
        if mask.any():
            df.loc[mask, 'actual_qty'] = actual_qty
            df.loc[mask, 'efficiency'] = efficiency
            df.to_csv(filepath, index=False)
            return True
        return False

# ============== MATERIAL FUNCTIONS (OPTIMIZED) ==============

def _get_material_key(date):
    """Key: mat:2026-01-01"""
    return f"mat:{date}"

def get_materials(date):
    """Get material records for date."""
    if redis_client:
        key = _get_material_key(date)
        try:
            data = redis_client.get(key)
            if data:
                return json.loads(data) if isinstance(data, str) else data
            return []
        except:
            return []
    else:
        filepath = os.path.join(get_base_path(), LOCAL_FILES['material'])
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            try:
                df = pd.read_csv(filepath)
                return df[df['date'] == date].to_dict('records')
            except:
                return []
        return []

def save_materials(date, entries):
    """Save materials - instant write to specific key."""
    if redis_client:
        key = _get_material_key(date)
        try:
            # Get existing and append
            existing = []
            data = redis_client.get(key)
            if data:
                existing = json.loads(data) if isinstance(data, str) else data
            existing.extend(entries)
            redis_client.set(key, json.dumps(existing))
            return True
        except:
            return False
    else:
        filepath = os.path.join(get_base_path(), LOCAL_FILES['material'])
        df = pd.DataFrame(entries)
        df.to_csv(filepath, mode='a', header=not os.path.exists(filepath), index=False)
        return True
