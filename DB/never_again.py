import os
import shutil
from datetime import datetime

def backup_database():
    """Create a backup of the database with timestamp"""
    try:
        # Get the current script's directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Define source and backup directories
        source_db = os.path.join(script_dir, 'QUANT.db')
        backup_dir = os.path.join(script_dir, 'backups')
        
        # Create backup directory if it doesn't exist
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)
            print(f"Created backup directory: {backup_dir}")
        
        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(backup_dir, f'QUANT_backup_{timestamp}.db')
        
        # Copy the database file
        shutil.copy2(source_db, backup_file)
        
        print(f"Database backup created successfully: {backup_file}")
        
        # List all backups
        list_backups(backup_dir)
        
    except Exception as e:
        print(f"Error creating database backup: {e}")
        raise

def list_backups(backup_dir):
    """List all available backups"""
    try:
        backups = [f for f in os.listdir(backup_dir) if f.endswith('.db')]
        if backups:
            print("\nAvailable backups:")
            for backup in sorted(backups, reverse=True):
                print(f"  - {backup}")
        else:
            print("\nNo backups found.")
    except Exception as e:
        print(f"Error listing backups: {e}")

if __name__ == "__main__":
    backup_database() 