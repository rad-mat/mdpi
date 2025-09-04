#!/usr/bin/env python3
"""
Complete cleanup script to reset the MDPI pipeline data and start from scratch.

This script will:
1. Drop all PostgreSQL tables and schemas
2. Clear MinIO S3 buckets
3. Remove local data files (processed and raw)
4. Clear log files
5. Reset dbt state files

Usage:
    python cleanup_all.py [--confirm]
    
Options:
    --confirm    Skip confirmation prompt and run cleanup immediately
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

import psycopg
from minio import Minio
from minio.error import S3Error

from src.utils.shared_config import get_default_config


def print_banner():
    """Print cleanup script banner"""
    print("=" * 60)
    print("🧹 MDPI Data Pipeline Complete Cleanup Script")
    print("=" * 60)
    print("This will permanently delete ALL data including:")
    print("• PostgreSQL tables and schemas")
    print("• MinIO S3 buckets and objects")
    print("• Local data files (raw + processed)")
    print("• Log files")
    print("• dbt state and artifacts")
    print("=" * 60)


def confirm_cleanup():
    """Ask user to confirm the cleanup operation"""
    response = input("⚠️  Are you sure you want to proceed? (type 'YES' to confirm): ")
    if response != "YES":
        print("❌ Cleanup cancelled.")
        sys.exit(0)
    print("✅ Cleanup confirmed. Starting...")


def cleanup_postgresql():
    """Clean up PostgreSQL database tables and schemas"""
    print("\n📊 Cleaning up PostgreSQL database...")
    
    config = get_default_config()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg.connect(
            host=config.db_host,
            port=config.db_port,
            dbname=config.db_name,
            user=config.db_user,
            password=config.db_password,
        )
        
        with conn.cursor() as cur:
            # Drop all tables in public schema
            print("  • Dropping tables in public schema...")
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            tables = cur.fetchall()
            
            for (table_name,) in tables:
                print(f"    - Dropping table: {table_name}")
                cur.execute(f'DROP TABLE IF EXISTS public."{table_name}" CASCADE')
            
            # Drop dbt schemas if they exist
            dbt_schemas = ['public_staging', 'public_marts']
            for schema in dbt_schemas:
                print(f"  • Dropping schema: {schema}")
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            
            # Drop any other non-system schemas
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'public')
            """)
            custom_schemas = cur.fetchall()
            
            for (schema_name,) in custom_schemas:
                print(f"  • Dropping custom schema: {schema_name}")
                cur.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
            
            conn.commit()
            print("  ✅ PostgreSQL cleanup completed")
            
    except psycopg.Error as e:
        print(f"  ❌ PostgreSQL cleanup failed: {e}")
    except Exception as e:
        print(f"  ❌ Unexpected error during PostgreSQL cleanup: {e}")
    finally:
        if 'conn' in locals():
            conn.close()


def cleanup_minio():
    """Clean up MinIO S3 buckets and objects"""
    print("\n🪣 Cleaning up MinIO S3 storage...")
    
    config = get_default_config()
    
    try:
        # Connect to MinIO
        client = Minio(
            f"{config.s3_host}:{config.s3_port}",
            access_key=config.s3_access_key,
            secret_key=config.s3_secret_key,
            secure=config.s3_secure,
        )
        
        # List all buckets
        print("  • Finding buckets...")
        buckets = client.list_buckets()
        
        for bucket in buckets:
            print(f"  • Cleaning bucket: {bucket.name}")
            
            # Remove all objects in the bucket
            try:
                objects = client.list_objects(bucket.name, recursive=True)
                object_names = [obj.object_name for obj in objects]
                
                if object_names:
                    print(f"    - Removing {len(object_names)} objects...")
                    for obj_name in object_names:
                        client.remove_object(bucket.name, obj_name)
                        print(f"      ◦ Removed: {obj_name}")
                else:
                    print("    - No objects to remove")
                
                # Remove the bucket itself
                print(f"    - Removing bucket: {bucket.name}")
                client.remove_bucket(bucket.name)
                
            except S3Error as e:
                print(f"    ❌ Error cleaning bucket {bucket.name}: {e}")
        
        print("  ✅ MinIO cleanup completed")
        
    except Exception as e:
        print(f"  ❌ MinIO cleanup failed: {e}")


def cleanup_local_files():
    """Clean up local data files and logs"""
    print("\n📁 Cleaning up local files...")
    
    # Directories and files to clean
    cleanup_paths = [
        "data/raw",
        "data/processed", 
        "logs",
        "dbt/target",
        "dbt/dbt_packages",
        "dbt/logs",
    ]
    
    # Files to clean but keep directories
    file_patterns = [
        "*.json",
        "*.log",
        "*.sql",
    ]
    
    for path in cleanup_paths:
        if os.path.exists(path):
            print(f"  • Cleaning directory: {path}")
            
            if os.path.isdir(path):
                # Remove all contents but keep directory structure
                for item in os.listdir(path):
                    item_path = os.path.join(path, item)
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                        print(f"    - Removed file: {item}")
                    elif os.path.isdir(item_path) and item not in ['.gitkeep', '__pycache__']:
                        shutil.rmtree(item_path)
                        print(f"    - Removed directory: {item}")
            else:
                os.remove(path)
                print(f"    - Removed file: {path}")
        else:
            print(f"  • Directory not found: {path}")
    
    # Clean up any remaining JSON files in project root
    print("  • Cleaning project root...")
    for json_file in Path(".").glob("*.json"):
        if json_file.name not in ["package.json", "tsconfig.json"]:  # Preserve common config files
            json_file.unlink()
            print(f"    - Removed: {json_file}")
    
    # Clean up Python cache
    print("  • Cleaning Python cache...")
    for pycache in Path(".").rglob("__pycache__"):
        if pycache.is_dir():
            shutil.rmtree(pycache)
            print(f"    - Removed: {pycache}")
    
    print("  ✅ Local files cleanup completed")


def create_empty_directories():
    """Recreate necessary empty directories with .gitkeep files"""
    print("\n📂 Recreating directory structure...")
    
    directories = [
        "data/raw",
        "data/processed",
        "logs",
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        gitkeep_path = os.path.join(directory, ".gitkeep")
        with open(gitkeep_path, "w") as f:
            f.write("")  # Empty .gitkeep file
        print(f"  • Created: {directory}/.gitkeep")
    
    print("  ✅ Directory structure recreated")


def main():
    """Main cleanup function"""
    parser = argparse.ArgumentParser(description="Complete MDPI pipeline cleanup")
    parser.add_argument(
        '--confirm', 
        action='store_true', 
        help='Skip confirmation prompt and run cleanup immediately'
    )
    args = parser.parse_args()
    
    print_banner()
    
    if not args.confirm:
        confirm_cleanup()
    
    print("\n🚀 Starting complete cleanup process...")
    
    # Run all cleanup operations
    cleanup_postgresql()
    cleanup_minio()
    cleanup_local_files()
    create_empty_directories()
    
    print("\n" + "=" * 60)
    print("🎉 Complete cleanup finished!")
    print("=" * 60)
    print("You can now start fresh by running:")
    print("  python main_orchestrated.py")
    print("  # or")
    print("  python main.py")
    print("=" * 60)


if __name__ == "__main__":
    main()