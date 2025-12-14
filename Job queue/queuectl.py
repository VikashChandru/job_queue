#!/usr/bin/env python3
"""
QueueCTL - CLI-based background job queue system
Main entry point for the CLI application
"""

import click
import json
import sys
from datetime import datetime
from pathlib import Path

from queue_manager import QueueManager
from worker import WorkerManager
from config import ConfigManager

# Use the project directory for persistent files so the CLI works
# regardless of the current working directory the user runs it from.
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = str(BASE_DIR / "config.json")
JOBS_PATH = str(BASE_DIR / "jobs.json")

# Initialize managers with explicit file paths
config_manager = ConfigManager(path=CONFIG_PATH)
queue_manager = QueueManager(config_manager, storage_path=JOBS_PATH)
worker_manager = WorkerManager(queue_manager, config_manager)


@click.group()
def cli():
    """QueueCTL - Background Job Queue Management System"""
    pass


@cli.command()
@click.argument('job_data')
def enqueue(job_data):
    """
    Enqueue a new job.
    
    Example: queuectl enqueue '{"id":"job1","command":"sleep 2"}'
    """
    try:
        job_dict = json.loads(job_data)
        
        if 'id' not in job_dict or 'command' not in job_dict:
            click.echo("Error: Job must contain 'id' and 'command' fields", err=True)
            sys.exit(1)
        
        job = queue_manager.enqueue(
            job_id=job_dict['id'],
            command=job_dict['command'],
            max_retries=job_dict.get('max_retries', config_manager.get('max_retries'))
        )
        
        click.echo(f"✓ Job '{job.id}' enqueued successfully")
        click.echo(f"  Command: {job.command}")
        click.echo(f"  Max retries: {job.max_retries}")
        
    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON format", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.group()
def worker():
    """Manage worker processes"""
    pass


@worker.command()
@click.option('--count', default=1, help='Number of workers to start')
def start(count):
    """Start worker processes"""
    try:
        worker_manager.start_workers(count)
        click.echo(f"✓ Started {count} worker(s)")
        click.echo("  Workers are running in background")
        click.echo("  Use 'queuectl worker stop' to stop them")
        click.echo("  Use 'queuectl status' to monitor progress")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@worker.command()
def stop():
    """Stop all worker processes gracefully"""
    try:
        stopped = worker_manager.stop_workers()
        if stopped > 0:
            click.echo(f"✓ Stopped {stopped} worker(s)")
        else:
            click.echo("No workers were running")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
def status():
    """Show queue status and active workers"""
    try:
        stats = queue_manager.get_stats()
        workers = worker_manager.get_active_workers()
        
        click.echo("=" * 50)
        click.echo("QUEUE STATUS")
        click.echo("=" * 50)
        
        click.echo(f"\n📊 Job Statistics:")
        click.echo(f"  Pending:     {stats['pending']}")
        click.echo(f"  Processing:  {stats['processing']}")
        click.echo(f"  Completed:   {stats['completed']}")
        click.echo(f"  Failed:      {stats['failed']}")
        click.echo(f"  Dead (DLQ):  {stats['dead']}")
        click.echo(f"  Total:       {stats['total']}")
        
        click.echo(f"\n👷 Active Workers: {len(workers)}")
        for i, w in enumerate(workers, 1):
            click.echo(f"  Worker {i}: PID {w['pid']} (running for {w['uptime']}s)")
        
        click.echo("=" * 50)
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--state', default=None, help='Filter by state (pending/processing/completed/failed/dead)')
@click.option('--limit', default=20, help='Maximum number of jobs to display')
def list(state, limit):
    """List jobs, optionally filtered by state"""
    try:
        jobs = queue_manager.list_jobs(state=state, limit=limit)
        
        if not jobs:
            click.echo(f"No jobs found{f' with state: {state}' if state else ''}")
            return
        
        click.echo(f"\n{'ID':<20} {'State':<12} {'Command':<30} {'Attempts':<10} {'Updated':<20}")
        click.echo("-" * 100)
        
        for job in jobs:
            cmd = job.command[:27] + "..." if len(job.command) > 30 else job.command
            updated = datetime.fromisoformat(job.updated_at).strftime('%Y-%m-%d %H:%M:%S')
            click.echo(f"{job.id:<20} {job.state:<12} {cmd:<30} {job.attempts}/{job.max_retries:<10} {updated:<20}")
        
        click.echo(f"\nShowing {len(jobs)} job(s)")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.group()
def dlq():
    """Manage Dead Letter Queue"""
    pass


@dlq.command()
@click.option('--limit', default=20, help='Maximum number of jobs to display')
def list(limit):
    """List jobs in the Dead Letter Queue"""
    try:
        jobs = queue_manager.list_dlq_jobs(limit=limit)
        
        if not jobs:
            click.echo("Dead Letter Queue is empty")
            return
        
        click.echo(f"\n{'ID':<20} {'Command':<40} {'Failed At':<20}")
        click.echo("-" * 90)
        
        for job in jobs:
            cmd = job.command[:37] + "..." if len(job.command) > 40 else job.command
            updated = datetime.fromisoformat(job.updated_at).strftime('%Y-%m-%d %H:%M:%S')
            click.echo(f"{job.id:<20} {cmd:<40} {updated:<20}")
        
        click.echo(f"\nShowing {len(jobs)} dead job(s)")
        
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@dlq.command()
@click.argument('job_id')
def retry(job_id):
    """Retry a job from the Dead Letter Queue"""
    try:
        job = queue_manager.retry_dlq_job(job_id)
        click.echo(f"✓ Job '{job_id}' moved back to pending queue")
        click.echo(f"  Attempts reset to 0")
        click.echo(f"  Max retries: {job.max_retries}")
    except ValueError as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@cli.group()
def config():
    """Manage configuration"""
    pass


@config.command()
@click.argument('key')
@click.argument('value')
def set(key, value):
    """Set a configuration value"""
    try:
        valid_keys = ['max_retries', 'backoff_base']
        
        if key not in valid_keys:
            click.echo(f"Error: Invalid key '{key}'. Valid keys: {', '.join(valid_keys)}", err=True)
            sys.exit(1)
        
        # Convert value to appropriate type
        if key in ['max_retries', 'backoff_base']:
            value = int(value)
        
        config_manager.set(key, value)
        click.echo(f"✓ Configuration updated: {key} = {value}")
        
    except ValueError:
        click.echo(f"Error: Invalid value for {key}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@config.command()
@click.argument('key', required=False)
def get(key):
    """Get configuration value(s)"""
    try:
        if key:
            value = config_manager.get(key)
            click.echo(f"{key}: {value}")
        else:
            cfg = config_manager.get_all()
            click.echo("\nCurrent Configuration:")
            click.echo("-" * 30)
            for k, v in cfg.items():
                click.echo(f"  {k}: {v}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()