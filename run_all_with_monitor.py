#!/usr/bin/env python3
"""Supervisor that launches scrapers in separate processes and monitors them.
It uses the `match_runner` module to run each scraper in its own process and logs PIDs.
"""
import logging
import multiprocessing
import os
import sys
import time
from functools import partial
from typing import Optional

LOG_FORMAT = '%(asctime)s - Supervisor - PID:%(process)d - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger('run_all_with_monitor')
# ensure supervisor log file and prevent log propagation to console
try:
    # remove root handlers to avoid duplicate console output
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    logdir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(logdir, exist_ok=True)
    fh = logging.FileHandler(os.path.join(logdir, 'supervisor.log'), encoding='utf-8')
    fh.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(fh)
    logger.propagate = False
except Exception:
    # if filesystem permissions prevent logging to file, keep console logging
    logger.exception('Failed to create supervisor log file')

try:
    import match_runner as mr
except Exception:
    sys.path.insert(0, os.path.dirname(__file__))
    import match_runner as mr

try:
    import psutil
    PSUTIL_AVAILABLE = True
except Exception:
    PSUTIL_AVAILABLE = False

import shutil
from collections import deque


def _bytes_to_human(n: int) -> str:
    # simple human friendly bytes
    for unit in ['B','K','M','G','T','P']:
        if abs(n) < 1024.0:
            return f"{n:3.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}P"


def _format_time(seconds: float) -> str:
    # seconds to H:MM:SS
    try:
        sec = int(seconds)
        h = sec // 3600
        m = (sec % 3600) // 60
        s = sec % 60
        if h:
            return f"{h}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"
    except Exception:
        return "0:00"


def _collect_proc_detailed(pid: int) -> dict:
    """Return detailed stats for PID using psutil. If psutil missing, return minimal."""
    try:
        p = psutil.Process(pid)
        with p.oneshot():
            name = p.name()
            cpu = p.cpu_percent(interval=0.0)
            times = p.cpu_times()
            total_time = (getattr(times, 'user', 0.0) or 0.0) + (getattr(times, 'system', 0.0) or 0.0)
            threads = p.num_threads()
            mem = p.memory_info().rss
            status = p.status()
        return {
            'pid': pid,
            'name': name,
            'cpu': cpu,
            'time': _format_time(total_time),
            'threads': threads,
            'mem': mem,
            'status': status
        }
    except Exception:
        return {'pid': pid, 'name': str(pid), 'cpu': 0.0, 'time': '0:00', 'threads': 0, 'mem': 0, 'status': 'stopped'}


def _display_top(process_rows: list):
    """Clear screen and print a top-like summary + process table."""
    # Clear screen
    print("\033[2J\033[H", end='')

    # Header: system overview
    try:
        load1, load5, load15 = os.getloadavg()
    except Exception:
        load1 = load5 = load15 = 0.0

    if PSUTIL_AVAILABLE:
        cpu_times = psutil.cpu_times_percent()
        vm = psutil.virtual_memory()
        disk = psutil.disk_io_counters()
        net = psutil.net_io_counters()
        procs = psutil.pids()
        total_threads = 0
        total = len(procs)
        running = 0
        for pid in procs:
            if not pid or not psutil.pid_exists(pid):
                continue
            try:
                p = psutil.Process(pid)
                total_threads += p.num_threads()
                if p.status() == psutil.STATUS_RUNNING:
                    running += 1
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue
        sleeping = total - running

        now = time.strftime('%H:%M:%S')
        print(f"Processes: {total} total, {running} running, {sleeping} sleeping, {total_threads} threads  {now}")
        print(f"Load Avg: {load1:.2f}, {load5:.2f}, {load15:.2f}")
        print(f"CPU usage: {cpu_times.user:.2f}% user, {cpu_times.system:.2f}% sys, {cpu_times.idle:.2f}% idle")
        print(f"PhysMem: {vm.total//1024//1024}M total, {vm.used//1024//1024}M used, {vm.available//1024//1024}M free")
        print(f"Disk IO: read={_bytes_to_human(disk.read_bytes) if disk else '0B'}, write={_bytes_to_human(disk.write_bytes) if disk else '0B'}")
        print(f"Net: in={_bytes_to_human(net.bytes_recv)}, out={_bytes_to_human(net.bytes_sent)}")
    else:
        # fallback minimal header
        now = time.strftime('%H:%M:%S')
        print(f"Processes: {len(process_rows)} tracked  {now}")
        print(f"Load Avg: {load1:.2f}, {load5:.2f}, {load15:.2f}")

    # Table header
    print("\nPID   COMMAND              %CPU  TIME     #TH  MEM-MB   STATUS")
    print("----  -------------------- ----- ------- ---- -------- -------")

    for r in process_rows:
        pid = r.get('pid')
        name = (r.get('name') or '')[:20].ljust(20)
        cpu = f"{r.get('cpu', 0.0):5.1f}"
        time_s = str(r.get('time', '0:00')).rjust(7)
        th = str(r.get('threads', 0)).rjust(4)
        mem_mb = f"{(r.get('mem', 0) / 1024 / 1024):8.1f}"
        status = r.get('status', '')
        print(f"{pid:<5} {name} {cpu} {time_s} {th} {mem_mb} {status}")




def _target_wrapper(func_name, arg=None, matches_path=None):
    """Wrapper to call functions from match_runner by name.
    If func_name == 'run_match', `arg` should be the match_id string and
    `matches_path` is forwarded as the optional matches.json path.
    Otherwise `arg` is passed as the single argument to the runner function.
    """
    logger.info('Starting target %s in PID %s', func_name, multiprocessing.current_process().pid)
    try:
        if func_name == 'run_match':
            mr.run_match(arg, matches_path)
        else:
            fn = getattr(mr, func_name)
            if arg:
                fn(arg)
            else:
                fn()
    except Exception:
        logger.exception('Target %s crashed', func_name)


def _collect_proc_stats(pid: int) -> dict:
    if PSUTIL_AVAILABLE:
        try:
            p = psutil.Process(pid)
            with p.oneshot():
                cpu = p.cpu_percent(interval=0.1)
                mem = p.memory_info().rss / 1024 / 1024
                status = p.status()
            return {'pid': pid, 'cpu': cpu, 'mem_mb': mem, 'status': status}
        except Exception:
            return {'pid': pid, 'cpu': 0.0, 'mem_mb': 0.0, 'status': 'stopped'}
    else:
        # fallback using ps command
        try:
            import subprocess
            out = subprocess.check_output(['ps', '-p', str(pid), '-o', '%cpu=,rss=,stat='])
            s = out.decode().strip()
            if not s:
                return {'pid': pid, 'cpu': 0.0, 'mem_mb': 0.0, 'status': 'stopped'}
            parts = s.split()
            cpu = float(parts[0])
            rss_kb = float(parts[1])
            mem_mb = rss_kb / 1024.0
            status = parts[2]
            return {'pid': pid, 'cpu': cpu, 'mem_mb': mem_mb, 'status': status}
        except Exception:
            return {'pid': pid, 'cpu': 0.0, 'mem_mb': 0.0, 'status': 'stopped'}


def start_processes(scrapers, matches_cfg_path=None, path=None, restart_failed=False, max_restarts=2, max_concurrent: int = 4, max_total: Optional[int] = None, max_batches_per_match: Optional[int] = None, posts_per_batch: Optional[int] = None):
    processes = {}
    restarts = {name: 0 for name in scrapers}
    pending = deque(scrapers)
    started_count = 0

    # scrapers is list of match_ids (strings). Start up to `max_concurrent` at once.
    def _start_next():
        nonlocal started_count
        if not pending:
            return None
        if max_total is not None and started_count >= max_total:
            return None
        match_id = pending.popleft()
        # propagate batch limit and posts-per-batch to child via env vars
        if max_batches_per_match is not None:
            os.environ['MAX_BATCHES_PER_MATCH'] = str(max_batches_per_match)
        else:
            os.environ.pop('MAX_BATCHES_PER_MATCH', None)
        if posts_per_batch is not None:
            os.environ['POSTS_PER_BATCH'] = str(posts_per_batch)
        else:
            os.environ.pop('POSTS_PER_BATCH', None)
        # suppress child console logging so supervisor CLI only shows the top view
        os.environ['SUPPRESS_CHILD_CONSOLE'] = '1'
        p = multiprocessing.Process(target=_target_wrapper, args=('run_match', match_id, path), name=f'match-{match_id}')
        p.start()
        processes[match_id] = p
        started_count += 1
        logger.info('Launched match %s PID=%s (started %d%s)', match_id, p.pid, started_count, (f"/{max_total}" if max_total else ''))
        return p

    # Start initial batch
    while len(processes) < max_concurrent and pending and (max_total is None or started_count < (max_total or 0) or max_total is None):
        _start_next()

    try:
        while True:
            alive_any = False
            process_rows = []
            # iterate snapshot of names to allow mutation
            for name, p in list(processes.items()):
                pid = p.pid
                if p.is_alive():
                    alive_any = True
                    if PSUTIL_AVAILABLE:
                        proc_stats = _collect_proc_detailed(pid)
                        proc_stats['match_name'] = name
                        process_rows.append(proc_stats)
                    else:
                        stats = _collect_proc_stats(pid)
                        process_rows.append({'pid': pid, 'name': name, 'cpu': stats['cpu'], 'time': '0:00', 'threads': 0, 'mem': stats['mem_mb'], 'status': stats['status']})
                else:
                    exitcode = p.exitcode
                    # handle restarted logic (don't display exited processes)
                    if exitcode != 0 and restart_failed and restarts.get(name, 0) < max_restarts:
                        restarts[name] += 1
                        logger.info('Restarting %s (attempt %d)', name, restarts[name])
                        np = multiprocessing.Process(target=_target_wrapper, args=('run_match', name, path), name=f'match-{name}')
                        np.start()
                        processes[name] = np
                        logger.info('Restarted %s PID=%s', name, np.pid)
                        alive_any = True
                    else:
                        # remove finished
                        processes.pop(name, None)
                        # try to start another if pending
                        if pending and (max_total is None or started_count < max_total):
                            _start_next()

            # Display using top-like view if psutil available
            if PSUTIL_AVAILABLE:
                # sort by cpu desc
                process_rows_sorted = sorted(process_rows, key=lambda x: x.get('cpu', 0.0), reverse=True)
                _display_top(process_rows_sorted)
            else:
                logger.info('--- Processes status ---')
                for r in process_rows:
                    logger.info('Match:%s PID:%s CPU:%.1f%% MEM:%.1fMB STATUS:%s', r.get('name'), r.get('pid'), r.get('cpu', 0.0), (r.get('mem', 0) / 1024 / 1024), r.get('status'))

            # If no live processes and nothing pending, we're done
            if not alive_any and not pending:
                logger.info('All match processes completed')
                break
            time.sleep(3)
    except KeyboardInterrupt:
        logger.info('KeyboardInterrupt received, terminating children...')
        for p in processes.values():
            try:
                p.terminate()
            except Exception:
                pass


def main():
    import argparse
    import json

    parser = argparse.ArgumentParser(description='Run multiple match scrapers in parallel and monitor them')
    parser.add_argument('--scrapers', help='Legacy: comma separated list (ignored when specifying matches)', default='urls,news,transfermarkt,reddit')
    parser.add_argument('--path', help='Optional path passed to each scraper')
    parser.add_argument('--restart-failed', action='store_true', help='Restart failed processes up to max restarts')
    parser.add_argument('--max-restarts', type=int, default=2, help='Maximum restarts per process')
    parser.add_argument('--all', action='store_true', help='Run all matches defined in config/matches.json')
    parser.add_argument('--matches', help='Comma separated list of match_ids to run', default=None)
    parser.add_argument('--max-concurrent', type=int, default=4, help='Maximum number of concurrent match processes')
    parser.add_argument('--max-total', type=int, default=None, help='Maximum total match processes to start (optional)')
    parser.add_argument('--max-batches-per-match', type=int, default=None, help='Maximum number of batches to save per match (optional)')
    parser.add_argument('--posts-per-batch', type=int, default=None, help='Number of posts per saved batch (optional)')

    args, unknown = parser.parse_known_args()

    match_ids = []

    if args.all:
        # look for matches.json in config/ or conf/ and support list or dict formats
        base_dir = os.path.dirname(__file__)
        candidate_config = os.path.join(base_dir, 'config', 'matches.json')
        candidate_conf = os.path.join(base_dir, 'conf', 'matches.json')
        chosen_cfg = args.path or (candidate_config if os.path.exists(candidate_config) else (candidate_conf if os.path.exists(candidate_conf) else candidate_conf))
        try:
            with open(chosen_cfg, 'r') as fh:
                cfg = json.load(fh)
            if isinstance(cfg, dict):
                matches_list = cfg.get('matches', [])
            elif isinstance(cfg, list):
                matches_list = cfg
            else:
                logger.error('Unexpected matches.json format: %s', chosen_cfg)
                return
            for m in matches_list:
                if m.get('active', True):
                    match_ids.append(m.get('match_id'))
        except Exception:
            logger.exception('Failed to load matches config from %s', chosen_cfg)
            return
    elif args.matches:
        match_ids = [s.strip() for s in args.matches.split(',') if s.strip()]
    else:
        # Accept flags like --barca_newcastle_2024_03_10
        for u in unknown:
            if u.startswith('--'):
                match_ids.append(u.lstrip('-'))

        # Fallback to legacy scrapers arg if nothing provided
        if not match_ids:
            match_ids = [s.strip() for s in args.scrapers.split(',') if s.strip()]

    if not match_ids:
        logger.error('No matches or scrapers specified. Use --all or --<match_id> or --matches')
        return

    # determine matches.json path (support both config/ and conf/)
    base_dir = os.path.dirname(__file__)
    candidate_config = os.path.join(base_dir, 'config', 'matches.json')
    candidate_conf = os.path.join(base_dir, 'conf', 'matches.json')
    if args.path:
        chosen_path = args.path
    elif os.path.exists(candidate_config):
        chosen_path = candidate_config
    elif os.path.exists(candidate_conf):
        chosen_path = candidate_conf
    else:
        chosen_path = candidate_conf

    logger.info('Supervisor starting for matches: %s', ','.join(match_ids))
    start_processes(
        match_ids,
        path=chosen_path,
        restart_failed=args.restart_failed,
        max_restarts=args.max_restarts,
        max_concurrent=args.max_concurrent,
        max_total=args.max_total,
        max_batches_per_match=args.max_batches_per_match,
        posts_per_batch=args.posts_per_batch,
    )


if __name__ == '__main__':
    main()
