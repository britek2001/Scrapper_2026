import argparse
import logging
import os
import sys
from multiprocessing import current_process
import time
from pathlib import Path

try:
    # If requested, suppress child console output early by redirecting
    # stdout/stderr to a per-process startup log file so imports don't print.
    if os.environ.get('SUPPRESS_CHILD_CONSOLE') == '1':
        try:
            pid = os.getpid()
            startup_logdir = os.path.join(os.path.dirname(__file__), 'logs')
            os.makedirs(startup_logdir, exist_ok=True)
            startup_path = os.path.join(startup_logdir, f'match_startup_{pid}.log')
            f = open(startup_path, 'a', encoding='utf-8')
            sys.stdout = f
            sys.stderr = f
        except Exception:
            pass
    import interation_scraper_fixed as isf
except Exception:
    # ensure import path when executed from the same folder
    sys.path.insert(0, os.path.dirname(__file__))
    import interation_scraper_fixed as isf

LOG_FORMAT = '%(asctime)s - PID:%(process)d - %(levelname)s - %(message)s'
DEFAULT_LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')


def configure_logging(logfile: str = None):
    """Configure root logger: stdout + optional file handler. Overwrites existing handlers."""
    root = logging.getLogger()
    # remove existing handlers
    for h in list(root.handlers):
        root.removeHandler(h)

    formatter = logging.Formatter(LOG_FORMAT)
    # add stream handler only when not suppressed by environment
    suppress = os.environ.get('SUPPRESS_CHILD_CONSOLE') == '1'
    if not suppress:
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        root.addHandler(sh)
    root.setLevel(logging.INFO)

    if logfile:
        Path(os.path.dirname(logfile)).mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(logfile, encoding='utf-8')
        fh.setFormatter(formatter)
        root.addHandler(fh)


# configure basic logging to console by default
configure_logging()
logger = logging.getLogger('match_runner')


def run_urls(path=None):
    logger.info('Starting URL extraction')
    driver = isf.setup_driver()
    try:
        urls = isf.Config().load_config(path) if path else isf.Config().load_config(os.path.join(os.path.dirname(__file__), 'config', 'urls.json'))
        scraper = isf.Urls_Extraction()
        results = scraper.execution_url_agentent(driver, urls)
        logger.info('URL extraction finished: %d items', len(results) if results else 0)
        return results
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def run_news(path=None):
    logger.info('Starting News scraping')
    driver = isf.setup_driver()
    try:
        tasks = isf.Config.load_tasks(path or os.path.join(os.path.dirname(__file__), 'config', 'tasks.json'))
        scraper = isf.News_Scraper()
        results = scraper.execution_url_agentent(driver, tasks)
        logger.info('News scraping finished: %d items', len(results) if results else 0)
        return results
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def run_transfermarkt(path=None):
    logger.info('Starting Transfermarkt scraping')
    players = isf.Config.load_player_urls(path or os.path.join(os.path.dirname(__file__), 'config', 'players.json'))
    scraper = isf.TransderMarkt_Scraper()
    results = scraper.execution_url_agentent(None, players)
    logger.info('Transfermarkt scraping finished: %d players', len(results) if results else 0)
    return results


def run_reddit(path=None):
    logger.info('Starting Reddit scraping')
    scr = isf.Redit_Twitter_Scraper(keywords_file=path or os.path.join(os.path.dirname(__file__), 'config', 'comment.json'))
    if scr.keywords:
        results = scr.scrape_by_keywords(scr.keywords, per_keyword_limit=200)
    else:
        results = scr.scrape_soccer_10000_pages()
    logger.info('Reddit scraping finished: %d posts', len(results) if results else 0)
    return results


def run_all():
    pid = current_process().pid
    logger.info('Running all scrapers in process %s', pid)
    start = time.time()
    out = {}
    try:
        out['urls'] = run_urls()
    except Exception as e:
        logger.exception('URLs failed: %s', e)
    try:
        out['news'] = run_news()
    except Exception as e:
        logger.exception('News failed: %s', e)
    try:
        out['transfermarkt'] = run_transfermarkt()
    except Exception as e:
        logger.exception('Transfermarkt failed: %s', e)
    try:
        out['reddit'] = run_reddit()
    except Exception as e:
        logger.exception('Reddit failed: %s', e)
    logger.info('All finished in %.1fs', time.time() - start)
    return out


def run_match(match_id: str, matches_path: str = None) -> dict:
    """Run scrapers for a single match identified by `match_id` from matches.json.
    Returns the match_results dict from MatchOrchestrator.run_match_scraper.
    """
    # create per-process log file so errors are traceable
    pid = os.getpid()
    logdir = os.environ.get('MATCH_LOG_DIR', DEFAULT_LOG_DIR)
    logfile = os.path.join(logdir, f'match_{match_id}_{pid}.log')
    configure_logging(logfile)

    # locate matches.json by default
    if not matches_path:
        matches_path = os.path.join(os.path.dirname(__file__), 'config', 'matches.json')

    # load matches (support both dict {"matches": [...], "global_settings": {...}} and plain list formats)
    import json
    try:
        with open(matches_path, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
    except Exception as e:
        logger.exception('Cannot load matches.json: %s', e)
        return {}

    if isinstance(cfg, dict):
        matches = cfg.get('matches', [])
        global_settings = cfg.get('global_settings', {}) or {}
    elif isinstance(cfg, list):
        matches = cfg
        global_settings = {}
    else:
        logger.error('Unexpected matches.json structure: %s', type(cfg))
        return {}

    target = None
    for m in matches:
        try:
            if m.get('match_id') == match_id:
                target = m
                break
        except Exception:
            continue

    if not target:
        logger.error('Match id %s not found in %s', match_id, matches_path)
        return {}

    # create driver and orchestrator
    driver = None
    try:
        driver = isf.setup_driver()
    except Exception:
        driver = None

    orch = isf.MatchOrchestrator(max_workers=3, output_base_dir=global_settings.get('output_base_dir', 'match_data'))

    try:
        result = orch.run_match_scraper(target, driver)
        return result
    except Exception:
        logger.exception('Error running match %s', match_id)
        return {}
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description='Run individual scrapers (each run shows its PID in logs)')
    parser.add_argument('--scraper', choices=['urls', 'news', 'transfermarkt', 'reddit', 'all'], default='all')
    parser.add_argument('--path', help='Optional path to config JSON for the scraper')
    args = parser.parse_args()

    logger.info('Process started: PID=%s', current_process().pid)
    if args.scraper == 'urls':
        run_urls(args.path)
    elif args.scraper == 'news':
        run_news(args.path)
    elif args.scraper == 'transfermarkt':
        run_transfermarkt(args.path)
    elif args.scraper == 'reddit':
        run_reddit(args.path)
    else:
        run_all()


if __name__ == '__main__':
    main()
