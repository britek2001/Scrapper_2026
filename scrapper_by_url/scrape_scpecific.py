#!/usr/bin/env python3
# Selenium Content Fetcher - Handles Bing redirects properly

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

import csv
import json
import time
import os
import re
from datetime import datetime
from urllib.parse import urlparse
import html2text
from collections import defaultdict


# ---------- CONFIGURATION ----------
CONFIG = {
    'timeout': 30,  # Longer timeout for Selenium
    'max_content_length': 15000,
    'delay_between_requests': 2,  # Longer delay to avoid blocking
    'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'output_directory': 'selenium_content',
    'headless': False,  # Set to True to run in background
    'batch_size': 100
}

# Charger config externe si présente (override partiel)
try:
    from config_loader import load_config
    cfg = load_config()
    if cfg:
        # Appliquer uniquement les clés connues
        for k in ('timeout','max_content_length','delay_between_requests','user_agent','output_directory','headless','batch_size'):
            if k in cfg:
                CONFIG[k] = cfg[k]
        # valeurs pratiques pour les prompts
        DEFAULT_INPUT_CSV = cfg.get('input_csv', '')
        DEFAULT_LIMIT = int(cfg.get('limit', 5))
        # NOUVEAU: support des URLs directement dans la config (liste de strings ou d'objets)
        DEFAULT_URLS = cfg.get('urls', None)
    else:
        DEFAULT_INPUT_CSV = ''
        DEFAULT_LIMIT = 5
        DEFAULT_URLS = None
except Exception:
    DEFAULT_INPUT_CSV = ''
    DEFAULT_LIMIT = 5
    DEFAULT_URLS = None


# ---------- CSV READER ----------
def read_urls_from_csv(csv_file, limit=None):
    """Read URLs from CSV file"""
    urls_data = []
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            # Try to detect if it's actually a CSV or if first line is CSV header
            first_line = f.readline().strip()
            f.seek(0)
            
            # Check if it looks like CSV with header
            if first_line.startswith('ID,Query,Category,URL') or ',' in first_line:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if limit and i >= limit:
                        break
                    
                    # Handle different possible column names
                    url = row.get('URL') or row.get('url') or row.get('Bing URL') or ''
                    urls_data.append({
                        'id': row.get('ID') or row.get('id') or str(i+1),
                        'url': url.strip(),
                        'query': row.get('Query') or row.get('query') or '',
                        'category': row.get('Category') or row.get('category') or 'uncategorized'
                    })
            else:
                # Simple line-by-line URLs
                for i, line in enumerate(f):
                    if limit and i >= limit:
                        break
                    url = line.strip()
                    if url and url.startswith('http'):
                        urls_data.append({
                            'id': str(i+1),
                            'url': url,
                            'query': '',
                            'category': 'uncategorized'
                        })
    except Exception as e:
        print(f"Error reading CSV file: {e}")
    
    return urls_data


def read_urls_from_json(json_file, limit=None):
    """Read URLs from a JSON file. Supports a list of strings or dicts, or a dict with 'urls' key."""
    urls_data = []
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            # list of strings or dicts
            urls_data = urls_from_config(data, limit=limit)
        elif isinstance(data, dict):
            # support top-level 'urls' key
            cfg_urls = data.get('urls') or data.get('URLs') or None
            if cfg_urls:
                urls_data = urls_from_config(cfg_urls, limit=limit)
            else:
                # fallback: look for simple 'input_csv' or other fields
                print('JSON file has no "urls" key; expected an array under "urls"')
    except Exception as e:
        print(f"Error reading JSON file: {e}")
    return urls_data


# ---------- SELENIUM SETUP ----------
def setup_driver():
    """Setup Selenium Chrome driver"""
    options = webdriver.ChromeOptions()
    
    if CONFIG['headless']:
        options.add_argument("--headless=new")
    
    # Add arguments to avoid detection
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={CONFIG['user_agent']}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Disable automation flags
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Add preferences
    prefs = {
        "profile.default_content_setting_values.notifications": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False
    }
    options.add_experimental_option("prefs", prefs)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    # Execute CDP commands to avoid detection
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {
        "userAgent": CONFIG['user_agent'],
        "platform": "macOS"
    })
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

# New helper: ensure driver is alive or recreate it
def ensure_driver_alive(driver):
    try:
        # quick check
        _ = driver.current_url
        return driver
    except Exception as e:
        print("  Driver dead, restarting Selenium driver...")
        try:
            driver.quit()
        except:
            pass
        return setup_driver()

# New helper: save a batch of results to JSON + CSV
def save_batch(results_batch, output_dir, start_idx, end_idx, timestamp):
	if not os.path.exists(output_dir):
		os.makedirs(output_dir)
	json_file = os.path.join(output_dir, f"selenium_results_{timestamp}_{start_idx:04d}_{end_idx:04d}.json")
	with open(json_file, 'w', encoding='utf-8') as f:
		json.dump(results_batch, f, indent=2, ensure_ascii=False)
	successful = [r for r in results_batch if r.get('success')]
	if successful:
		csv_file = os.path.join(output_dir, f"successful_content_{timestamp}_{start_idx:04d}_{end_idx:04d}.csv")
		with open(csv_file, 'w', newline='', encoding='utf-8') as f:
			writer = csv.writer(f)
			writer.writerow(['ID', 'Category', 'Domain', 'Title', 'Words', 'Lines', 'Real URL', 'Bing URL'])
			for result in successful:
				writer.writerow([
					result.get('original_id',''),
					result.get('category',''),
					result.get('domain',''),
					result.get('title','')[:100],
					result.get('word_count',0),
					result.get('line_count',0),
					result.get('real_url','')[:100],
					result.get('original_bing_url','')[:100]
				])
	print(f"  Saved batch {start_idx}-{end_idx}: {json_file}")

# ---------- EXTRACT REAL URL FROM BING ----------
def extract_real_url_with_selenium(driver, bing_url):
    """Use Selenium to get the real URL from Bing redirect"""
    try:
        print(f"  Visiting Bing URL...")
        driver.get(bing_url)
        
        # Wait for page to load
        time.sleep(3)
        
        # Get current URL - if redirect worked, this should be the real URL
        current_url = driver.current_url
        
        # Check if we're still on Bing
        if 'bing.com' in current_url:
            print(f"  Still on Bing, trying to find real link...")
            
            # Try to find and click the "Continue to site" button
            try:
                # Look for common Bing redirect buttons
                button_selectors = [
                    "a[href*='https://www.bing.com/ck/a']",  # Bing's own link
                    "a#bnp_btn_accept",  # Accept button
                    "a.continue",  # Continue button
                    "a[target='_blank']",  # External link
                    "a:not([href*='bing.com'])"  # Any non-Bing link
                ]
                
                for selector in button_selectors:
                    try:
                        link = driver.find_element(By.CSS_SELECTOR, selector)
                        if link:
                            link_url = link.get_attribute('href')
                            if link_url and 'bing.com' not in link_url:
                                print(f"  Found external link: {link_url[:60]}...")
                                return link_url
                    except:
                        continue
                
                # If no link found, try to extract from page source
                page_source = driver.page_source
                
                # Look for URL patterns in source
                url_patterns = [
                    r'https?://[^"\'\s<>]+',  # Standard URLs
                    r'&u=([^&]+)',  # Bing's u= parameter
                    r'redirectUrl[=:]\s*["\']([^"\']+)["\']'  # Redirect URLs
                ]
                
                for pattern in url_patterns:
                    matches = re.findall(pattern, page_source)
                    for match in matches:
                        if isinstance(match, tuple):
                            match = match[0]
                        if 'bing.com' not in match and match.startswith('http'):
                            print(f"  Found URL in source: {match[:60]}...")
                            return match
                
                # Last resort: check if there's a meta refresh
                meta_tags = driver.find_elements(By.CSS_SELECTOR, 'meta[http-equiv="refresh"]')
                for meta in meta_tags:
                    content = meta.get_attribute('content')
                    if content and 'url=' in content.lower():
                        url_part = content.split('url=')[-1].strip()
                        if url_part.startswith('http'):
                            print(f"  Found meta refresh URL: {url_part[:60]}...")
                            return url_part
            
            except Exception as e:
                print(f"  Error finding link: {str(e)[:50]}")
        
        return current_url
    
    except Exception as e:
        print(f"  Selenium error: {str(e)[:50]}")
        return bing_url


# ---------- EXTRACT CONTENT FROM REAL URL ----------
def extract_content_with_selenium(driver, url):
    """Extract content from a URL using Selenium"""
    try:
        print(f"  Visiting: {url[:60]}...")
        driver.get(url)
        
        # Wait for page to load
        time.sleep(3)
        
        # Get page source
        page_source = driver.page_source
        
        # Parse with BeautifulSoup
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            element.decompose()
        
        # Get title
        title = soup.title.string.strip() if soup.title else ""
        
        # Try to get main content
        content_text = ""
        
        # Try article tags first
        article_tags = soup.find_all(['article', 'main'])
        if article_tags:
            content_text = ' '.join([tag.get_text(strip=True) for tag in article_tags])
        
        # If no article tags, get all paragraphs
        if not content_text or len(content_text) < 300:
            paragraphs = soup.find_all('p')
            content_text = ' '.join([p.get_text(strip=True) for p in paragraphs])
        
        # If still too short, use html2text
        if len(content_text) < 200:
            h = html2text.HTML2Text()
            h.ignore_links = True
            h.ignore_images = True
            content_text = h.handle(str(soup))
        
        # Clean text
        content_text = re.sub(r'\s+', ' ', content_text).strip()
        
        # Count lines (sentences)
        sentences = [s.strip() for s in content_text.split('. ') if s.strip()]
        line_count = len(sentences)
        
        # Count words
        words = content_text.split()
        word_count = len(words)
        
        # Limit content length
        if len(content_text) > CONFIG['max_content_length']:
            content_text = content_text[:CONFIG['max_content_length']] + "..."
        
        return {
            'url': url,
            'title': title[:200],
            'content': content_text,
            'word_count': word_count,
            'line_count': line_count,
            'domain': urlparse(url).netloc,
            'success': True
        }
        
    except Exception as e:
        return {
            'url': url,
            'error': str(e)[:100],
            'success': False
        }


# ---------- PROCESS SINGLE URL ----------
def process_single_url(driver, bing_url, url_info):
    """Process a single Bing URL through Selenium"""
    print(f"\nProcessing: {bing_url[:60]}...")
    
    # Step 1: Extract real URL from Bing
    real_url = extract_real_url_with_selenium(driver, bing_url)
    
    # Step 2: If still on Bing, try to decode the u= parameter
    if 'bing.com' in real_url:
        print(f"  Still on Bing, trying to decode...")
        
        # Try to extract the u= parameter
        match = re.search(r'&u=([^&]+)', bing_url)
        if match:
            encoded_url = match.group(1)
            
            # Try to URL decode
            from urllib.parse import unquote
            decoded = unquote(encoded_url)
            
            # Try Base64 decode if it looks encoded
            if not decoded.startswith('http') and len(decoded) > 10:
                try:
                    import base64
                    # Add padding if needed
                    padding = (-len(decoded) % 4)
                    if padding:
                        decoded += '=' * padding
                    real_url = base64.b64decode(decoded).decode('utf-8')
                    print(f"  Decoded from Base64: {real_url[:60]}...")
                except:
                    pass
            elif decoded.startswith('http'):
                real_url = decoded
                print(f"  Decoded from URL encoding: {real_url[:60]}...")
    
    # Step 3: Extract content from real URL
    if 'bing.com' not in real_url:
        result = extract_content_with_selenium(driver, real_url)
    else:
        result = {
            'url': bing_url,
            'real_url': real_url,
            'error': 'Could not extract real URL from Bing',
            'success': False
        }
    
    # Add metadata
    result.update({
        'original_bing_url': bing_url,
        'real_url': real_url,
        'original_id': url_info['id'],
        'query': url_info['query'],
        'category': url_info['category']
    })
    
    return result


# NOUVEAU helper: construire urls_data depuis la config (supporte strings ou dicts)
def urls_from_config(cfg_urls, limit=None):
    urls_data = []
    if not cfg_urls:
        return urls_data
    for i, item in enumerate(cfg_urls):
        if limit and i >= limit:
            break
        if isinstance(item, str):
            urls_data.append({
                'id': str(i+1),
                'url': item,
                'query': '',
                'category': 'uncategorized'
            })
        elif isinstance(item, dict):
            url_val = item.get('URL') or item.get('url') or item.get('link') or ''
            urls_data.append({
                'id': str(item.get('ID') or item.get('id') or (i+1)),
                'url': url_val,
                'query': item.get('Query') or item.get('query') or '',
                'category': item.get('Category') or item.get('category') or 'uncategorized'
            })
    return urls_data


# ---------- MAIN PROCESSING ----------
def main():
    print("="*80)
    print("SELENIUM CONTENT FETCHER")
    print("Handles Bing redirects with real browser simulation")
    print("="*80)
    
    # Si la config contient des URLs, les utiliser directement (pas de prompt)
    if DEFAULT_URLS:
        # Use all URLs from config without prompting
        cfg_urls = DEFAULT_URLS
        urls_data = urls_from_config(cfg_urls, limit=None)
        print(f"Using {len(urls_data)} URLs from config (urls.json) — processing all.")
    else:
        # Get input file (use config default if available)
        prompt = f"Enter path to your CSV file [{DEFAULT_INPUT_CSV}]: " if DEFAULT_INPUT_CSV else "Enter path to your CSV file: "
        input_file = input(prompt).strip() or DEFAULT_INPUT_CSV
        
        if not os.path.exists(input_file):
            print(f"Error: File '{input_file}' not found!")
            return
        
        # Ask how many URLs to process
        print(f"\nNote: Selenium is slower but handles Bing redirects.")
        limit_input = input(f"How many URLs to process? (Enter for {DEFAULT_LIMIT}, or number): ").strip()
        if limit_input:
            try:
                limit = int(limit_input)
                print(f"Will process first {limit} URLs.")
            except:
                print(f"Invalid input, will process {DEFAULT_LIMIT} URLs.")
                limit = DEFAULT_LIMIT
        else:
            limit = DEFAULT_LIMIT

        # Read URLs from CSV or JSON
        if input_file.lower().endswith('.json'):
            urls_data = read_urls_from_json(input_file, limit=limit)
        else:
            urls_data = read_urls_from_csv(input_file, limit=limit)
    
    if not urls_data:
        print("No URLs found to process!")
        return
    
    print(f"✓ Read {len(urls_data)} URLs from CSV")
    
    # Confirm
    confirm = input(f"\nReady to process {len(urls_data)} URLs with Selenium. Continue? (y/n): ").strip().lower()
    if confirm not in ['y', 'yes']:
        print("Aborted.")
        return
    
    # Setup Selenium driver
    print("\nInitializing Selenium Chrome driver...")
    driver = setup_driver()
    
    # Prepare output timestamp and directory early for batch saves
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = CONFIG.get('output_directory', 'selenium_content')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    all_results = []
    stats = {
        'total': len(urls_data),
        'success': 0,
        'failed': 0,
        'total_words': 0,
        'total_lines': 0
    }
    
    try:
        print(f"\nProcessing {len(urls_data)} URLs with Selenium...")
        print("=" * 80)
        
        for i, url_info in enumerate(urls_data, 1):
            bing_url = url_info['url']
            
            print(f"\n[{i}/{len(urls_data)}] Processing URL {i}...")
            
            # Ensure driver is alive before each URL
            driver = ensure_driver_alive(driver)
            
            # Try processing, on WebDriver issues restart and retry once
            try:
                result = process_single_url(driver, bing_url, url_info)
            except WebDriverException as e:
                # If session invalid, restart driver and retry once
                err_msg = str(e).lower()
                print(f"  Selenium error: {str(e)[:80]}")
                driver = ensure_driver_alive(driver)
                try:
                    result = process_single_url(driver, bing_url, url_info)
                except Exception as e2:
                    print(f"  Second attempt failed: {str(e2)[:80]}")
                    result = {
                        'url': bing_url,
                        'error': 'Driver error after retry',
                        'success': False,
                        'original_bing_url': bing_url,
                        'real_url': bing_url,
                        'original_id': url_info.get('id'),
                        'query': url_info.get('query',''),
                        'category': url_info.get('category','')
                    }
            except KeyboardInterrupt:
                raise
            except Exception as e:
                # generic failure for this URL
                print(f"  Error processing URL: {str(e)[:80]}")
                result = {
                    'url': bing_url,
                    'error': str(e)[:100],
                    'success': False,
                    'original_bing_url': bing_url,
                    'real_url': bing_url,
                    'original_id': url_info.get('id'),
                    'query': url_info.get('query',''),
                    'category': url_info.get('category','')
                }
            
            all_results.append(result)
            
            # Update stats
            if result.get('success'):
                stats['success'] += 1
                stats['total_words'] += result.get('word_count',0)
                stats['total_lines'] += result.get('line_count',0)
                domain = result.get('domain','')
                words = result.get('word_count',0)
                lines = result.get('line_count',0)
                print(f"  ✓ SUCCESS: {domain} | {words} words | {lines} lines")
                if result.get('content'):
                    preview = result['content'][:100].replace('\n', ' ')
                    print(f"  Preview: {preview}...")
            else:
                stats['failed'] += 1
                error = result.get('error', 'Unknown')
                print(f"  ✗ FAILED: {error}")
            
            # Batch save every CONFIG['batch_size'] results
            batch_size = int(CONFIG.get('batch_size', 100))
            if batch_size > 0 and i % batch_size == 0:
                start_idx = i - (batch_size - 1)
                end_idx = i
                batch = all_results[start_idx-1:end_idx]
                save_batch(batch, output_dir, start_idx, end_idx, timestamp)
            
            # Delay between URLs
            if i < len(urls_data):
                print(f"  Waiting {CONFIG['delay_between_requests']} seconds...")
                time.sleep(CONFIG['delay_between_requests'])
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, stopping early...")
    finally:
        # Always close the driver
        print("\nClosing Selenium driver...")
        try:
            driver.quit()
        except:
            pass
    
    # After loop: save remaining results if not saved in last full batch
    total_saved_batches = len(all_results) // 100
    saved_count = total_saved_batches * 100
    if len(all_results) > saved_count:
        start_idx = saved_count + 1
        end_idx = len(all_results)
        batch = all_results[start_idx-1:end_idx]
        save_batch(batch, output_dir, start_idx, end_idx, timestamp)
    
    # Note: detailed full JSON save retained for backward compatibility
    if all_results:
        full_json = os.path.join(output_dir, f"selenium_results_full_{timestamp}.json")
        with open(full_json, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        print(f"  Full results saved: {full_json}")
    
    # Display statistics
    print(f"\n{'='*80}")
    print("PROCESSING COMPLETE")
    print("=" * 80)
    
    print(f"\nStatistics:")
    print(f"  Total URLs processed: {stats['total']}")
    print(f"  ✓ Successful: {stats['success']}")
    print(f"  ✗ Failed: {stats['failed']}")
    print(f"  Success rate: {stats['success']/stats['total']*100:.1f}%")
    
    if stats['success'] > 0:
        print(f"\nContent Extracted:")
        print(f"  Total words: {stats['total_words']:,}")
        print(f"  Total lines: {stats['total_lines']:,}")
        print(f"  Avg words/article: {stats['total_words']//stats['success']:,}")
        print(f"  Avg lines/article: {stats['total_lines']//stats['success']:,}")
    
    print(f"\nFiles Saved:")
    print(f"  Output folder: {output_dir}")
    
    print(f"\n✓ Done! Check the '{output_dir}' folder for results.")


if __name__ == "__main__":
    main()