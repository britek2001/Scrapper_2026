import json
import os
import re
import argparse
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup


def parse_profile(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, 'html.parser')
    data: Dict[str, Any] = {}
    # Name
    name_elem = soup.find('h1', class_='data-header__headline-wrapper') or soup.find('h1')
    if name_elem:
        data['name'] = name_elem.get_text(' ', strip=True)

    # Current club
    club = soup.find('span', class_='data-header__club')
    if club:
        a = club.find('a')
        data['current_club'] = a.get_text(strip=True) if a else club.get_text(strip=True)

    # Shirt number
    sn = soup.find('span', class_='data-header__shirt-number')
    if sn:
        data['shirt_number'] = sn.get_text(strip=True)

    # Basic info list
    info: Dict[str, str] = {}
    for ul in soup.find_all('ul', class_='data-header__items'):
        for li in ul.find_all('li'):
            text = li.get_text(':', strip=True)
            if ':' in text:
                k, v = [p.strip() for p in text.split(':', 1)]
                info[k] = v
    if info:
        data['basic_info'] = info

    # Market value
    mv = soup.find('a', class_='data-header__market-value-wrapper')
    if mv:
        data['market_value'] = mv.get_text(' ', strip=True)

    return data


def parse_table(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, 'html.parser')
    results: List[Dict[str, str]] = []
    tables = soup.find_all('table', class_='items') or soup.find_all('table')
    if not tables:
        return results

    # Use the first table by default
    table = tables[0]
    headers: List[str] = []
    thead = table.find('thead')
    if thead:
        for th in thead.find_all('th'):
            txt = th.get_text(' ', strip=True)
            headers.append(txt if txt else f'col_{len(headers)+1}')

    tbody = table.find('tbody') or table
    for tr in tbody.find_all('tr'):
        cols = tr.find_all(['td', 'th'])
        if not cols:
            continue
        row: Dict[str, str] = {}
        for i, td in enumerate(cols):
            key = headers[i] if i < len(headers) else f'col_{i+1}'
            row[key] = td.get_text(' ', strip=True)
        if row:
            results.append(row)

    return results


def parse_transfers(html: str) -> List[Dict[str, str]]:
    rows = parse_table(html)
    normalized: List[Dict[str, str]] = []
    for r in rows:
        # try to map common columns
        keys = {k.lower(): k for k in r.keys()}
        entry: Dict[str, str] = {}
        # heuristics
        if 'season' in keys:
            entry['season'] = r[keys['season']]
        if 'date' in keys:
            entry['date'] = r[keys['date']]
        # from/to club
        for k in ('from', 'from club', 'ab', 'verein-von'):
            if k in keys:
                entry['from_club'] = r[keys[k]]
                break
        for k in ('to', 'to club', 'nach', 'verein-zu'):
            if k in keys:
                entry['to_club'] = r[keys[k]]
                break
        for k in ('market value', 'market_value', 'marktwert'):
            if k in keys:
                entry['market_value'] = r[keys[k]]
                break
        for k in ('fee', 'ablöse', 'transfer fee'):
            if k in keys:
                entry['fee'] = r[keys[k]]
                break
        # fallback: include all original cols
        if not entry:
            entry = r
        normalized.append(entry)
    return normalized


def parse_injuries(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all('table', class_='items') or soup.find_all('table')
    injuries = []
    totals = {}
    if tables:
        # first table: injuries list
        t0 = tables[0].find('tbody') if tables[0].find('tbody') else tables[0]
        for row in t0.find_all('tr'):
            cols = [c.get_text(' ', strip=True) for c in row.find_all('td')]
            if not cols:
                continue
            # basic mapping
            item = {
                'season': cols[0] if len(cols) > 0 else '',
                'injury_type': cols[1] if len(cols) > 1 else '',
                'from': cols[2] if len(cols) > 2 else '',
                'until': cols[3] if len(cols) > 3 else '',
                'days_out': cols[4] if len(cols) > 4 else ''
            }
            injuries.append(item)

        if len(tables) > 1:
            t1 = tables[1].find('tbody') if tables[1].find('tbody') else tables[1]
            for row in t1.find_all('tr'):
                cols = [c.get_text(' ', strip=True) for c in row.find_all('td')]
                if not cols:
                    continue
                season = cols[0] if cols else ''
                totals[season] = {
                    'total_days': cols[1] if len(cols) > 1 else '',
                    'injuries_count': cols[2] if len(cols) > 2 else '',
                    'games_missed': cols[3] if len(cols) > 3 else ''
                }

    return {'injuries_list': injuries, 'season_totals': totals}


def parse_performance(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, 'html.parser')
    data: Dict[str, Any] = {}
    tables = soup.find_all('table', class_='items') or soup.find_all('table')
    for i, table in enumerate(tables, start=1):
        headers = []
        thead = table.find('thead')
        if thead:
            for th in thead.find_all('th'):
                txt = th.get_text(' ', strip=True)
                headers.append(txt if txt else f'col_{len(headers)+1}')

        tbody = table.find('tbody') or table
        rows = []
        for tr in tbody.find_all('tr'):
            cols = tr.find_all(['td', 'th'])
            if not cols:
                continue
            row = {}
            for j, td in enumerate(cols):
                key = headers[j] if j < len(headers) else f'col_{j+1}'
                row[key] = td.get_text(' ', strip=True)
            if row:
                rows.append(row)
        if rows:
            data[f'table_{i}'] = {'headers': headers, 'rows': rows}
    return data


def parse_achievements(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, 'html.parser')
    achievements = {}
    boxes = soup.find_all('div', class_='box')
    if not boxes:
        # fallback to tables
        tbls = soup.find_all('table')
        for i, t in enumerate(tbls, start=1):
            achievements[f'table_{i}'] = parse_table(str(t))
        return achievements

    for box in boxes:
        title_elem = box.find(['h2', 'div', 'th'])
        title = title_elem.get_text(' ', strip=True) if title_elem else 'achievements'
        items = []
        tbl = box.find('table')
        if tbl:
            for tr in tbl.find_all('tr'):
                tds = tr.find_all('td')
                if not tds:
                    continue
                row = [td.get_text(' ', strip=True) for td in tds]
                items.append(row)
        achievements[title] = items
    return achievements


def parse_kit_numbers(html: str) -> List[Dict[str, str]]:
    """Parse Rückennummern / kit numbers pages into list of {season, club, number}."""
    soup = BeautifulSoup(html, 'html.parser')
    results: List[Dict[str, str]] = []
    tables = soup.find_all('table')
    if not tables:
        return results
    # try to find table with header 'No.' or 'Rückennummer'
    table = tables[0]
    headers = [th.get_text(' ', strip=True).lower() for th in (table.find('thead') or table).find_all('th')]
    tbody = table.find('tbody') or table
    for tr in tbody.find_all('tr'):
        cols = [c.get_text(' ', strip=True) for c in tr.find_all(['td', 'th'])]
        if not cols:
            continue
        item = {}
        # heuristics
        if len(cols) >= 3:
            item['season'] = cols[0]
            item['club'] = cols[1]
            item['number'] = cols[2]
        else:
            # fallback map by header names
            for i, h in enumerate(headers):
                if 'season' in h or 'saison' in h:
                    item['season'] = cols[i] if i < len(cols) else ''
                if 'verein' in h or 'club' in h:
                    item['club'] = cols[i] if i < len(cols) else ''
                if 'nummer' in h or 'no' in h or 'rücken' in h:
                    item['number'] = cols[i] if i < len(cols) else ''
        results.append(item)
    return results


def parse_losses(html: str) -> List[Dict[str, str]]:
    """Parse losses/wins pages; reuse generic table parser but normalize some keys."""
    rows = parse_table(html)
    normalized = []
    for r in rows:
        entry = r.copy()
        # try to normalize date/team/result columns
        normalized.append(entry)
    return normalized


def parse_market_value(html: str) -> Dict[str, Any]:
    """Try to extract market value time series. Fallback to table extraction."""
    soup = BeautifulSoup(html, 'html.parser')
    # try to find JSON inside scripts (common pattern)
    scripts = soup.find_all('script')
    for s in scripts:
        txt = s.string or ''
        if 'chart' in txt.lower() or 'series' in txt.lower():
            # try to find array-like structure
            m = re.search(r"\[\s*\{.+?\}\s*\]", txt, re.S)
            if m:
                try:
                    data = json.loads(m.group(0))
                    return {'series': data}
                except Exception:
                    pass
    # fallback: tables
    tbls = soup.find_all('table')
    if tbls:
        return {'tables': [parse_table(str(t)) for t in tbls]}
    return {}


def parse_news(html: str) -> List[Dict[str, str]]:
    """Extract simple news items: title, date, link, excerpt."""
    soup = BeautifulSoup(html, 'html.parser')
    items = []
    # look for common containers
    containers = soup.find_all('div', class_=re.compile(r'news|box|article', re.I))
    for c in containers:
        a = c.find('a')
        title = a.get_text(' ', strip=True) if a else c.find(['h2', 'h3']) and c.find(['h2', 'h3']).get_text(' ', strip=True)
        if not title:
            continue
        date = ''
        date_elem = c.find('span', class_=re.compile(r'date|zeit|datum', re.I))
        if date_elem:
            date = date_elem.get_text(' ', strip=True)
        link = a['href'] if a and a.has_attr('href') else ''
        excerpt_elem = c.find('p')
        excerpt = excerpt_elem.get_text(' ', strip=True) if excerpt_elem else ''
        items.append({'title': title, 'date': date, 'link': link, 'excerpt': excerpt})
    # fallback to table rows
    if not items:
        return parse_table(html)
    return items


def parse_debuts(html: str) -> List[Dict[str, str]]:
    return parse_table(html)


def parse_goal_involvements(html: str) -> List[Dict[str, str]]:
    return parse_table(html)


def detect_page_type_from_path(path: str) -> Optional[str]:
    name = os.path.basename(path).lower()
    if 'transfers' in name:
        return 'transfers'
    if 'siege' in name or 'wins' in name:
        return 'wins'
    if 'meistetore' in name or 'top_goals' in name or 'meistetorbeteiligungen' in name:
        return 'top_goals'
    if 'elfmetertore' in name or 'penalty' in name:
        return 'penalty_goals'
    if 'profil' in name or 'profile' in name:
        return 'profile'
    if 'rueckennummern' in name or 'ruecken' in name:
        return 'rueckennummern'
    if 'niederlagen' in name or 'losses' in name:
        return 'losses'
    if 'marktwertverlauf' in name or 'marktwert' in name:
        return 'market_value'
    if 'news' in name:
        return 'news'
    if 'nationalmannschaft' in name or 'national_team' in name:
        return 'national_team'
    if 'debuets' in name or 'debuts' in name:
        return 'debuts'
    if 'leistungsdatendetails' in name or 'performance_details' in name:
        return 'performance_details'
    return None


def parse_file(path: str, page_type: Optional[str] = None) -> Any:
    with open(path, 'r', encoding='utf-8') as f:
        html = f.read()

    if not page_type:
        page_type = detect_page_type_from_path(path)

    # Dispatch to specialized parsers based on detected page type
    if page_type == 'profile':
        return parse_profile(html)

    if page_type == 'transfers':
        return parse_transfers(html)

    if page_type == 'injuries' or 'verletzungen' in (page_type or ''):
        return parse_injuries(html)

    if page_type in ('performance', 'performance_details', 'detailed_performance',
                     'club_performance', 'coach_performance', 'bilanz', 'balance'):
        return parse_performance(html)

    if page_type in ('erfolge', 'achievements'):
        return parse_achievements(html)

    if page_type == 'rueckennummern':
        return parse_kit_numbers(html)

    if page_type == 'losses' or page_type == 'wins':
        return parse_losses(html)

    if page_type == 'market_value':
        return parse_market_value(html)

    if page_type == 'news':
        return parse_news(html)

    if page_type == 'national_team':
        return parse_table(html)

    if page_type == 'debuts':
        return parse_debuts(html)

    if page_type == 'goal_involvements' or page_type == 'meistetorbeteiligungen':
        return parse_goal_involvements(html)

    if page_type in ('wins', 'top_goals', 'penalty_goals', 'meistetore', 'meistetorbeteiligungen', 'national_team'):
        return parse_table(html)

    # Fallback: try profile first, then performance/table
    prof = parse_profile(html)
    if prof:
        return prof
    perf = parse_performance(html)
    if perf:
        return perf
    return parse_table(html)


def extract_links(html: str) -> List[Dict[str, str]]:
    """Return list of links found in the HTML with href and text."""
    soup = BeautifulSoup(html, 'html.parser')
    links: List[Dict[str, str]] = []
    for a in soup.find_all('a', href=True):
        href = a['href'].strip()
        text = a.get_text(' ', strip=True)
        links.append({'href': href, 'text': text})
    return links


def main():
    parser = argparse.ArgumentParser(description='Transfermarkt HTML parser')
    parser.add_argument('path', help='Path to HTML file')
    parser.add_argument('--type', help='Page type (profile, transfers, wins, top_goals, penalty_goals)')
    parser.add_argument('--links', action='store_true', help='Also extract and include links found on the page')
    args = parser.parse_args()

    res = parse_file(args.path, args.type)
    if args.links:
        with open(args.path, 'r', encoding='utf-8') as f:
            html = f.read()
        links = extract_links(html)
        out = {'page_type': args.type or detect_page_type_from_path(args.path), 'data': res, 'links': links}
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
