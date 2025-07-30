#!/usr/bin/env python3
import os
import sys
import time as time_module
import requests
import pandas as pd
import base64
from datetime import datetime, timedelta, timezone
from requests.auth import HTTPBasicAuth
from requests import RequestException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

start_time = time_module.time()

# --- CONFIGURAÇÃO VIA AMBIENTE ---
personal_access_token = os.getenv('AZURE_PAT')
if not personal_access_token:
    print("❌ Erro: variável AZURE_PAT não definida.")
    sys.exit(1)

# Org e múltiplos projetos
ORG = os.getenv('ORG', 'arezzosa').strip()
projects_env = os.getenv('PROJECT', 'FUTURO_E-COMMERCE,ARZZ,ZZAPPS').strip()
PROJECTS = [p.strip() for p in projects_env.split(',') if p.strip()]

# Sessão HTTP com retry e pool
session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
session.mount("https://", adapter)

# Autenticação e headers padrão
token = base64.b64encode(f":{personal_access_token}".encode()).decode()
session.auth = HTTPBasicAuth('', personal_access_token)
session.headers.update({'Authorization': f'Basic {token}', 'Content-Type': 'application/json'})
session.params = {'api-version': '7.0'}

# Janela de dias configurável
days_window = int(os.getenv('AZURE_DAYS_WINDOW', '730'))
now = datetime.now(timezone.utc)
start_date = (now - timedelta(days=days_window)).date()
start_iso = f"{start_date.isoformat()}T00:00:00Z"
print(f"Buscando itens movimentados de {start_date} até {now.date()}...\n")

# Tipos permitidos
allowed_types = ["Feature", "User Story", "Spike", "Bug", "Fix", "Vulnerability"]
all_rows = []
closed_date_cache = {}

# Batch fetch de ClosedDate para ids
def fetch_closed_dates(ids):
    BATCH_SIZE = 200
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i:i+BATCH_SIZE]
        body = {
            'ids': batch,
            'fields': ['Microsoft.VSTS.Common.ClosedDate']
        }
        try:
            resp = session.post(
                f"https://dev.azure.com/{ORG}/_apis/wit/workitemsbatch",
                json=body, timeout=10
            )
            resp.raise_for_status()
            for wi in resp.json().get('value', []):
                wid = str(wi.get('id'))
                closed_date_cache[wid] = wi.get('fields', {}) \
                    .get('Microsoft.VSTS.Common.ClosedDate', '') or ''
        except RequestException:
            for wid in batch:
                closed_date_cache[str(wid)] = ''

# Função paginada para buscar **todas** as revisões
def fetch_all_revisions(wid, project, select_fields=None):
    all_revs = []
    skip = 0
    page_size = 200
    base_url = f"https://dev.azure.com/{ORG}/{project}/_apis/wit/workitems/{wid}/revisions"

    while True:
        params = {
            'api-version': '7.0',
            '$top': page_size,
            '$skip': skip
        }
        if select_fields:
            params['$select'] = select_fields

        resp = session.get(base_url, params=params, timeout=10)
        resp.raise_for_status()
        revs = resp.json().get('value', [])
        all_revs.extend(revs)

        # se vier menos que o page_size, era o último lote
        if len(revs) < page_size:
            break
        skip += page_size

    return all_revs

# Processa revisões de um work item (usa a paginação agora)
def process_revisions(wid, project, prefs_map, all_teams, start_iso):
    closed_date_global = closed_date_cache.get(str(wid), '')

    select_fields = (
        "fields(System.ChangedDate,System.Tags,System.State,"
        "System.AreaPath,Microsoft.VSTS.Common.ValueArea,"
        "Microsoft.VSTS.Common.ClosedDate)"
    )
    try:
        revs = fetch_all_revisions(wid, project, select_fields)
    except RequestException:
        return []

    if not revs:
        return []

    # ordena por número da revisão
    revs = sorted(revs, key=lambda x: x['rev'])

    # Dados atuais da última revisão
    last = revs[-1]['fields']
    actual_state = last.get('System.State', '')
    actual_area = last.get('System.AreaPath', '')
    actual_value_area = last.get('Microsoft.VSTS.Common.ValueArea', '')

    # ——— Extrai ActualTeam a partir de actual_area ———
    actual_team_source = actual_area or ''
    parts = actual_team_source.split('\\')
    actual_team = prefs_map.get(actual_team_source, [None])[0] if actual_team_source in prefs_map else None
    for seg in reversed(parts):
        if not actual_team and seg in all_teams:
            actual_team = seg
    if not actual_team:
        actual_team = project
    # ——————————————————————————————————————————————

    prev_tags = None
    rows = []

    for rev in revs:
        flds = rev['fields']
        changed = flds.get('System.ChangedDate', '')
        if not changed or changed < start_iso:
            continue

        tag_set = set(
            t.strip().upper() for t in flds.get('System.Tags', '').split(';') if t.strip()
        )
        if prev_tags is None:
            prev_tags = tag_set
            continue

        # Identifica equipe
        team_source = actual_area or flds.get('System.AreaPath', '')
        parts = team_source.split('\\')
        team_name = prefs_map.get(team_source, [None])[0] \
            if team_source in prefs_map else None
        for seg in parts[::-1]:
            if not team_name and seg in all_teams:
                team_name = seg
        if not team_name:
            team_name = project

        # Detecta tags BLOCKED_* e PAUSED
        current_blocked = {t for t in tag_set if t.startswith('BLOCKED')}
        prev_blocked = {t for t in prev_tags if t.startswith('BLOCKED')}
        current_paused = {'PAUSED'} if 'PAUSED' in tag_set else set()
        prev_paused = {'PAUSED'} if 'PAUSED' in prev_tags else set()

        # Gera eventos de mudança
        events = []
        for tag in current_blocked - prev_blocked:
            events.append((tag, 'Added'))
        for tag in prev_blocked - current_blocked:
            events.append((tag, 'Removed'))
        if (current_paused - prev_paused) or (prev_paused - current_paused):
            action = 'Added' if current_paused - prev_paused else 'Removed'
            events.append(('PAUSED', action))

        # Adiciona linhas para cada evento
        for tag, action in events:
            rows.append({
                'Project': project,
                'Team': team_name,
                'WorkItemId': wid,
                'Revision': rev['rev'],
                'ChangedDate': changed,
                'WorkItemType': flds.get('System.WorkItemType', ''),
                'Title': flds.get('System.Title', ''),
                'AreaPath': flds.get('System.AreaPath', ''),
                'ValueArea': actual_value_area,
                'State': flds.get('System.State', ''),
                'Tags': ';'.join(sorted(tag_set)),
                'ClosedDate': flds.get('Microsoft.VSTS.Common.ClosedDate') or closed_date_global,
                'ActualState': actual_state,
                'ActualAreaPath': actual_area,
                'ActualTeam': actual_team,
                'ActualValueArea': actual_value_area,
                'Keyword': 'BLOCKED' if tag.startswith('BLOCKED') else tag,
                'Tag': tag if tag.startswith('BLOCKED') else '',
                'Action': action
            })
        prev_tags = tag_set

    return rows

# Execução principal
for project in PROJECTS:
    print(f"=== Projeto: {project} ===")
    try:
        # Carrega equipes e prefs_map apenas uma vez por projeto
        teams_resp = session.get(
            f"https://dev.azure.com/{ORG}/_apis/projects/{project}/teams",
            timeout=10
        )
        teams_resp.raise_for_status()
        teams = [t['name'] for t in teams_resp.json().get('value', [])]

        prefs_map = {}
        for team in teams:
            vr = session.get(
                f"https://dev.azure.com/{ORG}/{project}/{team}"
                "/_apis/work/teamsettings/teamfieldvalues",
                timeout=10
            )
            vr.raise_for_status()
            for v in vr.json().get('values', []):
                val = v.get('value', '').strip()
                if val:
                    prefs_map.setdefault(val, []).append(team)
        all_teams = set(teams)

        # Consulta WIQL e busca batch de ClosedDates
        fields_list = ",".join(f"'{t}'" for t in allowed_types)
        wiql = {
            'query': (
                "SELECT [System.Id] FROM WorkItems "
                f"WHERE [System.TeamProject]='{project}' "
                f"AND [System.WorkItemType] IN ({fields_list}) "
                f"AND [System.ChangedDate]>='{start_date.isoformat()}T00:00:00Z'"
            )
        }

        wr = session.post(
            f"https://dev.azure.com/{ORG}/{project}/_apis/wit/wiql",
            json=wiql, timeout=10
        )
        wr.raise_for_status()
        ids = [str(w['id']) for w in wr.json().get('workItems', [])]
        print(f"✅ {len(ids)} itens encontrados.")

        fetch_closed_dates(ids)

        # Paraleliza processamentos
        with ThreadPoolExecutor(max_workers=20) as exe:
            futures = [
                exe.submit(process_revisions, wid, project,
                           prefs_map, all_teams, start_iso)
                for wid in ids
            ]
            for fut in as_completed(futures):
                all_rows.extend(fut.result())

    except RequestException as e:
        print(f"❌ Erro no projeto {project}: {e}")
        continue

# Exporta resultados
df = pd.DataFrame(all_rows)
print(f"\nSalvando {len(all_rows)} registros no CSV/XLSX...")
df.to_csv("movements_blocked_paused_all.csv",
          index=False, encoding='utf-8-sig')
df.to_excel("movements_blocked_paused_all.xlsx", index=False)
print("✅ Export concluído")
print(
    f"⏱️ Execução: {int((time_module.time()-start_time)//60)}m "
    f"{int((time_module.time()-start_time)%60)}s"
)
