#!/usr/bin/env python3
import os
import sys
import time as time_module
import requests
import pandas as pd
import base64
from datetime import datetime
from bs4 import BeautifulSoup

# ─────── Configurações de API de Teams/Areas ───────
API_VERSION_TEAMS = "7.1-preview.3"
API_VERSION_AREAS = "7.1-preview.1"

def list_teams(org: str, project: str, headers: dict) -> list[dict]:
    url    = f"https://dev.azure.com/{org}/_apis/projects/{project}/teams"
    params = {"api-version": API_VERSION_TEAMS}
    resp   = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return resp.json().get("value", [])

def get_team_areas(org: str, project: str, team_name: str, headers: dict) -> list[str]:
    url    = (
        f"https://dev.azure.com/{org}/{project}/{team_name}"
        "/_apis/work/teamsettings/teamfieldvalues"
    )
    params = {"api-version": API_VERSION_AREAS}
    resp   = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return [v.get("value","") for v in resp.json().get("values", [])]

# --- CONFIGURAÇÃO DE AUTENTICAÇÃO ---
personal_access_token = os.environ.get('AZURE_PAT')
if not personal_access_token:
    print("ERRO CRÍTICO: A variável de ambiente AZURE_PAT não foi encontrada.")
    sys.exit(1)

base64_pat = base64.b64encode(f":{personal_access_token}".encode()).decode()
headers = {'Authorization': f'Basic {base64_pat}'}

# Organização e lista de projetos
organization = os.environ.get('ORG', 'arezzosa')
projects_env = os.environ.get('PROJECT', 'FUTURO_E-COMMERCE,ZZAPPS,ARZZ')
projects = [p.strip() for p in projects_env.split(',') if p.strip()]

# --- FUNÇÕES DE API ---

def get_epics(org: str, proj: str) -> list[int]:
    url = (
        f"https://analytics.dev.azure.com/{org}/{proj}"
        "/_odata/v3.0-preview/WorkItems"
        "?$filter=WorkItemType eq 'Epic'"
    )
    resp = requests.get(url, headers=headers); resp.raise_for_status()
    df = pd.json_normalize(resp.json().get('value', []))
    return df[df['TagNames'].str.contains('PROJETOS', na=False)]['WorkItemId'].tolist()

def get_children(org: str, proj: str, epic_id: int) -> list[int]:
    url = (
        f"https://dev.azure.com/{org}/{proj}"
        f"/_apis/wit/workitems/{epic_id}"
        "?$expand=relations&api-version=7.0"
    )
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        return []
    return [
        int(r['url'].rsplit('/',1)[-1])
        for r in resp.json().get('relations', [])
        if r.get('rel')=='System.LinkTypes.Hierarchy-Forward'
    ]

def get_work_item(org: str, proj: str, wid: int) -> dict:
    url = (
        f"https://dev.azure.com/{org}/{proj}"
        f"/_apis/wit/workitems/{wid}?api-version=7.0"
    )
    resp = requests.get(url, headers=headers)
    return resp.json().get('fields', {}) if resp.status_code==200 else {}

def get_last_comment(org: str, proj: str, wid: int) -> str:
    url = (
        f"https://dev.azure.com/{org}/{proj}"
        f"/_apis/wit/workitems/{wid}/comments?api-version=7.0-preview.3"
    )
    resp = requests.get(url, headers=headers)
    if resp.status_code!=200:
        return ""
    comments = resp.json().get('comments',[])
    if not comments:
        return ""
    html = comments[-1].get('text','')
    return BeautifulSoup(html,'html.parser').get_text(separator=' ', strip=True)

# --- MAIN ---

def main():
    start_time = time_module.time()
    try:
        import openpyxl
    except ImportError:
        print("⚠️ Pacote openpyxl não instalado. Use: pip install openpyxl")

    all_issues = []
    for project in projects:
        print(f"\n▶ Projeto '{project}': carregando squads…")
        # monta mapa team→[areaPaths]
        teams = list_teams(organization, project, headers)
        squads_env = os.environ.get('AZURE_SQUADS','').strip()
        if squads_env:
            desired = [s.strip() for s in squads_env.split(',') if s.strip()]
            teams = [t for t in teams if t['name'] in desired]
        team_area_map = {
            t['name']: get_team_areas(organization, project, t['name'], headers)
            for t in teams
        }


        prefs_map = {}
        all_teams = []
        for tn, prefs in team_area_map.items():
            all_teams.append(tn)
            for p in prefs:
                prefs_map.setdefault(p, []).append(tn)


        print(f"▶ Projeto '{project}': buscando épicos com tag 'PROJETOS'…")
        epic_ids = get_epics(organization, project)
        print(f"   → {len(epic_ids)} épicos encontrados.")

        for idx, eid in enumerate(epic_ids, start=1):
            print(f"   • [{idx}/{len(epic_ids)}] Épico {eid}: buscando filhos…")

            for cid in get_children(organization, project, eid):
                
                fields = get_work_item(organization, project, cid)
                if fields.get('System.WorkItemType')!='Issue':
                    continue

                # extrai paths para determinar Team
                area      = fields.get('System.AreaPath','')

                parts = area.split('\\')

                # 1) match exato de prefixo único
                if area in prefs_map and len(prefs_map[area]) == 1:
                    team_name = prefs_map[area][0]
                # 2) terceiro nível do AreaPath
                elif len(parts) >= 3 and parts[2] in all_teams:
                    team_name = parts[2]
                # 3) segundo nível do AreaPath
                elif len(parts) >= 2 and parts[1] in all_teams:
                    team_name = parts[1]
                # 4) primeiro nível do AreaPath
                elif len(parts) >= 1 and parts[0] in all_teams:
                    team_name = parts[0]
                else:
                    team_name = project


                # monta linha
                issue = {
                    'Project':           project,
                    'Team':              team_name,
                    'ParentWorkItemID':  eid,
                    'WorkItemID':        cid,
                    'WorkItemType':      fields.get('System.WorkItemType',''),
                    'Title':             fields.get('System.Title',''),
                    'State':             fields.get('System.State',''),
                    'AreaPath':          area,
                    'CreatedDate':       fields.get('System.CreatedDate',''),
                    'Description':       BeautifulSoup(fields.get('System.Description',''),'html.parser')
                                              .get_text(separator=' ', strip=True),
                    'AssignedTo':        (fields.get('System.AssignedTo') or {}).get('displayName',''),
                    'Probabilidade':     fields.get('Custom.Probabilidade',''),
                    'Impacto':           fields.get('Custom.Impacto',''),
                    'DueDate':           fields.get('Microsoft.VSTS.Scheduling.DueDate',''),
                    'StackRank':         fields.get('Microsoft.VSTS.Common.StackRank',''),
                    'Priority':          fields.get('Microsoft.VSTS.Common.Priority',''),
                    'LastComment':       get_last_comment(organization, project, cid)
                }
                all_issues.append(issue)

    if not all_issues:
        print("Nenhuma issue encontrada.")
        return

    df = pd.DataFrame(all_issues)
    # converte datas
    for col in ['CreatedDate','DueDate']:
        if col in df:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    # exporta
    csv_file  = 'issues_base.csv'
    excel_file= 'issues_base.xlsx'
    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    df.to_excel(excel_file, index=False)
    print(f"\n✅ CSV:  {csv_file}")
    print(f"✅ XLSX: {excel_file}")
        # ─── Exibe tempo total ───
    elapsed = time_module.time() - start_time
    print(f"⏱️ Tempo total de execução: {round(elapsed, 2)}s")

if __name__ == "__main__":
    main()
