#!/usr/bin/env python3
import os
import base64
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
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

start_time = time.time()

# --- CONFIGURAÇÃO ---
token         = os.getenv("AZURE_PAT", "").strip()
organization  = os.getenv("ORG", "arezzosa").strip()
projects_env  = os.getenv("PROJECT", "FUTURO_E-COMMERCE,ZZAPPS,ARZZ").strip()
projects      = [p.strip() for p in projects_env.split(',') if p.strip()]

if not token:
    print("❌ Erro: defina a variável AZURE_PAT no ambiente.")
    exit(1)
if not projects:
    print("❌ Erro: defina ao menos um projeto em PROJECT no ambiente.")
    exit(1)

b64     = base64.b64encode(f":{token}".encode()).decode()
headers = {"Authorization": f"Basic {b64}"}

# --- AUXILIARES EXISTENTES ---
def run_wiql(query: str, project: str) -> list[int]:
    url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/wiql?api-version=7.0"
    r = requests.post(url, headers=headers, json={"query": query})
    r.raise_for_status()
    return [w["id"] for w in r.json().get("workItems", [])]

def fetch_fields(wid: int, project: str) -> dict:
    url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitems/{wid}?api-version=7.0"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json().get("fields", {})

def fetch_last_comment(wid: int, project: str) -> str:
    # incluímos $top=1 e orderBy=createdDate desc antes da versão
    url = (
        f"https://dev.azure.com/{organization}/{project}"
        f"/_apis/wit/workItems/{wid}/comments"
        "?$top=1&orderBy=createdDate desc&api-version=6.0-preview.3"
    )
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        return ""
    comments = r.json().get("comments", [])
    if not comments:
        return ""
    # aqui pegamos comments[0], que é o mais recente
    return BeautifulSoup(comments[0].get("text",""), "html.parser") \
               .get_text(separator=" ", strip=True)

def fetch_odata(wit: str, project: str) -> pd.DataFrame:
    url = (
        f"https://analytics.dev.azure.com/{organization}/{project}"
        f"/_odata/v3.0-preview/WorkItems?$filter=WorkItemType eq '{wit}'"
    )
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return pd.json_normalize(r.json().get("value", []))

def count_children(ids, project: str):
    tot = ok = 0
    for fid in ids:
        rels = requests.get(
            f"https://dev.azure.com/{organization}/{project}"
            f"/_apis/wit/workitems/{fid}?$expand=relations&api-version=7.0",
            headers=headers
        ).json().get("relations", [])
        for r in rels:
            if r.get("rel") == "System.LinkTypes.Hierarchy-Forward":
                cid = int(r["url"].rsplit('/', 1)[-1])
                st = fetch_fields(cid, project).get("System.State", "")
                if st != "Removed":
                    tot += 1
                    if st in ["Closed", "Resolved"]:
                        ok += 1
    return tot, ok

# --- EXECUÇÃO PRINCIPAL ---
all_dfs = []
cols = [
    "Project","Area Path","Team","Work Item ID","Work Item Type","Title","State","AssignedTo",
    "Created Date","Start Date","Target Date","ResolvedIn","Closed Date",
    "Description","Last Comment","FeatureCount","FeatureOK","%FeaturesEntregues",
    "USCount","USOK","%USEntregues"
]

for project in projects:
    print(f"\n🔍 Iniciando projeto: {project}")

    # ─── Carrega squads e suas area paths
    teams = list_teams(organization, project, headers)
    squads_env = os.getenv("AZURE_SQUADS", "").strip()
    if squads_env:
        desired = [s.strip() for s in squads_env.split(",") if s.strip()]
        teams = [t for t in teams if t.get("name") in desired]
    team_area_map = {}
    for t in teams:
        tn    = t.get("name")
        areas = get_team_areas(organization, project, tn, headers)
        team_area_map[tn] = areas

    # Monta prefs_map e all_teams para fallback
    prefs_map = {}
    for tn, prefs in team_area_map.items():
        for pref in prefs:
            prefs_map.setdefault(pref, []).append(tn)
    all_teams = set(team_area_map.keys())

    # 1) ÉPICOS com tag STATUS
    wiql_epics = (
        "Select [System.Id] From WorkItems "
        "Where [System.WorkItemType] = 'Epic' "
        "And [System.Tags] Contains 'STATUS' "
        f"And [System.TeamProject] = '{project}'"
    )
    epic_ids = run_wiql(wiql_epics, project)
    print(f"▶️ Total épicos com STATUS: {len(epic_ids)}")

    # 2) Processar épicos
    epic_rows = []
    for idx, wid in enumerate(epic_ids, 1):
        f = fetch_fields(wid, project)
        area_path      = f.get("System.AreaPath", "")
        iteration_path = f.get("System.IterationPath", "")

        # fallback em 4 níveis para Team
        parts = area_path.split('\\')
        team_name = ''
        if area_path in prefs_map and len(prefs_map[area_path]) == 1:
            team_name = prefs_map[area_path][0]
        elif len(parts) >= 3 and parts[2] in all_teams:
            team_name = parts[2]
        elif len(parts) >= 2 and parts[1] in all_teams:
            team_name = parts[1]
        elif len(parts) >= 1 and parts[0] in all_teams:
            team_name = parts[0]
        else:
            team_name = project

        atv = f.get("System.AssignedTo", {})
        at  = atv.get("displayName") if isinstance(atv, dict) else atv
        epic_rows.append({
            **{c: '' for c in cols},
            "Project":        project,
            "Area Path":      area_path,
            "Team":           team_name,
            "Work Item ID":   wid,
            "Work Item Type": f.get("System.WorkItemType", ""),
            "Title":          f.get("System.Title", ""),
            "State":          f.get("System.State", ""),
            "AssignedTo":     at,
            "Created Date":   f.get("System.CreatedDate", ""),
            "Start Date":     f.get("Microsoft.VSTS.Scheduling.StartDate", ""),
            "Target Date":    f.get("Microsoft.VSTS.Scheduling.TargetDate", ""),
            "ResolvedIn":     f.get("Custom.ResolvedIn", ""),
            "Closed Date":    f.get("Microsoft.VSTS.Common.ClosedDate", ""),
            "Description":    BeautifulSoup(f.get("System.Description", ""), "html.parser")
                                        .get_text(separator=" ", strip=True),
            "Last Comment":   fetch_last_comment(wid, project)
        })
        print(f"    • [{idx}/{len(epic_ids)}] Epic {wid} processado")
    df_epics = pd.DataFrame(epic_rows, columns=cols)

    # 3) Métricas de Features por épico
    features_df = fetch_odata("Feature", project)
    valid = features_df[
        (features_df["ParentWorkItemId"].isin(epic_ids)) &
        (features_df["State"] != "Removed")
    ]
    print(f"▶️ Total features relacionadas: {len(valid)}")
    df_epics["FeatureCount"] = df_epics["Work Item ID"] \
        .map(valid.groupby("ParentWorkItemId").size()).fillna(0).astype(int)
    df_epics["FeatureOK"] = df_epics["Work Item ID"] \
        .map(valid[valid["State"].isin(["Closed","Resolved"])].groupby("ParentWorkItemId").size()) \
        .fillna(0).astype(int)
    df_epics["%FeaturesEntregues"] = (
        df_epics["FeatureOK"]/df_epics["FeatureCount"]
    ).replace({pd.NA:0, float('inf'):0}).fillna(0).round(2)

    # 4) Contagem de US por épico
    us_counts, us_ok = {}, {}
    for idx, e in enumerate(epic_ids, 1):
        feats = valid[valid["ParentWorkItemId"] == e]["WorkItemId"]
        t, o = count_children(feats.tolist(), project)
        us_counts[e], us_ok[e] = t, o
        print(f"    • [{idx}/{len(epic_ids)}] Epic {e} -> US total={t}, ok={o}")
    df_epics["USCount"]      = df_epics["Work Item ID"].map(us_counts).fillna(0).astype(int)
    df_epics["USOK"]         = df_epics["Work Item ID"].map(us_ok).fillna(0).astype(int)
    df_epics["%USEntregues"] = (
        df_epics["USOK"]/df_epics["USCount"]
    ).replace({pd.NA:0, float('inf'):0}).fillna(0).round(2)

    # 5) Features independentes com STATUS
    wiql_feats = (
        "Select [System.Id] From WorkItems "
        "Where [System.WorkItemType] = 'Feature' "
        "And [System.Tags] Contains 'STATUS' "
        f"And [System.TeamProject] = '{project}'"
    )
    feat_ids = run_wiql(wiql_feats, project)
    print(f"▶️ Features independentes com STATUS: {len(feat_ids)}")
    feat_rows = []
    for idx, fid in enumerate(feat_ids, 1):
        f = fetch_fields(fid, project)
        area_path      = f.get("System.AreaPath", "")
        iteration_path = f.get("System.IterationPath", "")

        # fallback em 4 níveis para Team
        parts = area_path.split('\\')
        team_name = ''
        if area_path in prefs_map and len(prefs_map[area_path]) == 1:
            team_name = prefs_map[area_path][0]
        elif len(parts) >= 3 and parts[2] in all_teams:
            team_name = parts[2]
        elif len(parts) >= 2 and parts[1] in all_teams:
            team_name = parts[1]
        elif len(parts) >= 1 and parts[0] in all_teams:
            team_name = parts[0]
        else:
            team_name = project

        atv = f.get("System.AssignedTo", {})
        at  = atv.get("displayName") if isinstance(atv, dict) else atv
        t, o = count_children([fid], project)
        feat_rows.append({
            **{c: '' for c in cols},
            "Project":        project,
            "Area Path":      area_path,
            "Team":           team_name,
            "Work Item ID":   fid,
            "Work Item Type": f.get("System.WorkItemType", ""),
            "Title":          f.get("System.Title", ""),
            "State":          f.get("System.State", ""),
            "AssignedTo":     at,
            "Created Date":   f.get("System.CreatedDate", ""),
            "Start Date":     f.get("Microsoft.VSTS.Scheduling.StartDate", ""),
            "Target Date":    f.get("Microsoft.VSTS.Scheduling.TargetDate", ""),
            "ResolvedIn":     f.get("Custom.ResolvedIn", ""),
            "Closed Date":    f.get("Microsoft.VSTS.Common.ClosedDate", ""),
            "Description":    BeautifulSoup(f.get("System.Description", ""), "html.parser")
                                            .get_text(separator=" ", strip=True),
            "Last Comment":   fetch_last_comment(fid, project),
            "FeatureCount":   0,
            "FeatureOK":      0,
            "%FeaturesEntregues": 0,
            "USCount":        t,
            "USOK":           o,
            "%USEntregues":   round((o/t) if t else 0, 2)
        })
        print(f"    • [{idx}/{len(feat_ids)}] Feature {fid} independente -> US={o}")
    df_feats = pd.DataFrame(feat_rows, columns=cols)

    # 6) Merge final e export
    df_proj = pd.concat([df_epics, df_feats], ignore_index=True)[cols]
    all_dfs.append(df_proj)

# Concat todos projetos
final_df = pd.concat(all_dfs, ignore_index=True)

# Ajuste de horários e formatação BR
import numpy as np

def adjust(val, sub_h=False, eod=False):
    dt = pd.to_datetime(val, errors='coerce')
    if pd.isna(dt): return ''
    if sub_h: dt -= timedelta(hours=3)
    if eod:   dt = dt.normalize() + timedelta(hours=23, minutes=59, seconds=59)
    return dt.strftime('%d/%m/%Y %H:%M:%S')

for c in ['Created Date','Closed Date','ResolvedIn']:
    final_df[c] = final_df[c].apply(lambda v: adjust(v, sub_h=True))
for c in ['Start Date','Target Date']:
    final_df[c] = final_df[c].apply(lambda v: adjust(v, eod=True))

# Export arquivos
csv_file  = "status_report_all.csv"
final_df.to_csv(csv_file, index=False, encoding="utf-8-sig")
print(f"✅ CSV gerado: {csv_file}")
try:
    import openpyxl
    final_df.to_excel("status_report_all.xlsx", index=False)
    print("✅ XLSX gerado: status_report_all.xlsx")
except ImportError:
    print("⚠️ openpyxl não instalado, pulando XLSX")

print(f"⏱️ Tempo total: {time.time()-start_time:.2f}s")
