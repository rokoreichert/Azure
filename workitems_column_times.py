#!/usr/bin/env python3
import os
import requests
import base64
from datetime import datetime, timedelta, timezone
import time
import pandas as pd
from requests.auth import HTTPBasicAuth

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

# ─────────────────────────── CONFIGURAÇÕES ───────────────────────────
ORG = os.getenv("ORG", "arezzosa").strip()
projects_env = os.getenv("PROJECT", "FUTURO_E-COMMERCE,ZZAPPS,ARZZ").strip()
PROJECTS = [p.strip() for p in projects_env.split(",") if p.strip()]

pat = os.getenv("AZURE_PAT")
if not pat:
    raise ValueError("❌ A variável de ambiente AZURE_PAT não está definida")

days_window = int(os.getenv("AZURE_DAYS_WINDOW", "30"))

token = base64.b64encode(f":{pat}".encode("ascii")).decode("ascii")
headers = {
    "Content-Type":  "application/json",
    "Authorization": f"Basic {token}"
}
auth = HTTPBasicAuth("", pat)

allowed_types = ["Feature", "User Story", "Spike", "Bug", "Fix", "Vulnerability"]

all_data_rows = []
start_time = time.time()

for project in PROJECTS:
    # Define intervalo de datas
    now   = datetime.now(timezone.utc)
    start = (now - timedelta(days=days_window)).date()
    end   = now.date()
    print(f"\n🔍 Projeto: {project}")
    print(f"Buscando itens fechados de {start} até {end} (últimos {days_window} dias)...")

    # ─── Carrega squads e suas AreaPaths para este projeto ───
    teams = list_teams(ORG, project, headers)
    squads_env = os.getenv("AZURE_SQUADS", "").strip()
    if squads_env:
        desired = [s.strip() for s in squads_env.split(",") if s.strip()]
        teams = [t for t in teams if t.get("name") in desired]
    team_area_map = {}
    for t in teams:
        tn    = t.get("name")
        areas = get_team_areas(ORG, project, tn, headers)
        team_area_map[tn] = areas

    # ─── Prepara prefs_map e all_teams ───
    prefs_map = {}
    all_teams = []
    for tn, prefs in team_area_map.items():
        all_teams.append(tn)
        for p in prefs:
            prefs_map.setdefault(p, []).append(tn)


    # 1) Construir split_columns para este project
    split_columns = {}
    teams_url = f"https://dev.azure.com/{ORG}/_apis/projects/{project}/teams?api-version=7.0"
    for team in [t["name"] for t in requests.get(teams_url, auth=auth).json().get("value", [])]:
        boards_url = f"https://dev.azure.com/{ORG}/{project}/{team}/_apis/work/boards?api-version=7.0"
        for b in requests.get(boards_url, auth=auth).json().get("value", []):
            if b["name"].lower() in ("epics", "features", "stories"):
                cols_url = f"https://dev.azure.com/{ORG}/{project}/{team}/_apis/work/boards/{b['id']}/columns?api-version=7.0"
                for c in requests.get(cols_url, auth=auth).json().get("value", []):
                    key = (b["name"].lower().strip(), c["name"].lower().strip(), team.lower().strip())
                    split_columns[key] = c.get("isSplit", False)
                break

    # 2) Buscar IDs de WorkItems fechados via WIQL
    wiql_url = f"https://dev.azure.com/{ORG}/{project}/_apis/wit/wiql?api-version=7.0"
    wiql_query = f"""
SELECT [System.Id]
FROM WorkItems
WHERE [System.TeamProject] = '{project}'
  AND [System.State]       = 'Closed'
  AND [Microsoft.VSTS.Common.ClosedDate] >= '{start}'
  AND [Microsoft.VSTS.Common.ClosedDate] <= '{end}'
ORDER BY [System.ChangedDate] DESC
"""
    resp = requests.post(wiql_url, headers=headers, json={"query": wiql_query})
    resp.raise_for_status()
    ids = [str(w["id"]) for w in resp.json().get("workItems", [])]
    print(f"✅ {len(ids)} itens fechados encontrados em {project}.")

    # 3) Processar revisões
    data_rows = []
    skipped   = 0
    for idx, wid in enumerate(ids, 1):
        revs_resp = requests.get(
            f"https://dev.azure.com/{ORG}/{project}/_apis/wit/workItems/{wid}/revisions?api-version=7.0",
            headers=headers
        )
        if revs_resp.status_code != 200:
            skipped += 1
            continue

        revs = sorted(
            revs_resp.json().get("value", []),
            key=lambda x: x["fields"].get("System.ChangedDate", "")
        )
        if not revs:
            continue

        last       = revs[-1]["fields"]
        wi_type    = last.get("System.WorkItemType", "")
        area       = last.get("System.AreaPath", "")
        iteration  = last.get("System.IterationPath", "")
        title      = last.get("System.Title", "")
        closed_raw = last.get("Microsoft.VSTS.Common.ClosedDate", "")

        if wi_type not in allowed_types:
            continue

        # extrai paths
        area      = last.get("System.AreaPath", "")

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


        closed_str = ""
        if closed_raw:
            closed_dt  = datetime.strptime(closed_raw[:19], "%Y-%m-%dT%H:%M:%S") - timedelta(hours=3)
            closed_str = closed_dt.strftime("%Y-%m-%d %H:%M:%S")

        curr_col, entry_dt, entry_done = None, None, None
        for rev in revs:
            f         = rev["fields"]
            col       = f.get("System.BoardColumn", "")
            done_flag = f.get("System.BoardColumnDone", False)
            cd        = f.get("System.ChangedDate", "")
            if not col or not cd:
                continue
            dt = datetime.strptime(cd[:19], "%Y-%m-%dT%H:%M:%S") - timedelta(hours=3)

            if curr_col is None:
                curr_col, entry_dt, entry_done = col, dt, done_flag
                continue

            if col != curr_col or done_flag != entry_done:
                area_key    = area.split("\\")[-1].strip().lower()
                key         = (col.lower(), curr_col.lower(), area_key)
                split_state = "Done" if split_columns.get(key, False) and entry_done else ("Doing" if split_columns.get(key, False) else "")
                dias        = (dt - entry_dt).total_seconds() / 86400

                data_rows.append({
                    "Project":      project,
                    "WorkItemID":   wid,
                    "Team":         team_name,
                    "Title":        title,
                    "WorkItemType": wi_type,
                    "Board":        "Epics" if wi_type=="Feature" and "Epic" in title else ("Features" if wi_type=="Feature" else "Stories"),
                    "AreaPath":     area,
                    "Column":       curr_col,
                    "SplitState":   split_state,
                    "Start":        entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "End":          dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "Days":         round(dias, 2),
                    "ClosedDate":   closed_str
                })
                curr_col, entry_dt, entry_done = col, dt, done_flag

        # Último segmento até fechamento
        if curr_col and entry_dt and closed_str:
            dt_end      = datetime.strptime(closed_raw[:19], "%Y-%m-%dT%H:%M:%S") - timedelta(hours=3)
            dias_f      = (dt_end - entry_dt).total_seconds() / 86400
            area_key    = area.split("\\")[-1].strip().lower()
            key         = (curr_col.lower(), curr_col.lower(), area_key)
            split_state = "Done" if split_columns.get(key, False) and entry_done else ("Doing" if split_columns.get(key, False) else "")

            data_rows.append({
                "Project":      project,
                "WorkItemID":   wid,
                "Team":         team_name,
                "Title":        title,
                "WorkItemType": wi_type,
                "Board":        "Epics" if wi_type=="Feature" and "Epic" in title else ("Features" if wi_type=="Feature" else "Stories"),
                "AreaPath":     area,
                "Column":       curr_col,
                "SplitState":   split_state,
                "Start":        entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "End":          dt_end.strftime("%Y-%m-%d %H:%M:%S"),
                "Days":         round(dias_f, 2),
                "ClosedDate":   closed_str
            })

        if idx % 50 == 0:
            print(f"🔄 {idx}/{len(ids)} processados...")

    print(f"✅ Processados em {project}: {len(data_rows)} blocos, {skipped} itens pulados.")
    all_data_rows.extend(data_rows)

# 4) Exportação
if all_data_rows:
    df = pd.DataFrame(all_data_rows)
    csv_filename  = f"workitems_column_times.csv"
    xlsx_filename = f"workitems_column_times.xlsx"
    df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_filename, index=False)
    print(f"✅ Arquivos salvos: {csv_filename} e {xlsx_filename}")
else:
    print("⚠️ Nenhum dado para exportar.")

minutes, seconds = divmod(time.time() - start_time, 60)
print(f"⏱️ Execução total: {int(minutes)}m {int(seconds)}s")
