#!/usr/bin/env python3
import os
import base64
import requests
import pandas as pd
import time

start_time = time.time()

# ─────────────────────────── CONFIGURAÇÕES ───────────────────────────
# ORG via variável de ambiente, default original
ORG = os.getenv("ORG", "arezzosa").strip()
# PROJECT pode ser múltiplos, separados por vírgula
projects_env = os.getenv("PROJECT", "FUTURO_E-COMMERCE,ZZAPPS,ARZZ").strip()
PROJECTS = [p.strip() for p in projects_env.split(",") if p.strip()]

# PAT via variável de ambiente
PAT = os.getenv("AZURE_PAT", "").strip()
if not PAT:
    raise EnvironmentError(
        "Defina a variável de ambiente AZURE_PAT:\n"
        "  export AZURE_PAT='seu_personal_access_token'"
    )

# Cabeçalho Basic Auth
token = base64.b64encode(f":{PAT}".encode("utf-8")).decode("utf-8")
HEADERS = {
    "Authorization": f"Basic {token}",
    "Content-Type":  "application/json"
}

# Versão da API usada
API_VERSION_TEAMS = "7.1-preview.3"
API_VERSION_AREAS = "7.1-preview.1"

# ─────────────────── FUNÇÕES DE CHAMADA À API ───────────────────
def list_teams(org: str, project: str) -> list[dict]:
    url    = f"https://dev.azure.com/{org}/_apis/projects/{project}/teams"
    params = {"api-version": API_VERSION_TEAMS}
    resp   = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("value", [])


def get_team_areas(org: str, project: str, team_name: str) -> list[dict]:
    url    = (
        f"https://dev.azure.com/{org}/{project}/{team_name}"
        f"/_apis/work/teamsettings/teamfieldvalues"
    )
    params = {"api-version": API_VERSION_AREAS}
    resp   = requests.get(url, headers=HEADERS, params=params)
    resp.raise_for_status()
    return resp.json().get("values", [])


def main():
    all_rows = []

    # Itera sobre cada projeto
    for project in PROJECTS:
        print(f"➡️ Processando project: {project}")
        teams = list_teams(ORG, project)
        print(f"✔️  Encontrados {len(teams)} teams no projeto {project}")

        # Filtra por AZURE_SQUADS, se definido
        squads_env = os.getenv("AZURE_SQUADS", "").strip()
        if squads_env:
            desired = [s.strip() for s in squads_env.split(",") if s.strip()]
            print(f"📌 Filtrando teams definidos em AZURE_SQUADS: {desired}")
            teams = [t for t in teams if t.get("name") in desired]
            missing = set(desired) - {t.get("name") for t in teams}
            if missing:
                print(f"⚠️ Teams não encontrados e serão ignorados: {missing}")
        else:
            print("ℹ️ Nenhum filtro AZURE_SQUADS definido; processando todos os teams")

        # Coleta áreas de cada team
        for t in teams:
            name = t.get("name")
            areas = get_team_areas(ORG, project, name)
            if not areas:
                # Se não houver, adiciona linha vazia
                all_rows.append({"Project": project, "TeamName": name, "AreaPath": ""})
            else:
                for a in areas:
                    all_rows.append({
                        "Project":  project,
                        "TeamName": name,
                        "AreaPath": a.get("value", "")
                    })

    # Gera DataFrame e salva arquivos
    df = pd.DataFrame(all_rows)
    csv_file  = "teams_area_paths.csv"
    xlsx_file = "teams_area_paths.xlsx"
    df.to_csv(csv_file, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_file, index=False)

    elapsed = int(time.time() - start_time)
    print(f"✅ Export concluído em {elapsed}s — arquivos gerados: {csv_file}, {xlsx_file}")

if __name__ == "__main__":
    main()
