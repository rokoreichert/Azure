#!/usr/bin/env python3
import os
import requests
import base64
import time
import pandas as pd
from requests.auth import HTTPBasicAuth

start_time = time.time()

# ─────────────────────────── CONFIGURAÇÕES ───────────────────────────
# ORG via variável de ambiente, default original
ORG = os.getenv("ORG", "arezzosa").strip()
# PROJECT pode ser uma lista separada por vírgula, com defaults
projects_env = os.getenv("PROJECT", "FUTURO_E-COMMERCE,ZZAPPS,ARZZ").strip()
PROJECTS = [p.strip() for p in projects_env.split(",") if p.strip()]
API_VERSION = "7.0"

# PAT via variável de ambiente
PAT = os.getenv("AZURE_PAT", "").strip()
if not PAT:
    raise EnvironmentError(
        "Defina a variável de ambiente AZURE_PAT:\n"
        "  export AZURE_PAT='seu_personal_access_token'"
    )

# ────────────────────────── AUTENTICAÇÃO ──────────────────────────
token = base64.b64encode(f":{PAT}".encode("utf-8")).decode("utf-8")
headers = {
    "Content-Type":  "application/json",
    "Authorization": f"Basic {token}"
}
auth = HTTPBasicAuth("", PAT)


def listar_teams(org, project):
    """Retorna lista de equipes do projeto, excluindo 'Architecture'."""
    url = f"https://dev.azure.com/{org}/_apis/projects/{project}/teams?api-version={API_VERSION}"
    resp = requests.get(url, auth=auth, headers=headers)
    resp.raise_for_status()
    teams = [t["name"] for t in resp.json().get("value", [])]
    return [t for t in teams if t.lower() != "architecture"]


def get_area_paths(org, project, team):
    """Recupera os Area Paths de uma equipe."""
    url = (
        f"https://dev.azure.com/{org}/{project}/{team}"
        f"/_apis/work/teamsettings/teamfieldvalues?api-version={API_VERSION}"
    )
    resp = requests.get(url, auth=auth, headers=headers)
    if resp.status_code != 200:
        return []
    return [v.get("value", "") for v in resp.json().get("values", [])]


def get_board_columns(org, project, team, board_name):
    """Retorna colunas e metadados de um board."""
    url_boards = (
        f"https://dev.azure.com/{org}/{project}/{team}"
        f"/_apis/work/boards?api-version={API_VERSION}"
    )
    resp = requests.get(url_boards, auth=auth, headers=headers)
    resp.raise_for_status()
    boards = resp.json().get("value", [])

    for b in boards:
        if b["name"].lower() == board_name.lower():
            url_cols = (
                f"https://dev.azure.com/{org}/{project}/{team}"
                f"/_apis/work/boards/{b['id']}/columns?api-version={API_VERSION}"
            )
            r2 = requests.get(url_cols, auth=auth, headers=headers)
            r2.raise_for_status()
            cols = []
            for c in r2.json().get("value", []):
                states = c.get("stateMappings", {})
                unique_states = ", ".join(sorted(set(states.values())))
                valid_for = ", ".join(sorted(states.keys()))
                if c.get("isSplit", False):
                    cols.append((c["name"], "Doing", unique_states, valid_for))
                    cols.append((c["name"], "Done", unique_states, valid_for))
                else:
                    cols.append((c["name"], "", unique_states, valid_for))
            return cols
    return []


def main():
    all_data = []
    for project in PROJECTS:
        print(f"➡️ Processando project: {project}")
        squads = listar_teams(ORG, project)
        print(f"✔️  {len(squads)} equipes encontradas em {project}.")

        squads_env = os.getenv("AZURE_SQUADS", "").split(",")
        squads_env = [s.strip() for s in squads_env if s.strip()]
        if squads_env:
            print(f"📌 Filtrando squads definidos em AZURE_SQUADS: {squads_env}")
            squads = [s for s in squads if s in squads_env]
            missing = set(squads_env) - set(squads)
            if missing:
                print(f"⚠️ Squads não encontrados e serão ignorados: {missing}")
        else:
            print("ℹ️ Nenhum filtro AZURE_SQUADS definido; processando todas as squads")

        for team in squads:
            print(f"→ Squad: {team}")
            area_paths = get_area_paths(ORG, project, team)
            area_str = ", ".join(area_paths) if area_paths else ""
            print(f"   • Area Paths: {area_str or '(padrão)'}")

            for board in ("Epics", "Features", "Stories"):
                cols = get_board_columns(ORG, project, team, board)
                print(f"   • {board}: {len(cols)} colunas")
                if cols:
                    for name, split, states, valid in cols:
                        all_data.append({
                            "Project":       project,
                            "Team":          team,
                            "AreaPath":      area_str,
                            "Board":         board,
                            "Column":        name,
                            "SplitState":    split,
                            "StateMappings": states,
                            "IsValidFor":    valid
                        })
                else:
                    all_data.append({
                        "Project":       project,
                        "Team":          team,
                        "AreaPath":      area_str,
                        "Board":         board,
                        "Column":        "(nenhuma coluna)",
                        "SplitState":    "",
                        "StateMappings":"",
                        "IsValidFor":    ""
                    })

    df = pd.DataFrame(all_data)
    df.to_csv("fluxo_atual.csv", index=False, encoding="utf-8-sig")
    df.to_excel("fluxo_atual.xlsx", index=False)

    print(f"✅ Export concluído em {int(time.time() - start_time)}s — arquivos: fluxo_atual.csv / fluxo_atual.xlsx")

if __name__ == "__main__":
    main()
