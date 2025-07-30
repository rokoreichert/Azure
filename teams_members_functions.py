#!/usr/bin/env python3
import os, base64, logging, requests, pandas as pd, time

# ─────────────────────────── CONFIGURAÇÃO ───────────────────────────
# ORG via variável de ambiente (default: "arezzosa")
ORG = os.getenv("ORG", "arezzosa").strip()
# PROJECT pode ser múltiplos, separados por vírgula; defaults configurados
projects_env = os.getenv("PROJECT", "FUTURO_E-COMMERCE,ZZAPPS,ARZZ").strip()
PROJECTS = [p.strip() for p in projects_env.split(",") if p.strip()]
# PAT via variável de ambiente ou fallback
PAT = os.getenv("AZURE_PAT") or "SEU_PERSONAL_ACCESS_TOKEN"

# versões das APIs que vamos usar
CORE_API   = "7.1-preview.3"   # para projetos e squads
TEAMS_API  = "7.1-preview.1"   # para pegar AreaPath de time
GRAPH_API  = "6.0-preview.1"   # Graph API para usuários/grupos
# nomes dos grupos que queremos usar como "Função"
FUNCS = ["Backend","Frontend","PO","QA","UX"]

# ─────────────────────────── LOG ───────────────────────────
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger()

# ─────────────────── AUTENTICAÇÃO BASIC ───────────────────────
if not PAT:
    raise RuntimeError("Defina AZURE_PAT ou insira seu PAT na variável PAT")
token = base64.b64encode(f":{PAT}".encode()).decode()
HEADERS = {"Authorization": f"Basic {token}", "Content-Type": "application/json"}

# ─────────────────── MAIN ───────────────────────
def main():
    all_rows = []
    start = time.time()

    for PROJECT in PROJECTS:
        logger.info(f"=== Iniciando export para projeto: {PROJECT} ===")
        # 1) Lista squads
        url_teams = f"https://dev.azure.com/{ORG}/_apis/projects/{PROJECT}/teams"
        resp = requests.get(url_teams, headers=HEADERS, params={"api-version": CORE_API})
        resp.raise_for_status()
        squad_names = [t["name"] for t in resp.json().get("value", [])]
        logger.info(f"Encontradas {len(squad_names)} squads no projeto {PROJECT}")

        # 1b) filtro AZURE_SQUADS
        squads_env = os.getenv("AZURE_SQUADS", "").strip()
        if squads_env:
            desired = [s.strip() for s in squads_env.split(",") if s.strip()]
            logger.info(f"📌 Filtrando squads em AZURE_SQUADS: {desired}")
            filtered = [s for s in squad_names if s in desired]
            missing = set(desired) - set(filtered)
            if missing:
                logger.warning(f"⚠️ Squads não encontrados: {missing}")
            squad_names = filtered
        else:
            logger.info("ℹ️ Nenhum filtro AZURE_SQUADS definido; processando todas squads.")

        # 2) Coleta AreaPath para cada squad
        squad_info = {}
        for name in squad_names:
            url_area = (f"https://dev.azure.com/{ORG}/{PROJECT}/{name}"
                        f"/_apis/work/teamsettings/teamfieldvalues")
            r = requests.get(url_area, headers=HEADERS, params={"api-version": TEAMS_API})
            if r.status_code != 200:
                logger.warning(f"Falha ao obter AreaPath de {name}: {r.status_code}")
                ap = f"{PROJECT}\\{name}"
            else:
                vals = r.json().get("values", [])
                ap = vals[0].get("value") if vals else f"{PROJECT}\\{name}"
            squad_info[name] = {"areaPath": ap}
        logger.info("AreaPath de todas as squads carregados")

        # 3) project_id + scopeDescriptor
        proj = requests.get(f"https://dev.azure.com/{ORG}/_apis/projects/{PROJECT}",
                            headers=HEADERS, params={"api-version": CORE_API})
        proj.raise_for_status()
        project_id = proj.json()["id"]
        logger.info(f"Project ID: {project_id}")
        graph_base = f"https://vssps.dev.azure.com/{ORG}"
        desc = requests.get(f"{graph_base}/_apis/graph/descriptors/{project_id}",
                            headers=HEADERS, params={"api-version": GRAPH_API})
        desc.raise_for_status()
        scope_desc = desc.json()["value"]
        logger.info(f"ScopeDescriptor: {scope_desc}")

        # 4) Recupera Graph-groups e mapeia descriptors
        grps = requests.get(f"{graph_base}/_apis/graph/groups",
                            headers=HEADERS,
                            params={"scopeDescriptor": scope_desc, "api-version": GRAPH_API})
        grps.raise_for_status()
        all_groups = grps.json().get("value", [])
        group_map = {g["displayName"]: g["descriptor"] for g in all_groups}
        logger.info(f"Recuperados {len(group_map)} Graph-groups")

        # 5) Mapeia usuários→funções
        func_map = {}
        for func in FUNCS:
            desc_f = group_map.get(func)
            if not desc_f:
                logger.warning(f"Grupo função '{func}' não encontrado")
                continue
            mems = requests.get(f"{graph_base}/_apis/graph/memberships/{desc_f}",
                                headers=HEADERS,
                                params={"direction":"down","api-version": GRAPH_API})
            for m in mems.json().get("value", []):
                md = m.get("memberDescriptor")
                ux = requests.get(f"{graph_base}/_apis/graph/users/{md}",
                                  headers=HEADERS, params={"api-version": GRAPH_API})
                if ux.status_code==200:
                    uname = ux.json().get("principalName")
                    func_map[uname] = func
        logger.info(f"Mapeadas {len(func_map)} usuários→função")

        # 6) Lista membros de cada squad
        for team_name, info in squad_info.items():
            desc_t = group_map.get(team_name)
            if not desc_t:
                logger.warning(f"Squad '{team_name}' não existe em Graph-groups, pulando")
                continue
            ms = requests.get(f"{graph_base}/_apis/graph/memberships/{desc_t}",
                              headers=HEADERS,
                              params={"direction":"down","api-version": GRAPH_API})
            if ms.status_code!=200:
                logger.warning(f"Falha memberships '{team_name}': {ms.status_code}")
                continue
            members = ms.json().get("value", [])
            logger.info(f"Squad '{team_name}': {len(members)} membros")
            for m in members:
                md = m.get("memberDescriptor")
                ux = requests.get(f"{graph_base}/_apis/graph/users/{md}",
                                  headers=HEADERS, params={"api-version": GRAPH_API})
                if ux.status_code!=200:
                    continue
                u = ux.json()
                uname = u.get("principalName","")
                all_rows.append({
                    "Project":      PROJECT,
                    "Team":         team_name,
                    "AreaPath":     info.get("areaPath",""),
                    "DisplayName":  u.get("displayName",""),
                    "MailAddress":  u.get("mailAddress",""),
                    "Função":       func_map.get(uname,"")
                })

    # 7) Exporta resultados
    if all_rows:
        df = pd.DataFrame(all_rows)
        df.to_csv("teams_members_functions.csv", index=False, encoding="utf-8-sig")
        df.to_excel("teams_members_functions.xlsx", index=False, engine="openpyxl")
        elapsed = int(time.time() - start)
        logger.info(f"✔ Export concluído: {len(df)} linhas em {elapsed}s")
    else:
        logger.warning("Nenhum registro para exportar.")

if __name__ == "__main__":
    main()
