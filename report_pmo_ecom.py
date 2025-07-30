#!/usr/bin/env python3
import os
import sys
import time as time_module
import base64
import requests
import pandas as pd
from datetime import datetime, time
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
        f"/_apis/work/teamsettings/teamfieldvalues"
    )
    params = {"api-version": API_VERSION_AREAS}
    resp   = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    # retorna só os valores dos area paths
    return [v.get("value","") for v in resp.json().get("values", [])]

def process_project(organization: str, project: str, headers: dict) -> pd.DataFrame:
    hoje = datetime.now()

    # ─── 0) Carrega Teams e todas as suas Area Paths ───
    teams = list_teams(organization, project, headers)
    # filtra se a variável estiver definida
    squads_env = os.environ.get("AZURE_SQUADS", "").strip()
    if squads_env:
        desired = [s.strip() for s in squads_env.split(",") if s.strip()]
        teams = [t for t in teams if t.get("name") in desired]

    team_area_map = {}
    for t in teams:
        tn = t.get("name")
        areas = get_team_areas(organization, project, tn, headers)
        team_area_map[tn] = areas

    # ─── AQUI ───
    # Mapa rápido de prefixos → times, e lista de todos os nomes de time
    prefs_map = {}
    all_teams = []
    for tn, prefs in team_area_map.items():
        all_teams.append(tn)
        for p in prefs:
            prefs_map.setdefault(p, []).append(tn)



    # ─── 1) Buscar épicos com tag 'PROJETOS' ───
    print(f"\n▶ [{project}] Buscando épicos...")
    epics_url = (
        f"https://analytics.dev.azure.com/{organization}/{project}"
        "/_odata/v3.0-preview/WorkItems"
        "?$filter=WorkItemType eq 'Epic'"
    )
    resp = requests.get(epics_url, headers=headers); resp.raise_for_status()
    epics_data = resp.json().get('value', [])
    epics_df   = pd.json_normalize(epics_data)
    epics_df   = epics_df[epics_df['TagNames'].str.contains('PROJETOS', na=False)]
    epic_ids   = epics_df['WorkItemId'].tolist()
    print(f"   → {len(epic_ids)} épicos encontrados.")
    if not epic_ids:
        return pd.DataFrame()

    # ─── 2) Buscar features filhas ───
    print(f"▶ [{project}] Buscando features...")
    features_url = (
        f"https://analytics.dev.azure.com/{organization}/{project}"
        "/_odata/v3.0-preview/WorkItems"
        "?$filter=WorkItemType eq 'Feature'"
    )
    resp = requests.get(features_url, headers=headers); resp.raise_for_status()
    features_data = resp.json().get('value', [])
    features_df   = pd.json_normalize(features_data)

    # Normaliza datas
    features_df['TargetDate'] = (
        pd.to_datetime(features_df.get('TargetDate',''), errors='coerce')
          .dt.tz_localize(None)
          .apply(lambda d: datetime.combine(d.date(), time(23,59,59)) if pd.notnull(d) else d)
    )
    features_df['ClosedDate'] = (
        pd.to_datetime(features_df.get('ClosedDate',''), errors='coerce')
          .dt.tz_localize(None)
    )
    features_df['ResolvedIn'] = (
        pd.to_datetime(features_df.get('Custom_ResolvedIn',''), errors='coerce')
          .dt.tz_localize(None)
    )

    # Filtra só as features ativas ligadas aos épicos
    features_df = features_df[
        features_df['ParentWorkItemId'].isin(epic_ids) &
        (features_df['State'] != 'Removed')
    ]
    feature_ids = features_df['WorkItemId'].tolist()
    print(f"   → {len(feature_ids)} features associadas encontradas.")

    # ─── 3) Métricas de Features ───
    features_ativas = features_df[features_df['State'] != 'Removed']
    total_summary     = features_ativas.groupby('ParentWorkItemId') \
                          .size().reset_index(name='TotalFilhos_Features')
    concluidos_summary = features_df[features_df['State'].isin(['Closed','Resolved'])] \
                          .groupby('ParentWorkItemId').size() \
                          .reset_index(name='Concluidos_Features')
    atrasados_summary = features_df[
                          (features_df['TargetDate'] < hoje) &
                          (~features_df['State'].isin(['Closed','Resolved']))
                        ].groupby('ParentWorkItemId').size() \
                         .reset_index(name='Atrasados_Features')
    esperados_summary = features_df[
                          (features_df['TargetDate'] < hoje)
                        ].groupby('ParentWorkItemId').size() \
                         .reset_index(name='FilhosEsperadosConcluidos_Features')

    atrasaram_df = features_df[features_df['State'].isin(['Closed','Resolved'])].copy()
    atrasaram_df['Entrega'] = atrasaram_df['ResolvedIn'] \
        .combine_first(atrasaram_df['ClosedDate']) \
        .fillna(hoje)
    atrasaram_df['Atrasaram'] = atrasaram_df.apply(
        lambda r: pd.notnull(r['TargetDate']) and r['Entrega'] > r['TargetDate'],
        axis=1
    )
    atrasaram_summary = atrasaram_df[atrasaram_df['Atrasaram']] \
        .groupby('ParentWorkItemId').size() \
        .reset_index(name='Atrasaram_Features')

    # ─── 4) Contagem de children de cada feature ───
    print(f"▶ [{project}] Buscando e contando filhos das features...")
    child_counts = {}
    concluidos_c  = {}
    atrasados_c   = {}
    atrasaram_c   = {}
    esperados_c   = {}

    for idx, fid in enumerate(feature_ids, 1):
        print(f"   • feature {idx}/{len(feature_ids)}", end='\r')
        url  = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitems/{fid}?$expand=relations&api-version=7.0"
        resp = requests.get(url, headers=headers)
        count = concl = atrd = atrm = esp = 0

        if resp.status_code == 200:
            rels      = resp.json().get('relations', [])
            child_ids = [
                r['url'].split('/')[-1] for r in rels
                if r.get('rel') == 'System.LinkTypes.Hierarchy-Forward'
            ]
            if child_ids:
                batch_url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitemsbatch?api-version=7.0"
                payload   = {
                    "ids": child_ids,
                    "fields": [
                        "System.WorkItemType","System.State",
                        "Microsoft.VSTS.Scheduling.TargetDate",
                        "Microsoft.VSTS.Common.ClosedDate","Custom.ResolvedIn"
                    ]
                }
                batch = requests.post(batch_url, headers=headers, json=payload)
                if batch.status_code == 200:
                    for item in batch.json().get('value', []):
                        flds    = item.get('fields', {})
                        wi_type = flds.get('System.WorkItemType','')
                        if wi_type not in ['User Story','Spike','Bug']:
                            continue
                        state = flds.get('System.State','')
                        if state == 'Removed':
                            continue

                        tgt = pd.to_datetime(flds.get('Microsoft.VSTS.Scheduling.TargetDate'), errors='coerce')
                        if pd.notnull(tgt):
                            tgt = tgt.tz_localize(None)
                            tgt = datetime.combine(tgt.date(), time(23,59,59))

                        closed   = pd.to_datetime(flds.get('Microsoft.VSTS.Common.ClosedDate'), errors='coerce')
                        resolved = pd.to_datetime(flds.get('Custom.ResolvedIn'), errors='coerce')
                        entrega  = (resolved if pd.notnull(resolved) else closed)
                        entrega  = entrega.tz_localize(None) if pd.notnull(entrega) else hoje

                        count += 1
                        if state in ['Closed','Resolved']:
                            concl += 1
                            if pd.notnull(tgt) and entrega > tgt:
                                atrm += 1
                        if pd.notnull(tgt) and hoje > tgt:
                            esp += 1
                            if state not in ['Closed','Resolved']:
                                atrd += 1

        child_counts[fid] = count
        concluidos_c[fid] = concl
        atrasados_c[fid]  = atrd
        atrasaram_c[fid]  = atrm
        esperados_c[fid]  = esp

    print("\n   ✔️ Contagem de filhos concluída.")

    # ─── 5) Agregar resumos de children por épico ───
    feature_child_df = pd.DataFrame(
        list(child_counts.items()),
        columns=['FeatureId','TotalFilhos_Children']
    )
    feature_concluidos_df = pd.DataFrame(
        list(concluidos_c.items()),
        columns=['FeatureId','Concluidos_Children']
    )
    feature_atrasados_df = pd.DataFrame(
        list(atrasados_c.items()),
        columns=['FeatureId','Atrasados_Children']
    )
    feature_atrasaram_df = pd.DataFrame(
        list(atrasaram_c.items()),
        columns=['FeatureId','Atrasaram_Children']
    )
    feature_esperados_df = pd.DataFrame(
        list(esperados_c.items()),
        columns=['FeatureId','FilhosEsperadosConcluidos_Children']
    )

    epic_child_summary = features_df[['WorkItemId','ParentWorkItemId']] \
        .merge(feature_child_df, left_on='WorkItemId', right_on='FeatureId', how='left') \
        .groupby('ParentWorkItemId') \
        .agg({'TotalFilhos_Children':'sum'}) \
        .reset_index()
    epic_concluidos = features_df[['WorkItemId','ParentWorkItemId']] \
        .merge(feature_concluidos_df, left_on='WorkItemId', right_on='FeatureId', how='left') \
        .groupby('ParentWorkItemId') \
        .agg({'Concluidos_Children':'sum'}) \
        .reset_index()
    epic_atrasados = features_df[['WorkItemId','ParentWorkItemId']] \
        .merge(feature_atrasados_df, left_on='WorkItemId', right_on='FeatureId', how='left') \
        .groupby('ParentWorkItemId') \
        .agg({'Atrasados_Children':'sum'}) \
        .reset_index()
    epic_atrasaram = features_df[['WorkItemId','ParentWorkItemId']] \
        .merge(feature_atrasaram_df, left_on='WorkItemId', right_on='FeatureId', how='left') \
        .groupby('ParentWorkItemId') \
        .agg({'Atrasaram_Children':'sum'}) \
        .reset_index()
    epic_esperados = features_df[['WorkItemId','ParentWorkItemId']] \
        .merge(feature_esperados_df, left_on='WorkItemId', right_on='FeatureId', how='left') \
        .groupby('ParentWorkItemId') \
        .agg({'FilhosEsperadosConcluidos_Children':'sum'}) \
        .reset_index()

    # ─── 6) Buscar detalhes dos épicos e atribuir Team ───
    print(f"▶ [{project}] Buscando detalhes finais dos épicos...")
    details = []
    for i, wid in enumerate(epic_ids, 1):
        print(f"   • épico {i}/{len(epic_ids)}", end='\r')
        wi_url    = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitems/{wid}?api-version=7.0"
        desc_resp = requests.get(wi_url, headers=headers)
        last_comment = ''
        if desc_resp.status_code == 200:
            fields          = desc_resp.json().get('fields', {})
            area_path       = fields.get('System.AreaPath','')

            parts = area_path.split('\\')

            # 1) match exato de prefixo único
            if area_path in prefs_map and len(prefs_map[area_path]) == 1:
                team = prefs_map[area_path][0]
            # 2) terceiro nível do AreaPath
            elif len(parts) >= 3 and parts[2] in all_teams:
                team = parts[2]
            # 3) segundo nível do AreaPath
            elif len(parts) >= 2 and parts[1] in all_teams:
                team = parts[1]
            # 4) primeiro nível do AreaPath
            elif len(parts) >= 1 and parts[0] in all_teams:
                team = parts[0]
            else:
                team = project

            # Limpa descrição e captura último comentário
            description_clean = BeautifulSoup(fields.get('System.Description',''),'html.parser') \
                .get_text(separator=' ', strip=True)
            cm_url      = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitems/{wid}/comments?$top=1&orderBy=createdDate desc"
            cm_resp     = requests.get(cm_url, headers=headers)
            if cm_resp.status_code == 200 and cm_resp.json().get('comments'):
                last_comment = BeautifulSoup(cm_resp.json()['comments'][0].get('text',''),'html.parser') \
                    .get_text(separator=' ', strip=True)

            details.append({
                'WorkItemId':      wid,
                'Title':           fields.get('System.Title',''),
                'AreaPath':        area_path,
                'Team':            team,
                'WorkItemType':    fields.get('System.WorkItemType',''),
                'State':           fields.get('System.State',''),
                'AssignedTo':      (fields.get('System.AssignedTo') or {}).get('displayName',''),
                'TagNames':        fields.get('System.Tags',''),
                'TargetDate':      fields.get('Microsoft.VSTS.Scheduling.TargetDate',''),
                'ClosedDate':      fields.get('Microsoft.VSTS.Common.ClosedDate',''),
                'Description':     description_clean,
                'LastComment':     last_comment
            })

    # ─── 7) Montar final_df e mesclar métricas ───
    final_df = pd.DataFrame(details)

    for df, name in [
        (total_summary,      'TotalFilhos_Features'),
        (concluidos_summary, 'Concluidos_Features'),
        (atrasados_summary,  'Atrasados_Features'),
        (atrasaram_summary,  'Atrasaram_Features'),
        (esperados_summary,  'FilhosEsperadosConcluidos_Features')
    ]:
        final_df = final_df.merge(
            df,
            left_on='WorkItemId', right_on='ParentWorkItemId',
            how='left'
        ).drop(columns=['ParentWorkItemId'], errors='ignore')
        final_df[name] = final_df[name].fillna(0).astype(int)

    # Percentuais de Features
    final_df['%Conclusao_Features'] = (
        final_df['Concluidos_Features'] / final_df['TotalFilhos_Features']
    ).replace([float('inf'), float('nan')], 0).round(2)
    final_df['%EsperadoConclusao_Features'] = (
        final_df['FilhosEsperadosConcluidos_Features'] / final_df['TotalFilhos_Features']
    ).replace([float('inf'), float('nan')], 0).round(2)
    final_df['%Atrasaram_Features'] = (
        final_df['Atrasaram_Features'] / final_df['TotalFilhos_Features']
    ).replace([float('inf'), float('nan')], 0).round(2)

    # --- Mescla métricas de Children ---
    for df, name in [
        (epic_child_summary,  'TotalFilhos_Children'),
        (epic_concluidos,      'Concluidos_Children'),
        (epic_atrasados,       'Atrasados_Children'),
        (epic_atrasaram,       'Atrasaram_Children'),
        (epic_esperados,       'FilhosEsperadosConcluidos_Children')
    ]:
        final_df = final_df.merge(
            df,
            left_on='WorkItemId', right_on='ParentWorkItemId',
            how='left'
        ).drop(columns=['ParentWorkItemId'], errors='ignore')
        final_df[name] = final_df[name].fillna(0).astype(int)

    # Percentuais de Children
    final_df['%Conclusao_Children'] = (
        final_df['Concluidos_Children'] / final_df['TotalFilhos_Children']
    ).replace([float('inf'), float('nan')], 0).round(2)
    final_df['%EsperadoConclusao_Children'] = (
        final_df['FilhosEsperadosConcluidos_Children'] / final_df['TotalFilhos_Children']
    ).replace([float('inf'), float('nan')], 0).round(2)
    final_df['%Atrasaram_Children'] = (
        final_df['Atrasaram_Children'] / final_df['TotalFilhos_Children']
    ).replace([float('inf'), float('nan')], 0).round(2)

    # Ordena e retorna
    final_df = final_df.sort_values(by='WorkItemId').reset_index(drop=True)
    return final_df

def main():
    start_time = time_module.time()

    # Autenticação
    pat = os.environ.get('AZURE_PAT')
    if not pat:
        print("❌ ERRO: AZURE_PAT não definido."); sys.exit(1)
    b64     = base64.b64encode(f":{pat}".encode()).decode()
    headers = {'Authorization': f"Basic {b64}"}

    organization = os.environ.get('ORG', 'arezzosa')
    projects_env = os.environ.get('PROJECT', 'FUTURO_E-COMMERCE,ZZAPPS,ARZZ')
    projects     = [p.strip() for p in projects_env.split(',') if p.strip()]

    all_dfs = []
    for proj in projects:
        try:
            df = process_project(organization, proj, headers)
            if not df.empty:
                df['Project'] = proj
                all_dfs.append(df)
            else:
                print(f"[{proj}] Sem épicos para processar.")
        except Exception as e:
            print(f"❗ Falha ao processar '{proj}': {e}")

    if not all_dfs:
        print("❌ Nenhum projeto processado."); sys.exit(1)

    result_df = pd.concat(all_dfs, ignore_index=True)

    # Exporta CSV + XLSX (aba única)
    result_df.to_csv('epics_with_full_counts.csv', index=False, encoding='utf-8-sig')
    with pd.ExcelWriter('epics_with_full_counts.xlsx', engine='openpyxl') as w:
        result_df.to_excel(w, sheet_name='AllProjects', index=False)

    print("✅ Arquivos gerados: epics_with_full_counts.csv, epics_with_full_counts.xlsx")
    print(f"⏱️ Tempo total: {round(time_module.time() - start_time, 2)}s")

if __name__ == "__main__":
    main()
