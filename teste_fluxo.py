# teste_fluxo.py
import asyncio, aiohttp, sqlite3, requests, re, json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from markitdown import MarkItDown

# ============ CONFIG ============
SEARCH_URL   = "https://pncp.gov.br/api/search/"
BASE_PNCP    = "https://pncp.gov.br/api/pncp/v1/orgaos/"
TIPOS_DOCUMENTO = ["edital","ata"]  #edital ou ata
ORDENACAO       = ["data","-data"]  # data,-data,relevancia; sendo que "-data" é o mais antigo
PAGES           = list(range(1,2)) # lembrar que o limite superior da faixa não é incluso, então se quiser que vá até a página 20, é preciso colocar 21
TAM_PAGINA      = 1
MAX_CONN        = 5
DB_PATH         = "database_lite.db"

# ============ HELPERS ============
def now():
    return datetime.now(timezone.utc)

def log(msg):
    print(f"[{now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# ============ BANCO ============
conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.executescript("""
-- licitacoes (colunas = chaves JSON)
CREATE TABLE IF NOT EXISTS licitacoes (
    id TEXT PRIMARY KEY,
    "index" TEXT,
    doc_type TEXT,
    title TEXT,
    description TEXT,
    item_url TEXT,
    document_type TEXT,
    createdAt DATETIME,
    numero TEXT,
    ano INTEGER,
    numero_sequencial INTEGER,
    numero_sequencial_compra_ata INTEGER,
    numero_controle_pncp TEXT,
    orgao_id TEXT,
    orgao_cnpj TEXT,
    orgao_nome TEXT,
    orgao_subrogado_id TEXT,
    orgao_subrogado_nome TEXT,
    unidade_id TEXT,
    unidade_codigo TEXT,
    unidade_nome TEXT,
    esfera_id TEXT,
    esfera_nome TEXT,
    poder_id TEXT,
    poder_nome TEXT,
    municipio_id TEXT,
    municipio_nome TEXT,
    uf TEXT,
    modalidade_licitacao_id TEXT,
    modalidade_licitacao_nome TEXT,
    situacao_id TEXT,
    situacao_nome TEXT,
    data_publicacao_pncp DATETIME,
    data_atualizacao_pncp DATETIME,
    data_assinatura DATETIME,
    data_inicio_vigencia DATETIME,
    data_fim_vigencia DATETIME,
    cancelado BOOLEAN,
    valor_global REAL,
    tem_resultado BOOLEAN,
    tipo_id TEXT,
    tipo_nome TEXT,
    tipo_contrato_id TEXT,
    fonte_orcamentaria TEXT,
    fonte_orcamentaria_id TEXT,
    fonte_orcamentaria_nome TEXT,
    tipo_contrato_nome TEXT
);
CREATE INDEX IF NOT EXISTS idx_licitacoes_controle
  ON licitacoes(numero_controle_pncp);

-- itens
CREATE TABLE IF NOT EXISTS itens (
    id_licitacao TEXT,
    numeroItem INTEGER,
    descricao TEXT,
    valor_total REAL,
    PRIMARY KEY (id_licitacao, numeroItem)
);

-- arquivos
CREATE TABLE IF NOT EXISTS arquivos (
    id_licitacao TEXT,
    sequencial_documento INTEGER,
    url TEXT,
    titulo TEXT,
    status_ativo BOOLEAN,
    PRIMARY KEY (id_licitacao, sequencial_documento)
);

-- markdown
CREATE TABLE IF NOT EXISTS arquivo_markdown (
    id_licitacao TEXT,
    sequencial_documento INTEGER,
    nome_arquivo TEXT,
    conteudo_markdown TEXT,
    convertido_com_sucesso BOOLEAN,
    erro TEXT,
    timestamp TEXT,
    PRIMARY KEY(id_licitacao, sequencial_documento)
);
                


""")
conn.commit()

sem = asyncio.Semaphore(MAX_CONN)

# ============ FETCH ============
async def fetch_search(page_list: list[int]):
    itens = []
    async with aiohttp.ClientSession() as s:
        for ordem in ORDENACAO:
            for doc in TIPOS_DOCUMENTO:
                for pg in page_list: # Changed PAGES to page_list
                    p = {
                        "pagina": pg, "tam_pagina": TAM_PAGINA,
                        "ordenacao": ORDENACAO[0], "q": "",
                        "tipos_documento": TIPOS_DOCUMENTO[0], "status": "todos"
                    }
                    async with sem:
                        resp = await s.get(SEARCH_URL, params=p)
                        if resp.status != 200:
                            log(f"ERRO search {pg}: {resp.status}")
                            continue
                        data = await resp.json()
                    page_items = data.get("items", [])
                    log(f"SEARCH página {pg}: {len(page_items)} itens")
                    itens.extend(page_items)
    return itens

async def fetch_itens(org, ano, seq):
    url = f"{BASE_PNCP}{org}/compras/{ano}/{seq}/itens"
    params = {"pagina":1,"tamanhoPagina":2}
    try:
        async with aiohttp.ClientSession() as s:
            async with sem:
                r = await s.get(url, params=params)
                return [] if r.status!=200 else await r.json()
    except Exception: return []

async def fetch_arquivos(org, ano, seq):
    url = f"{BASE_PNCP}{org}/compras/{ano}/{seq}/arquivos"
    params = {"pagina":1,"tamanhoPagina":2}
    try:
        async with aiohttp.ClientSession() as s:
            async with sem:
                r = await s.get(url, params=params)
                return [] if r.status!=200 else await r.json()
    except Exception: return []

# ============ MARKDOWN ============
def postprocess(lic_id, seq_doc, url):
    try:
        cd = requests.head(url, allow_redirects=True, timeout=10)\
              .headers.get("Content-Disposition","")
        fname = re.search(r'filename="?([^";]+)"?', cd)
        fname = fname.group(1) if fname else f"{lic_id}_{seq_doc}"
    except Exception:
        fname = f"{lic_id}_{seq_doc}"
    md_file = fname

    md = MarkItDown(enable_plugins=False)
    try:
        txt = md.convert(url).text_content
        ok, err = True, ""
    except Exception as e:
        txt, ok, err = "Não foi possível converter para markdown", False, str(e)

    # Cada thread abre sua própria conexão/cursor
    conn_local = sqlite3.connect(DB_PATH)
    c_local = conn_local.cursor()
    c_local.execute("""INSERT OR REPLACE INTO arquivo_markdown
                 VALUES (?,?,?,?,?,?,?)""",
              (lic_id, seq_doc, md_file, txt, ok, err, now().isoformat()))
    conn_local.commit()
    conn_local.close()
    log(f"MARKDOWN {lic_id}: convertido={ok}")

# ============ MAIN ============
async def main():
    primarios = await fetch_search()

    # deduplicação primária
    ids_all = {p["id"] for p in primarios}
    c.execute("SELECT id FROM licitacoes")
    ids_db  = {r[0] for r in c.fetchall()}
    novos   = ids_all - ids_db
    log(f"NOVOS {len(novos)} / TOTAL {len(ids_all)}")

    # inserção direta (colunas iguais às chaves)
    for it in primarios:
        cols = list(it.keys())
        placeholders = ",".join("?" for _ in cols)
        cols_escaped = [f'"{c}"' if c.lower()=="index" else c for c in cols]
        c.execute(f"INSERT OR IGNORE INTO licitacoes ({','.join(cols_escaped)}) VALUES ({placeholders})",
                  [it[k] for k in cols])
    conn.commit()

    # Fase 1: processar somente os novos, buscar itens/arquivos e inserir no banco, coletar arquivos para conversão
    arquivos_para_converter = []
    for it in primarios:
        if it["id"] not in novos:
            continue
        lic_id = it["id"]
        org, ano, seq = it["orgao_cnpj"], it["ano"], it["numero_sequencial"]

        # ---- ITENS ----
        itens = await fetch_itens(org, ano, seq)
        for obj in itens:
            c.execute("""
              INSERT OR IGNORE INTO itens (id_licitacao, numeroItem, descricao, valor_total)
              VALUES (?,?,?,?)""",
              (lic_id, obj["numeroItem"], obj.get("descricao"), obj.get("valorTotal")))
        conn.commit()
        if itens:
            log(f"ITENS {lic_id}: {len(itens)} gravados. Exemplo -> {json.dumps(itens[0], ensure_ascii=False)[:120]}...")

        # ---- ARQUIVOS ----
        arquivos = await fetch_arquivos(org, ano, seq)
        for ar in arquivos:
            c.execute("""INSERT OR IGNORE INTO arquivos
                         (id_licitacao, sequencial_documento, url, titulo, status_ativo)
                         VALUES (?,?,?,?,?)""",
                      (lic_id, ar["sequencialDocumento"], ar["url"],
                       ar.get("titulo"), ar.get("statusAtivo")))
            arquivos_para_converter.append((lic_id, ar["sequencialDocumento"], ar["url"]))
        conn.commit()
        if arquivos:
            log(f"ARQUIVOS {lic_id}: {len(arquivos)} gravados. Exemplo -> {json.dumps(arquivos[0], ensure_ascii=False)[:120]}...")

    # Fase 2: converter arquivos em markdown após todas as requisições
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_CONN*5) as executor:
        executor.map(lambda args: postprocess(*args), arquivos_para_converter)


async def recuperar_faltantes():
    # Buscar todas as licitações
    batch_size = 500
    offset = 0
    while True:
        c.execute("SELECT numero_controle_pncp, orgao_cnpj, ano, numero_sequencial FROM licitacoes ORDER BY numero_controle_pncp LIMIT ? OFFSET ?", (batch_size, offset))
        licitacoes_batch = c.fetchall()
        if not licitacoes_batch:
            break

        for lic_id, org, ano, seq in licitacoes_batch:
            # Verificar itens
            c.execute("SELECT COUNT(*) FROM itens WHERE numero_controle_pncp=?", (lic_id,))
        n_itens = c.fetchone()[0]
        if n_itens == 0:
            itens = await fetch_itens(org, ano, seq)
            for obj in itens:
                c.execute("""
                  INSERT OR IGNORE INTO itens (id_licitacao, numeroItem, descricao, valor_total)
                  VALUES (?,?,?,?)""",
                  (lic_id, obj["numeroItem"], obj.get("descricao"), obj.get("valorTotal")))
            conn.commit()
            if itens:
                log(f"RECUPERADO ITENS {lic_id}: {len(itens)} gravados.")

            # Verificar arquivos
            c.execute("SELECT COUNT(*) FROM arquivos WHERE numero_controle_pncp=?", (lic_id,))
            n_arquivos = c.fetchone()[0]
            if n_arquivos == 0:
                arquivos = await fetch_arquivos(org, ano, seq)
                for ar in arquivos:
                    c.execute("""INSERT OR IGNORE INTO arquivos
                                 (numero_controle_pncp, sequencial_documento, url, titulo, status_ativo)
                                 VALUES (?,?,?,?,?)""",
                              (lic_id, ar["sequencialDocumento"], ar["url"],
                               ar.get("titulo"), ar.get("statusAtivo")))
                conn.commit()
                if arquivos:
                    log(f"RECUPERADO ARQUIVOS {lic_id}: {len(arquivos)} gravados.")
        offset += batch_size

async def main():
    PAGE_BATCH_SIZE = 5
    MARKDOWN_BATCH_SIZE = 50
    # FULL_PAGES_LIST would be defined based on the global PAGES or other logic
    # For this refactoring, we assume PAGES is the intended list.
    FULL_PAGES_LIST = PAGES

    # Fetch initial DB state using numero_controle_pncp
    c.execute("SELECT numero_controle_pncp FROM licitacoes") # SCHEMACHANGE
    ids_db_master = {r[0] for r in c.fetchall()}

    arquivos_para_converter = []
    total_new_licitacoes_identified_count = 0

    # Loop through FULL_PAGES_LIST in chunks
    for i in range(0, len(FULL_PAGES_LIST), PAGE_BATCH_SIZE):
        page_batch_list = FULL_PAGES_LIST[i:i + PAGE_BATCH_SIZE]
        log(f"Fetching API page batch: {page_batch_list}")
        # Ensure fetch_search is called with the list of pages for the current batch
        primarios_this_api_batch = await fetch_search(page_batch_list)

        licitacoes_to_insert_in_db_current_batch = []
        # This list will hold licitacoes that are new and need item/file processing
        licitacoes_newly_identified_for_items_files_current_batch = []

        for it_api in primarios_this_api_batch:
            lic_pncp_id = it_api.get("numero_controle_pncp") # Use numero_controle_pncp from API
            if not lic_pncp_id:
                log(f"SKIP API item: Licitação sem numero_controle_pncp. Data: {it_api}")
                continue

            # Check against master list of IDs already in DB or marked as processed in this run
            if lic_pncp_id not in ids_db_master:
                licitacoes_to_insert_in_db_current_batch.append(it_api)
                licitacoes_newly_identified_for_items_files_current_batch.append(it_api)
                ids_db_master.add(lic_pncp_id) # Add to master list to avoid re-processing if found in later API page batches

        if licitacoes_to_insert_in_db_current_batch:
            log(f"Attempting to insert {len(licitacoes_to_insert_in_db_current_batch)} new licitacoes from batch into DB.")
            for it_to_insert_db in licitacoes_to_insert_in_db_current_batch:
                # The 'id' key from API might still be present, ensure 'numero_controle_pncp' is correctly mapped
                # to the primary key column 'numero_controle_pncp' in the licitacoes table.
                # The schema change made 'numero_controle_pncp' the PK.
                # The API data contains 'numero_controle_pncp' which should be used.
                # The generic column insertion should work if 'numero_controle_pncp' is present in it_to_insert_db keys.
                cols = list(it_to_insert_db.keys())
                placeholders = ",".join("?" for _ in cols)
                cols_escaped = [f'"{c}"' if c.lower()=="index" else c for c in cols]

                # Ensure 'numero_controle_pncp' field from it_to_insert_db is correctly inserted into 'numero_controle_pncp' PK column
                # This relies on it_to_insert_db having a 'numero_controle_pncp' key from the API.
                c.execute(f"INSERT OR IGNORE INTO licitacoes ({','.join(cols_escaped)}) VALUES ({placeholders})",
                          [it_to_insert_db.get(k) for k in cols]) # Use .get(k) for safety, though keys should exist
            conn.commit()
            total_new_licitacoes_identified_count += len(licitacoes_to_insert_in_db_current_batch)

        log(f"Processing items/files for {len(licitacoes_newly_identified_for_items_files_current_batch)} licitacoes from batch.")
        for it_new_process in licitacoes_newly_identified_for_items_files_current_batch:
            # This lic_pncp_id is confirmed new to the database
            lic_pncp_id = it_new_process["numero_controle_pncp"]
            org = it_new_process.get("orgao_cnpj") # Use .get for safety
            ano = it_new_process.get("ano")
            seq = it_new_process.get("numero_sequencial")

            if not all([org, ano, seq]): # Basic check for necessary fields for fetching items/arquivos
                log(f"SKIP items/files: Licitação {lic_pncp_id} missing org_cnpj, ano, or numero_sequencial.")
                continue

            # Fetch and insert Itens (using numero_controle_pncp as the foreign key)
            itens = await fetch_itens(org, ano, seq)
            if itens:
                for obj in itens:
                    c.execute("INSERT OR IGNORE INTO itens (numero_controle_pncp, numeroItem, descricao, valor_total) VALUES (?,?,?,?)",
                              (lic_pncp_id, obj.get("numeroItem"), obj.get("descricao"), obj.get("valorTotal"))) # SCHEMACHANGE 반영
                conn.commit()
                log(f"ITENS {lic_pncp_id}: {len(itens)} gravados.")

            # Fetch and insert Arquivos (using numero_controle_pncp as the foreign key)
            arquivos_db_entries = await fetch_arquivos(org, ano, seq)
            if arquivos_db_entries:
                for ar in arquivos_db_entries:
                    c.execute("INSERT OR IGNORE INTO arquivos (numero_controle_pncp, sequencial_documento, url, titulo, status_ativo) VALUES (?,?,?,?,?)",
                              (lic_pncp_id, ar.get("sequencialDocumento"), ar.get("url"), ar.get("titulo"), ar.get("statusAtivo"))) # SCHEMACHANGE 반영
                    if ar.get("url"): # Ensure URL exists before adding to markdown conversion
                        arquivos_para_converter.append((lic_pncp_id, ar.get("sequencialDocumento"), ar.get("url")))
                conn.commit()
                log(f"ARQUIVOS {lic_pncp_id}: {len(arquivos_db_entries)} gravados.")

            # Trigger markdown conversion if batch size reached
            if len(arquivos_para_converter) >= MARKDOWN_BATCH_SIZE:
                log(f"Processing markdown for a batch of {len(arquivos_para_converter)} files.")
                with ThreadPoolExecutor(max_workers=MAX_CONN * 2) as executor:
                    executor.map(lambda args: postprocess(*args), arquivos_para_converter)
                arquivos_para_converter.clear()

    log(f"Finished processing all API page batches. Total new licitacoes identified for DB insertion: {total_new_licitacoes_identified_count}")

    # Final Markdown Conversion Trigger
    if arquivos_para_converter:
        log(f"Processing remaining {len(arquivos_para_converter)} markdown files.")
        with ThreadPoolExecutor(max_workers=MAX_CONN * 2) as executor:
            executor.map(lambda args: postprocess(*args), arquivos_para_converter)
        arquivos_para_converter.clear()

    # Fase 3: recuperar itens/arquivos faltantes (now internally batched)
    await recuperar_faltantes()

if __name__ == "__main__":
    asyncio.run(main())
