# teste_fluxo.py
import asyncio, aiohttp, sqlite3, requests, re, json
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
async def fetch_search():
    itens = []
    async with aiohttp.ClientSession() as s:
        for ordem in ORDENACAO:
            for doc in TIPOS_DOCUMENTO:
                for pg in PAGES:
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
    c.execute("SELECT id, orgao_cnpj, ano, numero_sequencial FROM licitacoes")
    licitacoes = c.fetchall()
    for lic_id, org, ano, seq in licitacoes:
        # Verificar itens
        c.execute("SELECT COUNT(*) FROM itens WHERE id_licitacao=?", (lic_id,))
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
        c.execute("SELECT COUNT(*) FROM arquivos WHERE id_licitacao=?", (lic_id,))
        n_arquivos = c.fetchone()[0]
        if n_arquivos == 0:
            arquivos = await fetch_arquivos(org, ano, seq)
            for ar in arquivos:
                c.execute("""INSERT OR IGNORE INTO arquivos
                             (id_licitacao, sequencial_documento, url, titulo, status_ativo)
                             VALUES (?,?,?,?,?)""",
                          (lic_id, ar["sequencialDocumento"], ar["url"],
                           ar.get("titulo"), ar.get("statusAtivo")))
            conn.commit()
            if arquivos:
                log(f"RECUPERADO ARQUIVOS {lic_id}: {len(arquivos)} gravados.")

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
    with ThreadPoolExecutor(max_workers=MAX_CONN) as executor:
        executor.map(lambda args: postprocess(*args), arquivos_para_converter)

    # Fase 3: recuperar itens/arquivos faltantes
    await recuperar_faltantes()

if __name__ == "__main__":
    asyncio.run(main())
