from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
from typing import List, Optional
from pydantic import BaseModel
import uvicorn

app = FastAPI()

# Allow CORS for frontend running on different origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE = "database2.db"

def get_db_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

class Licitacao(BaseModel):
    id: str
    title: Optional[str]
    description: Optional[str]
    numero: Optional[str]
    ano: Optional[int]
    orgao_nome: Optional[str]
    unidade_nome: Optional[str]
    esfera_nome: Optional[str]
    municipio_nome: Optional[str]
    uf: Optional[str]
    modalidade_licitacao_nome: Optional[str]
    situacao_nome: Optional[str]
    data_publicacao_pncp: Optional[str]
    data_inicio_vigencia: Optional[str]
    data_fim_vigencia: Optional[str]
    valor_global: Optional[float]

class Item(BaseModel):
    id_licitacao: str
    numeroItem: int
    descricao: str
    valor_total: float

class Arquivo(BaseModel):
    id_licitacao: str
    sequencial_documento: int
    url: Optional[str]
    titulo: Optional[str]
    status_ativo: Optional[bool]

@app.get("/licitacoes", response_model=List[Licitacao])
def list_licitacoes(orgao: Optional[str] = None, tipo: Optional[str] = None, situacao: Optional[str] = None,
                    municipio: Optional[str] = None, data_inicio: Optional[str] = None, data_fim: Optional[str] = None):
    conn = get_db_connection()
    query = "SELECT id, title, description, numero, ano, orgao_nome, unidade_nome, esfera_nome, municipio_nome, uf, modalidade_licitacao_nome, situacao_nome, data_publicacao_pncp, data_inicio_vigencia, data_fim_vigencia, valor_global FROM licitacoes WHERE 1=1"
    params = []
    if orgao:
        query += " AND orgao_nome = ?"
        params.append(orgao)
    if tipo:
        query += " AND modalidade_licitacao_nome = ?"
        params.append(tipo)
    if situacao:
        query += " AND situacao_nome = ?"
        params.append(situacao)
    if municipio:
        query += " AND municipio_nome = ?"
        params.append(municipio)
    if data_inicio:
        query += " AND data_publicacao_pncp >= ?"
        params.append(data_inicio)
    if data_fim:
        query += " AND data_publicacao_pncp <= ?"
        params.append(data_fim)
    query += " ORDER BY data_publicacao_pncp DESC LIMIT 100"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [Licitacao(**dict(row)) for row in rows]

@app.get("/licitacoes/{licitacao_id}", response_model=Licitacao)
def get_licitacao(licitacao_id: str):
    conn = get_db_connection()
    row = conn.execute("SELECT id, title, description, numero, ano, orgao_nome, unidade_nome, esfera_nome, municipio_nome, uf, modalidade_licitacao_nome, situacao_nome, data_publicacao_pncp, data_inicio_vigencia, data_fim_vigencia, valor_global FROM licitacoes WHERE id = ?", (licitacao_id,)).fetchone()
    conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Licitacao not found")
    return Licitacao(**dict(row))

@app.get("/licitacoes/{licitacao_id}/itens", response_model=List[Item])
def get_itens(licitacao_id: str):
    conn = get_db_connection()
    rows = conn.execute("SELECT id_licitacao, numeroItem, descricao, valor_total FROM itens WHERE id_licitacao = ?", (licitacao_id,)).fetchall()
    conn.close()
    return [Item(**dict(row)) for row in rows]

@app.get("/licitacoes/{licitacao_id}/arquivos", response_model=List[Arquivo])
def get_arquivos(licitacao_id: str):
    conn = get_db_connection()
    rows = conn.execute("SELECT id_licitacao, sequencial_documento, url, titulo, status_ativo FROM arquivos WHERE id_licitacao = ?", (licitacao_id,)).fetchall()
    conn.close()
    return [Arquivo(**dict(row)) for row in rows]

@app.get("/arquivo_markdown/{id_licitacao}/{sequencial_documento}")
def get_arquivo_markdown(id_licitacao: str, sequencial_documento: int):
    conn = get_db_connection()
    row = conn.execute("SELECT conteudo_markdown FROM arquivo_markdown WHERE id_licitacao = ? AND sequencial_documento = ?", (id_licitacao, sequencial_documento)).fetchone()
    conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Markdown content not found")
    return {"conteudo_markdown": row["conteudo_markdown"]}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
