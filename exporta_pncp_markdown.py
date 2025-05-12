import sqlite3
import os
import re
from datetime import datetime
from typing import Any, Optional

# ==================== utilidades ====================

def format_currency(value: Optional[float]) -> str:
    """Converte número em string no formato monetário brasileiro ou retorna '-' se None."""
    if value is None:
        return "-"
    try:
        return f"R$ {value:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    except (TypeError, ValueError):
        return "-"


def safe_get(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    """Retorna o valor da coluna ou um valor padrão se inexistente/nulo."""
    if default is None:
        default = "-"
    return row[key] if key in row.keys() and row[key] not in (None, "") else default


def sanitize_filename(name: str, max_len: int = 120) -> str:
    """Remove ou substitui caracteres que não podem aparecer em nomes de arquivo."""
    sanitized = re.sub(r"[\\/:*?\"<>|]", "-", name)
    sanitized = re.sub(r"\s+", "_", sanitized)
    return sanitized[:max_len].strip(".-_")


# ==================== geração do markdown com Front Matter ====================

def create_markdown(
    licitacao: sqlite3.Row,
    itens: list[sqlite3.Row],
    arquivos: list[sqlite3.Row],
    docs_md: list[sqlite3.Row],
) -> str:
    """
    Gera o conteúdo completo de um arquivo Markdown, incluindo Front Matter YAML e seções:
    - Descrição Geral
    - Itens Licitados
    - Documentos Relacionados
    - Conteúdo dos arquivos
    """
    fm_lines: list[str] = []
    # campos front matter
    title = safe_get(licitacao, 'title')
    date = safe_get(licitacao, 'data_publicacao_pncp')
    slug = sanitize_filename(safe_get(licitacao, 'numero_controle_pncp') or safe_get(licitacao, 'id'))
    valor_global = licitacao['valor_global'] if licitacao['valor_global'] is not None else 0
    # coletar tags e categorias
    tags = [safe_get(licitacao, 'uf'), safe_get(licitacao, 'modalidade_licitacao_nome')]

    fm_lines.append("---")
    fm_lines.append(f"title: \"{title}\"")
    fm_lines.append(f"date: \"{date}\"")
    fm_lines.append("draft: false")
    fm_lines.append(f"slug: \"{slug}\"")
    fm_lines.append(f"lic_id: \"{safe_get(licitacao, 'id')}\"")
    fm_lines.append(f"numero: \"{safe_get(licitacao, 'numero')}\"")
    fm_lines.append(f"ano: {safe_get(licitacao, 'ano')}")
    fm_lines.append(f"numero_sequencial: {safe_get(licitacao, 'numero_sequencial')}")
    fm_lines.append(f"numero_sequencial_compra_ata: \"{safe_get(licitacao, 'numero_sequencial_compra_ata')}\"")
    fm_lines.append(f"orgao_nome: \"{safe_get(licitacao, 'orgao_nome')}\"")
    fm_lines.append(f"uf: \"{safe_get(licitacao, 'uf')}\"")
    fm_lines.append(f"municipio: \"{safe_get(licitacao, 'municipio_nome')}\"")
    fm_lines.append(f"modalidade: \"{safe_get(licitacao, 'modalidade_licitacao_nome')}\"")
    fm_lines.append(f"situacao: \"{safe_get(licitacao, 'situacao_nome')}\"")
    fm_lines.append(f"valor_global: {valor_global}")
    fm_lines.append(f"items_count: {len(itens)}")
    fm_lines.append(f"docs_count: {len(arquivos)}")
    fm_lines.append(f"tags: [{', '.join(f'\"{t}\"' for t in tags if t and t != '-')}]" )
    fm_lines.append("categories: [\"licitacoes\"]")
    fm_lines.append("---\n")

    # corpo do documento
    md_lines: list[str] = ['']

    # Descrição Geral
    md_lines.append("## Descrição Geral")
    md_lines.append(safe_get(licitacao, 'description', '(Sem descrição)'))

    # Itens Licitados
    md_lines.append("\n---\n\n## Itens Licitados")
    md_lines.append("| Número | Descrição | Valor Total |")
    md_lines.append("|--------|-----------|-------------|")
    for item in itens:
        valor_formatado = format_currency(item['valor_total']) if 'valor_total' in item.keys() else "-"
        md_lines.append(f"| {item['numeroItem']} | {safe_get(item, 'descricao')} | {valor_formatado} |")

    # Documentos Relacionados
    md_lines.append("\n---\n\n## Documentos Relacionados")
    if arquivos:
        for arq in arquivos:
            md_lines.append(f"- [{safe_get(arq, 'titulo')}]({safe_get(arq, 'url')})")
    else:
        md_lines.append("(Nenhum documento relacionado)")

    # Conteúdo dos arquivos
    md_lines.append("\n---\n\n## Conteúdo dos arquivos")
    if docs_md:
        for doc in docs_md:
            doc_title = safe_get(doc, 'nome_arquivo', f"Documento {safe_get(doc, 'sequencial_documento')}")
            md_lines.append(f"\n### {doc_title}\n")
            md_lines.append(doc['conteudo_markdown'] or '(Sem conteúdo)')
    else:
        md_lines.append("(Nenhum conteúdo convertido disponível)")

    # combinar front matter + corpo
    return '\n'.join(fm_lines + md_lines)


# ==================== exportação em lote ====================

def convert_all_to_markdown(db_path: str, output_folder: str) -> None:
    if not os.path.exists(output_folder):
        os.makedirs(output_folder, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT * FROM licitacoes")
    licitacoes = cur.fetchall()

    for lic in licitacoes:
        cur.execute("SELECT * FROM itens WHERE id_licitacao=? ORDER BY numeroItem", (lic['id'],))
        itens = cur.fetchall()

        cur.execute("SELECT * FROM arquivos WHERE id_licitacao=? ORDER BY sequencial_documento", (lic['id'],))
        arquivos = cur.fetchall()

        cur.execute(
            "SELECT * FROM arquivo_markdown WHERE id_licitacao=? AND sequencial_documento<>0 AND convertido_com_sucesso=1 ORDER BY sequencial_documento",
            (lic['id'],),
        )
        docs_md = cur.fetchall()

        md_content = create_markdown(lic, itens, arquivos, docs_md)

        raw_name = safe_get(lic, 'numero_controle_pncp') or safe_get(lic, 'id')
        file_name = f"{sanitize_filename(raw_name)}.md"
        file_path = os.path.join(output_folder, file_name)

        try:
            with open(file_path, "w", encoding="utf-8") as mdfile:
                mdfile.write(md_content)
            ok, err_msg = True, None
        except Exception as exc:
            ok, err_msg = False, str(exc)

        timestamp = datetime.now().isoformat()
        cur.execute(
            """
            INSERT OR REPLACE INTO arquivo_markdown 
            (id_licitacao, sequencial_documento, nome_arquivo, conteudo_markdown, convertido_com_sucesso, erro, timestamp)
            VALUES (?, 0, ?, ?, ?, ?, ?)""",
            (
                lic['id'],
                file_name,
                md_content if ok else "",
                ok,
                err_msg,
                timestamp,
            ),
        )

        print(f"Markdown {'gerado' if ok else 'falhou'}: {file_path}{' -> ' + err_msg if err_msg else ''}")

    conn.commit()
    conn.close()


if __name__ == '__main__':
    db_path = "database.db"
    output_folder = "licitacoes-site/content/licitacoes"

    convert_all_to_markdown(db_path, output_folder)
