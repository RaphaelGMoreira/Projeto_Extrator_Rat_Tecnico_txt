# Projeto Extrator RAT Tecnico TXT

Projeto para extrair atendimentos tecnicos (RAT) de arquivos `.txt` e gerar uma planilha Excel padronizada com duas abas:

- `DADOS`: registros normalizados para operacao
- `LOG`: trilha de regras aplicadas, alteracoes e inconsistencias

## Objetivo

Transformar textos operacionais (incluindo formatos heterogeneos, como exportacoes de conversa) em dados consistentes para auditoria, consolidacao e acompanhamento de campo.

## Funcionalidades Principais

- Leitura de um ou varios arquivos `.txt`
- Extracao de campos com multiplos formatos de entrada
- Normalizacao de datas, horas, tecnico, status, categoria e KM
- Regras sequenciais por tecnico/dia (incluindo geracao de retorno)
- Regras de qualidade e marcacao de inconsistencias
- Filtros por tecnico, status, cidade e inconsistencias
- Exportacao Excel com abas `DADOS` e `LOG`
- Interface GUI em `Tkinter` e fallback CLI
- Testes automatizados de regras de negocio

## Tecnologias Utilizadas

- Python 3.11+
- Pandas
- OpenPyXL (engine de escrita Excel)
- Pytest (testes)
- GitHub Actions (CI)
- CodeQL (code scanning)

## Estrutura de Pastas

```text
.
|-- extrator.py
|-- interface.py
|-- regras_config.json
|-- regras/
|   |-- tecnicos_regras.json
|   |-- categorias.json
|   |-- status.json
|   |-- km.json
|   |-- endereco.json
|   |-- qualidade.json
|   |-- filtros.json
|   `-- README.txt
|-- tests/
|   `-- test_regras_negocio.py
|-- .github/
|   |-- dependabot.yml
|   |-- pull_request_template.md
|   `-- workflows/
|       |-- ci.yml
|       `-- codeql.yml
|-- docs/
|   `-- security/
|       `-- github_security_setup.md
|-- requirements.txt
|-- requirements-dev.txt
|-- SECURITY.md
|-- CONTRIBUTING.md
|-- CODE_OF_CONDUCT.md
|-- CITATION.cff
`-- LICENSE
```

## Pre-requisitos

- Python 3.11 ou superior
- `pip` atualizado
- Internet (opcional, quando as regras de rota/geocodificacao forem acionadas)
- `Tkinter` para modo grafico (opcional; sem ele o sistema usa CLI)

## Instalacao

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Para ambiente de desenvolvimento (com testes):

```powershell
pip install -r requirements-dev.txt
```

## Como Executar

Modo GUI (preferencial):

```powershell
python interface.py
```

Modo programatico (exemplo):

```python
from extrator import gerar_excel

resumo = gerar_excel(
    arquivos=["entrada_01.txt", "entrada_02.txt"],
    saida="saida.xlsx",
    data_inicio="01/03/2026",
    data_fim="31/03/2026",
    filtro_tecnico="ALAN",
    filtro_status="RESOLVIDO",
    filtro_cidade="SAO PAULO",
    somente_inconsistencias=False,
)

print(resumo)
```

## Como Testar

```powershell
python -m pytest -q
```

## Como Contribuir

Fluxo recomendado para colaboradores frequentes: **branching interno (sem fork)**.

- `main`: producao
- `develop`: integracao
- `feature/*`: novas funcionalidades
- `fix/*`: correcao de bugs
- `hotfix/*`: correcao urgente em producao

Detalhes completos em [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Exemplos de Uso

Exemplos reais de plano tecnico e validacoes estao documentados nestes arquivos:

- `roadmap_impl_validacao_km_retorno.md`
- `jira_card_00_story_principal_validacao_km_retorno.md`
- `pr_texto_validacao_km_retorno.md`

## Seguranca

Politicas de seguranca e reporte de vulnerabilidades:

- [`SECURITY.md`](SECURITY.md)
- [`docs/security/github_security_setup.md`](docs/security/github_security_setup.md)

Arquivos grandes e Git LFS:

- [`docs/governance/git_lfs_guideline.md`](docs/governance/git_lfs_guideline.md)

## Suporte / Contato

- Abra uma Issue no repositorio: <https://github.com/RaphaelGMoreira/Projeto_Extrator_Rat_Tecnico_txt/issues>
- Para vulnerabilidades, use o fluxo privado descrito em `SECURITY.md`

## Licenca

Este projeto esta licenciado sob a Licenca MIT. Veja [`LICENSE`](LICENSE).

## Suposicoes Declaradas

- Como nao havia metadados de empacotamento no projeto, foi adotado setup simples via `requirements*.txt`.
- A licenca escolhida foi MIT (pode ser alterada se voce preferir outra politica juridica).
