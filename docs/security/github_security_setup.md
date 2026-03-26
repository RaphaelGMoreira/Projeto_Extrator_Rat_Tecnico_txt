# GitHub Security Setup

Este guia descreve configuracoes recomendadas no repositorio para reduzir risco de supply chain, vazamento de segredos e regressao de seguranca.

## 1) Dependabot Alerts

No GitHub:

1. `Settings` -> `Security` -> `Code security and analysis`
2. Ative `Dependency graph`
3. Ative `Dependabot alerts`
4. Ative `Dependabot security updates`

Arquivo relacionado:

- `.github/dependabot.yml`

## 2) Secret Scanning

No GitHub:

1. `Settings` -> `Security` -> `Code security and analysis`
2. Ative `Secret scanning`
3. Ative `Push protection`

Observacao:

- Em repositorios privados, alguns recursos podem exigir plano/licenca GitHub Advanced Security.

## 3) Code Scanning (CodeQL)

No GitHub:

1. `Security` -> `Code scanning`
2. Confirme execucao do workflow `CodeQL`
3. Corrija findings criticos antes de merge em `main`

Arquivo relacionado:

- `.github/workflows/codeql.yml`

## 4) Private Vulnerability Reporting

No GitHub:

1. `Settings` -> `Security` -> `Code security and analysis`
2. Ative `Private vulnerability reporting`

Politica de resposta:

- `SECURITY.md`

## 5) Branch Protection (main/develop)

No GitHub:

1. `Settings` -> `Branches` -> `Add branch protection rule`
2. Criar regra para `main`
3. Criar regra para `develop`

Opcoes recomendadas:

- Require a pull request before merging
- Require approvals (>=1)
- Require review from Code Owners (se houver `CODEOWNERS`)
- Require status checks to pass
- Require branches to be up to date
- Block force pushes
- Do not allow branch deletion

## 6) Pull Request Hygiene

Recomendado:

- Usar `.github/pull_request_template.md`
- Exigir PR pequeno e objetivo
- Validar testes e impacto de seguranca em toda alteracao

## 7) Dados Sensiveis

Boas praticas para este projeto:

- Nao commitar arquivos operacionais reais (`.txt`/`.xlsx`) com dados pessoais
- Manter dados de exemplo anonimizados
- Revisar periodicamente o historico Git para remocao de dados sensiveis antigos, se necessario
