# Contribuindo com o Projeto

Obrigado por contribuir com o `Projeto_Extrator_Rat_Tecnico_txt`.

Este documento define o fluxo de colaboracao recomendado para manter qualidade, previsibilidade e seguranca.

## Fluxo de Branches

Para colaboradores frequentes, priorize fluxo por branch no repositorio principal.

- `main`: branch estavel/producao
- `develop`: integracao continua
- `feature/*`: novas funcionalidades
- `fix/*`: correcoes de bug
- `hotfix/*`: correcoes urgentes em producao

Exemplos:

- `feature/validacao-km-diaria`
- `fix/normalizacao-hora`
- `hotfix/exportacao-log`

## Padrao de Commits

Use convencao inspirada em Conventional Commits:

- `feat: adiciona validacao de km por rota`
- `fix: corrige ordenacao do ultimo chamado`
- `docs: atualiza README com fluxo de testes`
- `test: adiciona casos para parse de horario`
- `chore: atualiza dependencias de desenvolvimento`

Regras praticas:

- Commits pequenos e focados
- Mensagens objetivas no imperativo
- Evite misturar refactor amplo com correcoes funcionais

## Ambiente Local

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

## Validacoes Antes do PR

Execute no minimo:

```powershell
python -m pytest -q
```

Se adicionar novas regras de negocio:

- Inclua testes de regressao em `tests/`
- Atualize documentacao quando houver mudanca de comportamento

## Processo de Pull Request

1. Crie sua branch a partir de `develop` (ou `main` para `hotfix/*`).
2. Implemente alteracoes com testes e docs.
3. Abra PR para `develop` (ou para `main` em hotfix).
4. Preencha o template de PR.
5. Aguarde revisao e status checks.

## Politica de Revisao

- Minimo de 1 aprovacao para merge
- Sem auto-merge em PR com checks quebrados
- Rebase/merge final somente com branch atualizada

## Branch Protection (Recomendado no GitHub)

Aplicar em `main` e `develop`:

- Require a pull request before merging
- Require approvals: 1 ou mais
- Require status checks to pass
- Require branches to be up to date
- Block force pushes
- Restrict deletion

## Escopo de Mudancas

Para facilitar manutencao:

- Mudancas funcionais: 1 PR por tema
- Mudancas de documentacao: separar quando possivel
- Evite PRs gigantes sem necessidade

## Relato de Bugs e Sugestoes

- Bugs: abrir Issue com passo a passo, comportamento atual e esperado
- Melhoria: abrir Issue com contexto, beneficio e possivel abordagem

## Seguranca

Nao abra vulnerabilidade em issue publica.

Siga o fluxo de `SECURITY.md`.
