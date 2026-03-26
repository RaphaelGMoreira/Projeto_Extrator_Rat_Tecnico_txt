# Security Policy

## Versoes Suportadas

| Versao | Suporte de seguranca |
|---|---|
| `main` (mais recente) | Sim |
| branches antigas sem manutencao | Nao |

## Como Reportar Vulnerabilidades

Use um canal privado.

Recomendado (GitHub):

1. Aba `Security` do repositorio
2. `Report a vulnerability` (Private vulnerability reporting)

Alternativa:

- Abra contato direto com o mantenedor do repositorio e descreva o problema de forma privada.

Nao publique detalhes sensiveis em issues abertas.

## O Que Informar no Reporte

- Tipo de vulnerabilidade
- Impacto potencial
- Passos para reproducao
- Evidencias (logs, payload, cenarios)
- Sugestao de mitigacao (se tiver)

## Compromisso de Resposta (Meta)

- Confirmacao de recebimento: ate 3 dias uteis
- Triagem inicial: ate 7 dias uteis
- Plano de correcao: conforme severidade e risco operacional

## Recomendacoes de Hardening no GitHub

Ativar obrigatoriamente:

- Dependabot alerts
- Dependabot security updates
- Secret scanning
- Push protection
- Code scanning (CodeQL)
- Private vulnerability reporting

Guia pratico: [`docs/security/github_security_setup.md`](docs/security/github_security_setup.md)

## Higiene de Dados

Este repositorio pode lidar com dados operacionais e pessoais em arquivos `.txt`/`.xlsx`.

Boas praticas:

- Nao versionar planilhas geradas e dumps operacionais
- Nao commitar conversas brutas com dados pessoais
- Usar dados anonimizados para testes e exemplos
- Revisar PRs para evitar vazamento de PII e segredos
