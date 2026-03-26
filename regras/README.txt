Arquivos de regras de negócio (modular):
- tecnicos_regras.json: lista de regras de técnico.
- categorias.json: ordem, palavras-chave e categoria padrão.
- status.json: classificação de STATUS e DESCRIÇÃO DO CHAMADO.
- km.json: normalização de KM e regras sequenciais de KM.
- endereco.json: regras de máscara para ENDEREÇO.
- qualidade.json: validações críticas antes da exportação.
- filtros.json: comportamento de filtros e inconsistências.

Precedência de carga:
1) DEFAULT interno
2) regras_config.json (legado)
3) pasta regras/ (modular, prioridade máxima)
