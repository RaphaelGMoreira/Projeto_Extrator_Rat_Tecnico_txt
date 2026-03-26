# [TITULO]
VALIDACAO AVANCADA DE KM POR RAT + SELECAO CORRETA DO ULTIMO CHAMADO PARA SCRIPT DE RETORNO

## [TIPO]
Story

## [CONTEXTO]
Hoje existem inconsistencias de KM (ex.: digitacao incorreta gerando KM percorrido muito alto) e casos em que o Script de Retorno nao usa o ultimo chamado real do tecnico no dia.
Tambem houve erro de rota (ex.: 404 NOT_FOUND), entao precisamos de tratamento robusto sem travar o processamento.

## [OBJETIVO]
Aumentar a confiabilidade dos dados de deslocamento e garantir que o Script de Retorno seja gerado com base no ultimo atendimento valido do dia.

## [ESCOPO]
1. Calcular km_percorrido = km_final - km_inicial.
2. Bloquear RAT com km_percorrido <= 0.
3. Calcular km_dia = ultimo_km_final_do_dia - primeiro_km_inicial_do_dia.
4. Se km_dia <= 150, aprovar dia sem validacao extra.
5. Se km_dia > 150, validar por API de rotas (origem/destino por RAT).
6. Aplicar tolerancia: max(10 km, 20% da distancia da rota).
7. Fora da tolerancia: marcar como PENDENTE REVISAO e gerar AJUSTE_SUGERIDO (nao aplicar automatico).
8. Ajuste sugerido deve manter continuidade entre RATs do dia.
9. Implementar trilha de auditoria para ajustes (original, sugerido, motivo, aprovador, data/hora).
10. Selecionar ultimo chamado do dia para Script de Retorno por ordenacao:
termino DESC, inicio DESC, chamado DESC (numerico), created_at DESC.
11. Fixar timezone America/Sao_Paulo para ordenacao temporal.

## [FORA DE ESCOPO]
1. Alteracao visual avancada da interface alem de status/alertas de validacao.
2. Correcao automatica sem aprovacao humana.

## [CRITERIOS DE ACEITE]
1. Sistema bloqueia km_percorrido <= 0.
2. Sistema calcula km_dia e aplica limite de 150 km.
3. Acima de 150 km, sistema valida rota por endereco inicio/fim.
4. Divergencia fora da tolerancia gera ajuste sugerido.
5. Ajuste exige aprovacao (nao automatico).
6. Auditoria registra valor original, sugerido, aprovador e timestamp.
7. Script de retorno usa o ultimo chamado pela regra de ordenacao definida.
8. Ordenacao respeita timezone America/Sao_Paulo.
9. Falha da API de rota (ex.: 404/timeout) nao quebra o processamento; registro fica PENDENTE REVISAO com motivo tecnico.

## [DETALHE TECNICO]
1. Persistir RATs por tecnico_id + data_atendimento para calculo diario e ordenacao consistente.
2. Guardar campos de validacao:
km_percorrido, km_dia, distancia_rota_km, tolerancia_km, status_validacao_km, motivo_validacao.
3. Guardar auditoria de ajuste:
campo, valor_original, valor_sugerido, motivo, aprovado_por, aprovado_em.
4. Aplicar deteccao de outlier por mediana do dia:
sinalizar quando km_rat > 2.5x mediana e diferenca minima >= 20 km.

## [TAREFAS]
1. Criar camada de persistencia para RATs e auditoria.
2. Implementar motor de validacao de KM (rat, dia, rota, tolerancia, outlier).
3. Integrar cliente de rota com tratamento de erro (404/timeout/rate limit).
4. Implementar status PENDENTE REVISAO e AJUSTE_SUGERIDO.
5. Ajustar algoritmo do Script de Retorno para ultimo chamado do dia.
6. Cobrir regras com testes automatizados.
7. Expor no relatorio/Excel as colunas de validacao e auditoria.

## [DEFINITION OF DONE]
1. Criterios de aceite 100% atendidos.
2. Testes automatizados passando.
3. Logs e auditoria disponiveis para consulta.
4. Documentacao tecnica e de operacao atualizada.
