# [CARD 1] BACKEND - PERSISTENCIA E AUDITORIA

## Objetivo
Criar base persistente para armazenar RATs do dia, validacoes de KM e trilha de auditoria.

## Escopo
1. Criar entidades/tabelas para RAT, validacao_km e auditoria_ajustes.
2. Persistir tecnico_id, data_atendimento, inicio, termino, chamado, km_inicial, km_final, enderecos.
3. Persistir status_validacao_km e motivo_validacao.
4. Persistir trilha de ajustes: valor original/sugerido, aprovador, timestamp.

## Criterios de aceite
1. Dados de RAT ficam consultaveis por tecnico + data.
2. Auditoria de ajuste fica rastreavel por registro.
3. Endpoint/servico retorna historico diario sem depender de formulario aberto.

## Estimativa
5 pontos
