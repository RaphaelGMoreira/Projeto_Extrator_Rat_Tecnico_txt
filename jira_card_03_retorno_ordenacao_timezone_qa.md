# [CARD 3] SCRIPT DE RETORNO + ORDENACAO + QA

## Objetivo
Garantir que o Script de Retorno use o ultimo chamado real do dia e validar comportamento fim-a-fim.

## Escopo
1. Filtrar por tecnico_id + data_atendimento.
2. Ordenar por: termino DESC, inicio DESC, chamado DESC (numerico), created_at DESC.
3. Fixar timezone America/Sao_Paulo.
4. Gerar script de retorno com base no primeiro da ordenacao.
5. Implementar testes automatizados de regressao.
6. Validar cenarios sem horario (fallback por created_at).

## Criterios de aceite
1. Retorno sempre usa ultimo chamado correto do dia.
2. Timezone evita inversao de ordem.
3. Testes passam em cenarios com/sem horario.

## Estimativa
3 pontos
