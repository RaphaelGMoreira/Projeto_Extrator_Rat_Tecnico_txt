# [CARD 2] MOTOR DE VALIDACAO KM + ROTA + OUTLIER

## Objetivo
Implementar regras de KM com tolerancia por rota e deteccao estatistica de outlier.

## Escopo
1. Regra RAT: km_percorrido = km_final - km_inicial.
2. Bloqueio: km_percorrido <= 0.
3. Regra diaria: km_dia = ultimo_km_final_do_dia - primeiro_km_inicial_do_dia.
4. Ate 150 km/dia: aprovar automatico.
5. Acima de 150 km: validar por API de rota.
6. Tolerancia: max(10 km, 20% rota).
7. Fora da tolerancia: PENDENTE REVISAO + AJUSTE_SUGERIDO.
8. Outlier por mediana: sinalizar quando km_rat > 2.5x mediana e diff >= 20 km.
9. Falha API (404/timeout): nao travar fluxo, registrar pendencia tecnica.

## Criterios de aceite
1. Todas as regras acima aplicadas e testadas.
2. Nenhum ajuste automatico sem aprovacao.
3. Logs de validacao por RAT disponiveis em relatorio.

## Estimativa
8 pontos
