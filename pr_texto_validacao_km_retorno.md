## Contexto
Este PR corrige inconsistencias de KM e a escolha incorreta do ultimo chamado para geracao do Script de Retorno.

## Problema
- Casos de digitacao geravam KM PERCORRIDO fora da realidade.
- Em alguns dias, o Script de Retorno nao era baseado no ultimo chamado real do tecnico.
- Dependencia de API de rota sem tratamento robusto (ex.: 404 NOT_FOUND) podia comprometer a validacao.

## O que foi implementado
1. Regra base por RAT:
   - km_percorrido = km_final - km_inicial
   - bloqueio quando km_percorrido <= 0
2. Regra diaria:
   - km_dia = ultimo_km_final_do_dia - primeiro_km_inicial_do_dia
   - aprovacao automatica se km_dia <= 150
3. Validacao por rota para km_dia > 150:
   - calculo de distancia por API de rotas
   - tolerancia max(10km, 20%)
4. Status e seguranca operacional:
   - fora da tolerancia => PENDENTE REVISAO
   - geracao de AJUSTE_SUGERIDO sem autoaplicacao
5. Auditoria:
   - registro de original/sugerido/motivo/aprovador/timestamp
6. Script de Retorno:
   - ultimo chamado definido por ordenacao:
     termino DESC, inicio DESC, chamado DESC, created_at DESC
   - timezone fixo: America/Sao_Paulo
7. Resiliencia de integracao:
   - erros da API de rota (404/timeout) nao interrompem processamento
   - registro segue para revisao com motivo tecnico

## Como validar
1. Executar processamento com dataset contendo:
   - km_percorrido <= 0
   - dia com km_dia <= 150
   - dia com km_dia > 150 e divergencia de rota
2. Confirmar:
   - bloqueios e pendencias corretas
   - ajuste sugerido sem aplicacao automatica
   - auditoria preenchida
   - script de retorno usando ultimo chamado correto
3. Validar ordenacao com timezone America/Sao_Paulo.

## Impacto
- Maior confiabilidade dos KM reportados
- Menor risco de correcao indevida
- Melhor rastreabilidade/auditoria
- Geracao de retorno mais consistente

## Checklist
- [ ] Regras de KM implementadas
- [ ] Tolerancia de rota aplicada
- [ ] Ajuste sugerido sem autoaplicar
- [ ] Auditoria persistida
- [ ] Ordenacao do ultimo chamado corrigida
- [ ] Timezone fixado
- [ ] Testes automatizados cobrindo cenarios criticos
