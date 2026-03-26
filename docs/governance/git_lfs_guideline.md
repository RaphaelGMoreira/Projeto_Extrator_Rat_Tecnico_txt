# Git LFS Guideline

## Avaliacao Atual

- Os maiores arquivos observados estao em `Planilhas geradas/` e ficam na faixa de poucos MB.
- Hoje, o maior ganho vem de **nao versionar artefatos gerados**.

## Quando Usar Git LFS

Adote Git LFS se for necessario manter no Git arquivos binarios grandes e historicos frequentes.

Candidatos comuns:

- `*.xlsx`
- `*.xls`
- `*.csv` muito grandes (quando historico crescer)
- dumps de dados anonimizados acima de 10-20 MB

## Estrategia Recomendada

1. Manter saídas operacionais fora do Git (`Planilhas geradas/` ja ignorado).
2. Se precisar versionar amostras binarias, usar pasta dedicada `samples/`.
3. Ativar LFS apenas para essa pasta/tipos, evitando custo desnecessario.

## Exemplo de Setup

```bash
git lfs install
git lfs track "*.xlsx"
git add .gitattributes
git add samples/
git commit -m "chore: habilita git lfs para planilhas de exemplo"
```
