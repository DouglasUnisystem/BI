CREATE OR REPLACE VIEW VW_VALOR_DEBITO_CP AS
SELECT (SUM(NVL(PA.VALOR_PAGO,0)) + SUM(NVL(PA.VALOR_JUROS,0))) - (SUM(NVL(PA.VALOR_DESCONTO,0))) AS VALOR_DEBITO,
       PA.SEQ_PLA_PAGAR,
       PA.DATA_PGTO,
       TRUNC(PA.DATA_PGTO) AS DATA
  FROM AGRICOLA.PAGAR_PAGTOS PA
 GROUP BY PA.SEQ_PLA_PAGAR, PA.DATA_PGTO;
