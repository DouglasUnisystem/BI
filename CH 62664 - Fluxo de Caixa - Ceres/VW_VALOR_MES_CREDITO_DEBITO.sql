CREATE OR REPLACE VIEW VW_VALOR_MES_CREDITO_DEBITO AS
SELECT SUM(VALOR_DEBITO) AS VALOR_DEBITO,
       SUM(VALOR_CREDITO) AS VALOR_CREDITO,
       MES,
       MES_ANO,
       ID_MES,
       COD_EMPRESA,
       COD_SAFRA,
       MES_NUM,
       DATA
  FROM (SELECT 0 AS VALOR_DEBITO,
               VC.VALOR_CREDITO,
               VC.MES,
               VC.MES_ANO,
               VC.ID_MES,
               VC.COD_EMPRESA,
               VC.COD_SAFRA,
               VC.MES_NUM,
               DATA
          FROM VW_VALOR_MES_CREDITO_FUTURO VC
        UNION
        SELECT VD.VALOR_DEBITO,
               0 AS VALOR_CREDITO,
               VD.MES,
               VD.MES_ANO,
               VD.ID_MES,
               VD.COD_EMPRESA,
               VD.COD_SAFRA,
               VD.MES_NUM,
               DATA
          FROM VW_VALOR_MES_DEBITO_FUTURO VD
        )
 GROUP BY MES, MES_ANO, ID_MES, COD_EMPRESA, COD_SAFRA, MES_NUM, DATA
 ORDER BY MES_ANO;
