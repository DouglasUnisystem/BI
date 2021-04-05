CREATE MATERIALIZED VIEW VW_CONS_RETROSP_PR_ORCADO REFRESH FORCE ON DEMAND START WITH TO_DATE('10-02-2021 11:44:56', 'DD-MM-YYYY HH24:MI:SS') NEXT SYSDATE + 10/24/60 AS
(SELECT GM.COD_TABELA,SUBSTR(GM.Cod_Conta,1,PC1.TAMANHO) AS COD_CONTA, GM.Cod_Empresa, GM.Cod_Safra,
                       SUM(NVL(AGRICOLA.ConverteValor(GM.VALOR_JANEIRO,GM.MOEDA_JANEIRO,'','','') ,0)) AS TOTAL_JAN,
                       SUM(NVL(AGRICOLA.ConverteValor(GM.VALOR_FEVEREIRO,GM.MOEDA_FEVEREIRO,'','','') ,0)) AS TOTAL_FEV,
                       SUM(NVL(AGRICOLA.ConverteValor(GM.VALOR_MARCO,GM.MOEDA_MARCO,'','','') ,0)) AS TOTAL_MAR,
                       SUM(NVL(AGRICOLA.ConverteValor(GM.VALOR_ABRIL,GM.MOEDA_ABRIL,'','','') ,0)) AS TOTAL_ABR,
                       SUM(NVL(AGRICOLA.ConverteValor(GM.VALOR_MAIO,GM.MOEDA_MAIO,'','','') ,0)) AS TOTAL_MAI,
                       SUM(NVL(AGRICOLA.ConverteValor(GM.VALOR_JUNHO,GM.MOEDA_JUNHO,'','','') ,0)) AS TOTAL_JUN,
                       SUM(NVL(AGRICOLA.ConverteValor(GM.VALOR_JULHO,GM.MOEDA_JULHO,'','','') ,0)) AS TOTAL_JUL,
                       SUM(NVL(AGRICOLA.ConverteValor(GM.VALOR_AGOSTO,GM.MOEDA_AGOSTO,'','','') ,0)) AS TOTAL_AGO,
                       SUM(NVL(AGRICOLA.ConverteValor(GM.VALOR_SETEMBRO,GM.MOEDA_SETEMBRO,'','','') ,0)) AS TOTAL_SET,
                       SUM(NVL(AGRICOLA.ConverteValor(GM.VALOR_OUTUBRO,GM.MOEDA_OUTUBRO,'','','') ,0)) AS TOTAL_OUT,
                       SUM(NVL(AGRICOLA.ConverteValor(GM.VALOR_NOVEMBRO,GM.MOEDA_NOVEMBRO,'','','') ,0)) AS TOTAL_NOV,
                       SUM(NVL(AGRICOLA.ConverteValor(GM.VALOR_DEZEMBRO,GM.MOEDA_DEZEMBRO,'','','') ,0)) AS TOTAL_DEZ,
                       SUM(NVL(GM.VALOR_JANEIRO ,0)+NVL(GM.VALOR_FEVEREIRO,0)+
                           NVL(GM.VALOR_MARCO   ,0)+NVL(GM.VALOR_ABRIL    ,0)+
                           NVL(GM.VALOR_MAIO    ,0)+NVL(GM.VALOR_JUNHO    ,0)+
                           NVL(GM.VALOR_JULHO   ,0)+NVL(GM.VALOR_AGOSTO   ,0)+
                           NVL(GM.VALOR_SETEMBRO,0)+NVL(GM.VALOR_OUTUBRO  ,0)+
                           NVL(GM.VALOR_NOVEMBRO,0)+NVL(GM.VALOR_DEZEMBRO ,0)
                          ) AS TOTAL_ANO
                  FROM AGRICOLA.GER_PREVISAO_CONTAS GM,
                       AGRICOLA.Ger_Plano_Contas    PC,
                       AGRICOLA.Ger_Plano_Contas    PC1
                 WHERE GM.COD_TABELA = PC.COD_TABELA
                   AND GM.Cod_Conta  = PC.Cod_Conta
                   AND GM.COD_TABELA = PC1.COD_TABELA
                   AND PC.GRAU = '5'
                   AND SUBSTR(GM.Cod_Conta,1,PC1.TAMANHO) = SUBSTR(PC1.Cod_Conta,1,PC1.TAMANHO)
                 GROUP BY GM.COD_TABELA,SUBSTR(GM.Cod_Conta,1,PC1.TAMANHO), GM.Cod_Empresa, GM.Cod_Safra
               );
