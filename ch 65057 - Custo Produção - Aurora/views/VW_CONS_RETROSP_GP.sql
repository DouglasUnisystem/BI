CREATE MATERIALIZED VIEW VW_CONS_RETROSP_GP REFRESH FORCE ON DEMAND START WITH TO_DATE('10-02-2021 11:44:56', 'DD-MM-YYYY HH24:MI:SS') NEXT SYSDATE + 10/24/60 AS
(SELECT '1' AS PR,
                       PR.Cod_Empresa,
                       PR.Cod_Safra,
                       PC.Cod_Tabela,
                       TRIM(RPAD(SUBSTR(PC.Cod_Conta,1,PC.TAMANHO),8,' ')) AS Cod_Conta,
                       '%' AS NOME_CONTA_GRUPO,
                       '%' AS SEQ_PLA_GRUPO_CONTA,
                       PC.NOME_CONTA,
                       PC.DEBITO_CREDITO,
                       PR.TOTAL_JAN,
                       PR.TOTAL_FEV,
                       PR.TOTAL_MAR,
                       PR.TOTAL_ABR,
                       PR.TOTAL_MAI,
                       PR.TOTAL_JUN,
                       PR.TOTAL_JUL,
                       PR.TOTAL_AGO,
                       PR.TOTAL_SET,
                       PR.TOTAL_OUT,
                       PR.TOTAL_NOV,
                       PR.TOTAL_DEZ,
                       (NVL(PR.TOTAL_JAN,0)+NVL(PR.TOTAL_FEV,0)+NVL(PR.TOTAL_MAR,0)+NVL(PR.TOTAL_ABR,0)+NVL(PR.TOTAL_MAI,0)+NVL(PR.TOTAL_JUN,0)+
                        NVL(PR.TOTAL_JUL,0)+NVL(PR.TOTAL_AGO,0)+NVL(PR.TOTAL_SET,0)+NVL(PR.TOTAL_OUT,0)+NVL(PR.TOTAL_NOV,0)+NVL(PR.TOTAL_DEZ,0)) TOTAL_ANO
                  FROM AGRICOLA.GER_PLANO_CONTAS PC,
                       (SELECT GM.COD_TABELA,SUBSTR(GM.Cod_Conta,1,PC1.TAMANHO) AS COD_CONTA,GM.Cod_Empresa,GM.Cod_Safra,
                               SUM(0) as TOTAL_JAN,
                               SUM(0) as TOTAL_FEV,
                               SUM(0) as TOTAL_MAR,
                               SUM(0) as TOTAL_ABR,
                               SUM(0) as TOTAL_MAI,
                               SUM(DECODE(PC.Debito_Credito,'C',NVL(GM.VALOR_JUNHO    ,0),NVL(-GM.VALOR_JUNHO    ,0))) AS TOTAL_JUN,
                               SUM(DECODE(PC.Debito_Credito,'C',NVL(GM.VALOR_JULHO    ,0),NVL(-GM.VALOR_JULHO    ,0))) AS TOTAL_JUL,
                               SUM(DECODE(PC.Debito_Credito,'C',NVL(GM.VALOR_AGOSTO   ,0),NVL(-GM.VALOR_AGOSTO   ,0))) AS TOTAL_AGO,
                               SUM(DECODE(PC.Debito_Credito,'C',NVL(GM.VALOR_SETEMBRO ,0),NVL(-GM.VALOR_SETEMBRO ,0))) AS TOTAL_SET,
                               SUM(DECODE(PC.Debito_Credito,'C',NVL(GM.VALOR_OUTUBRO  ,0),NVL(-GM.VALOR_OUTUBRO  ,0))) AS TOTAL_OUT,
                               SUM(0) as TOTAL_NOV,
                               SUM(0) as TOTAL_DEZ,
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
                         GROUP BY GM.COD_TABELA,SUBSTR(GM.Cod_Conta,1,PC1.TAMANHO),GM.Cod_Empresa,GM.Cod_Safra
                       ) PR
                 WHERE PC.COD_TABELA                     = PR.COD_TABELA
                   AND SUBSTR(PC.Cod_Conta,1,PC.TAMANHO) = PR.Cod_Conta
                   And PC.Grau <= '5'
               );
