CREATE OR REPLACE FUNCTION FRETORNA_SALDO_INICIAL_FLUXO (MCOD_EMPRESA CHAR:='0', MCOD_FILIAL CHAR:='0', MDATAB DATE, MDATAF DATE) RETURN NUMBER IS
  mResultado NUMBER;
BEGIN
  SELECT SALDO_ATUAL INTO mResultado
    FROM (SELECT SUM(ROUND(ROUND(NVL(SC.SALDO_CONTA,0),2) - ROUND(NVL(MV.DEBITO_ANTERIOR,0),2) + ROUND(NVL(MV.CREDITO_ANTERIOR,0),2) - ROUND(NVL(MV.VALOR_DEBITO,0),2) + ROUND(NVL(MV.VALOR_CREDITO,0),2),2)) AS SALDO_ATUAL                     
            FROM GUSTAVO.CTB_PLANO_CONTAS PC,
                (SELECT CT.CTB_TABELA,
                        CT.CTB_CONTA,
                        SUM(DECODE(HI.DEB_CRED,'D',DECODE(SIGN(TRUNC(FI.DATA_DOCUM)-TO_DATE(MDATAB, 'DD/MM/YYYY')),-1,ROUND(FI.VALOR_REAL,2) + ROUND(DECODE(FI.JUROS_CPAGAR,'S',0,DECODE(FI.VALOR_JUROS,NULL,0,FI.VALOR_JUROS)-DECODE(FI.VALOR_ANTECIPACAO,NULL,0,FI.VALOR_ANTECIPACAO)),2),0),0)) AS DEBITO_ANTERIOR,                       
                        SUM(DECODE(HI.DEB_CRED,'C',DECODE(SIGN(TRUNC(FI.DATA_DOCUM)-TO_DATE(MDATAB, 'DD/MM/YYYY')),-1,ROUND(FI.VALOR_REAL,2) + ROUND(DECODE(FI.JUROS_CPAGAR,'S',0,DECODE(FI.VALOR_JUROS,NULL,0,FI.VALOR_JUROS)-DECODE(FI.VALOR_ANTECIPACAO,NULL,0,FI.VALOR_ANTECIPACAO)),2),0),0)) AS CREDITO_ANTERIOR,                        
                        SUM(DECODE(HI.DEB_CRED,'D',DECODE(SIGN(TRUNC(FI.DATA_DOCUM)-TO_DATE(MDATAB, 'DD/MM/YYYY')),-1,0,DECODE(SIGN(TO_DATE(TRUNC(FI.DATA_DOCUM), 'DD/MM/YYYY') - TO_DATE(TRUNC(MDATAF), 'DD/MM/YYYY')),1,0,ROUND(FI.VALOR_REAL,2) + ROUND(DECODE(FI.JUROS_CPAGAR,'S',0,DECODE(FI.VALOR_JUROS,NULL,0,FI.VALOR_JUROS)-DECODE(FI.VALOR_ANTECIPACAO,NULL,0,FI.VALOR_ANTECIPACAO)),2))),0)) AS VALOR_DEBITO,                       
                        SUM(DECODE(HI.DEB_CRED,'C',DECODE(SIGN(TRUNC(FI.DATA_DOCUM)-TO_DATE(MDATAB, 'DD/MM/YYYY')),-1,0,DECODE(SIGN(TO_DATE(TRUNC(FI.DATA_DOCUM), 'DD/MM/YYYY') - TO_DATE(TRUNC(MDATAF), 'DD/MM/YYYY')),1,0,ROUND(FI.VALOR_REAL,2) + ROUND(DECODE(FI.JUROS_CPAGAR,'S',0,DECODE(FI.VALOR_JUROS,NULL,0,FI.VALOR_JUROS)-DECODE(FI.VALOR_ANTECIPACAO,NULL,0,FI.VALOR_ANTECIPACAO)),2))),0)) AS VALOR_CREDITO
                   FROM GUSTAVO.FIN_MOVIMENTO  FI,
                        GUSTAVO.FIN_HISTORICOS HI,
                        GUSTAVO.FIN_CONTAS     CT
                  WHERE FI.SEQ_PLA_FIN_HIST     = HI.SEQ_PLA_FIN_HIST
                    AND FI.SEQ_PLA_FIN_CONTA    = CT.SEQ_PLA_FIN_CONTA
                    AND NVL(FI.Cancelado,'N') = 'N'
                    AND ((TO_CHAR(FI.COD_EMPRESA) = MCOD_EMPRESA ) or ('0' = MCOD_EMPRESA ))
                    AND ((TO_CHAR(FI.COD_FILIAL)  = MCOD_FILIAL  ) or ('0' = MCOD_FILIAL  ))
                    AND TO_DATE(TRUNC(FI.DATA_DOCUM),'DD/MM/YYYY') <= TO_DATE(MDATAF, 'DD/MM/YYYY')
                    --24/12/2020 pedido do Uilerson
                    --AND CT.TIPO_CONTA  IN ('B','C','T','G','E')
                    AND CT.TIPO_CONTA  IN ('B')
                    --
                  GROUP BY CT.CTB_TABELA,CT.CTB_CONTA
                )MV,
                (SELECT CTB_TABELA,
                        CTB_CONTA,
                        SUM(NVL(SALDO_FINANCEIRO,0)) AS SALDO_CONTA
                   FROM GUSTAVO.CTB_SALDO_CONTAS
                  WHERE 1 = 1 --TRUNC(DATA) = '01/01/1990'
                    AND CTB_CONTA>99999
                    AND ((TO_CHAR(COD_EMPRESA) = MCOD_EMPRESA ) or ('0' = MCOD_EMPRESA ))
                    AND ((TO_CHAR(COD_FILIAL)  = MCOD_FILIAL  ) or ('0' = MCOD_FILIAL  ))                   
                  GROUP BY CTB_TABELA,CTB_CONTA
                ) SC
          WHERE PC.GRAU       = '5'
            AND PC.CTB_TABELA = MV.CTB_TABELA(+)
            AND PC.CTB_CONTA  = MV.CTB_CONTA (+)
            AND PC.CTB_TABELA = SC.CTB_TABELA(+)
            AND PC.CTB_CONTA  = SC.CTB_CONTA (+)
            AND (PC.CTB_TABELA,PC.CTB_CONTA) IN (SELECT CTB_TABELA,CTB_CONTA
                                                   FROM GUSTAVO.FIN_CONTAS
                                                --24/12/2020 pedido do Uilerson
                                                --WHERE TIPO_CONTA IN ('B','C','T','G','E')
                                                  WHERE TIPO_CONTA IN ('B')
			                        --
                                                 )
            AND (PC.CTB_TABELA,PC.CTB_CONTA) IN (SELECT B.CTB_TABELA,B.CTB_CONTA
                                                   FROM GUSTAVO.USUARIO_CONTAS A,
                                                        GUSTAVO.FIN_CONTAS     B
                                                  WHERE A.SEQ_PLA_FIN_CONTA = B.SEQ_PLA_FIN_CONTA
                                                  --24/12/2020 pedido do Uilerson
                                                  --AND B.TIPO_CONTA IN ('B','C','T','G','E')
                                                    AND B.TIPO_CONTA IN ('B')
			                          --
                                               )
         );
  RETURN mResultado;
END;
/
