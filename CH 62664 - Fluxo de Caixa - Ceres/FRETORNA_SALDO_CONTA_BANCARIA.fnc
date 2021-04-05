CREATE OR REPLACE FUNCTION FRETORNA_SALDO_CONTA_BANCARIA (MCOD_EMPRESA NUMBER :=0,  MDATA DATE) RETURN NUMBER IS
  mResultado NUMBER;
BEGIN
  SELECT SALDO_ATUAL INTO mResultado
    FROM (SELECT SUM(ROUND(ROUND(NVL(SC.SALDO_CONTA,0),2) - ROUND(NVL(MV.DEBITO_ANTERIOR,0),2) + ROUND(NVL(MV.CREDITO_ANTERIOR,0),2) - ROUND(NVL(MV.VALOR_DEBITO,0),2) + ROUND(NVL(MV.VALOR_CREDITO,0),2),2)) AS SALDO_ATUAL
            FROM CERES2311.CTB_PLANO_CONTAS PC,
                (SELECT CT.CTB_TABELA,
                       CT.CTB_CONTA,
                       SUM(DECODE(HI.DEB_CRED,'D',DECODE(SIGN(TRUNC(FI.DATA_DOCUM)-TO_DATE('01/01/2000')),-1,ROUND(FI.VALOR_REAL,2)+ROUND(DECODE(FI.JUROS_CPAGAR,'S',0,DECODE(FI.VALOR_JUROS,NULL,0,FI.VALOR_JUROS)-DECODE(FI.VALOR_ANTECIPACAO,NULL,0,FI.VALOR_ANTECIPACAO)),2),0),0)) AS DEBITO_ANTERIOR,
                       SUM(DECODE(HI.DEB_CRED,'C',DECODE(SIGN(TRUNC(FI.DATA_DOCUM)-TO_DATE('01/01/2000')),-1,ROUND(FI.VALOR_REAL,2)+ROUND(DECODE(FI.JUROS_CPAGAR,'S',0,DECODE(FI.VALOR_JUROS,NULL,0,FI.VALOR_JUROS)-DECODE(FI.VALOR_ANTECIPACAO,NULL,0,FI.VALOR_ANTECIPACAO)),2),0),0)) AS CREDITO_ANTERIOR,
                       SUM(DECODE(HI.DEB_CRED,'D',DECODE(SIGN(TRUNC(FI.DATA_DOCUM)-TO_DATE('01/01/2000')),-1,0,DECODE(SIGN(TRUNC(FI.DATA_DOCUM)-TO_DATE(MDATA)),1,0,ROUND(FI.VALOR_REAL,2)+ROUND(DECODE(FI.JUROS_CPAGAR,'S',0,DECODE(FI.VALOR_JUROS,NULL,0,FI.VALOR_JUROS)-DECODE(FI.VALOR_ANTECIPACAO,NULL,0,FI.VALOR_ANTECIPACAO)),2))),0)) AS VALOR_DEBITO,
                       SUM(DECODE(HI.DEB_CRED,'C',DECODE(SIGN(TRUNC(FI.DATA_DOCUM)-TO_DATE('01/01/2000')),-1,0,DECODE(SIGN(TRUNC(FI.DATA_DOCUM)-TO_DATE(MDATA)),1,0,ROUND(FI.VALOR_REAL,2)+ROUND(DECODE(FI.JUROS_CPAGAR,'S',0,DECODE(FI.VALOR_JUROS,NULL,0,FI.VALOR_JUROS)-DECODE(FI.VALOR_ANTECIPACAO,NULL,0,FI.VALOR_ANTECIPACAO)),2))),0)) AS VALOR_CREDITO
                  FROM CERES2311.FIN_MOVIMENTO  FI,
                       CERES2311.FIN_HISTORICOS HI,
                       CERES2311.FIN_CONTAS     CT
                 WHERE FI.SEQ_PLA_FIN_HIST     = HI.SEQ_PLA_FIN_HIST
                   AND FI.SEQ_PLA_FIN_CONTA    = CT.SEQ_PLA_FIN_CONTA
                   AND NVL(FI.Cancelado,'N') = 'N'
                   AND (FI.COD_EMPRESA = MCOD_EMPRESA or 0 = MCOD_EMPRESA)
                   AND TRUNC(FI.DATA_DOCUM)   <= MDATA
                   AND CT.TIPO_CONTA = 'B'
                   --AND CT.TIPO_CONTA IN ('C','T','G','B')
                 GROUP BY CT.CTB_TABELA,CT.CTB_CONTA
               )MV,
               (SELECT CTB_TABELA,
                       CTB_CONTA,
                       SUM(NVL(SALDO_FINANCEIRO,0)) AS SALDO_CONTA
                  FROM CERES2311.CTB_SALDO_CONTAS
                 WHERE 1 = 1
                   AND CTB_CONTA>99999
                   AND (COD_EMPRESA = MCOD_EMPRESA or 0 = MCOD_EMPRESA)
                 GROUP BY CTB_TABELA,CTB_CONTA
               ) SC
          WHERE PC.GRAU       = '5'
           AND PC.CTB_TABELA = MV.CTB_TABELA(+)
           AND PC.CTB_CONTA  = MV.CTB_CONTA (+)
           AND PC.CTB_TABELA = SC.CTB_TABELA(+)
           AND PC.CTB_CONTA  = SC.CTB_CONTA (+)
           AND (PC.CTB_TABELA,PC.CTB_CONTA) IN (SELECT CTB_TABELA,CTB_CONTA
                                                  FROM CERES2311.FIN_CONTAS
                                                 WHERE TIPO_CONTA IN ('B','T'))
           AND (PC.CTB_TABELA,PC.CTB_CONTA) IN (SELECT B.CTB_TABELA,B.CTB_CONTA
                                                  FROM CERES2311.USUARIO_CONTAS A,
                                                       CERES2311.FIN_CONTAS     B
                                                 WHERE A.SEQ_PLA_FIN_CONTA = B.SEQ_PLA_FIN_CONTA
                                                   AND B.TIPO_CONTA IN ('B','T')
                                                   --AND B.TIPO_CONTA IN ('C','T','G','B')
                                               )
         );
  RETURN mResultado;
END;
/
