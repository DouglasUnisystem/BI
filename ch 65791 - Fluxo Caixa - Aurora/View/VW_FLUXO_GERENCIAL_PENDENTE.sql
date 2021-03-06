CREATE MATERIALIZED VIEW VW_FLUXO_GERENCIAL_PENDENTE REFRESH FORCE ON DEMAND START WITH SYSDATE NEXT SYSDATE + 10/24/60 AS
SELECT DISTINCT TRIM(RPAD(SUBSTR(PC.Cod_Conta,1,PC.TAMANHO),8,' ')) AS Cod_Conta,
               PC.NOME_CONTA,
               PC.DEBITO_CREDITO,
               PR.DESC_FIN_HIST,
               PR.DATA_LCTO,
               PR.COD_EMPRESA,
               PR.COD_FILIAL,
               PR.COD_SAFRA,
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
               PR.TOTAL_ANO

          FROM AGRICOLA.GER_PLANO_CONTAS PC,
               (SELECT GM.COD_TABELA,TRIM(SUBSTR(GM.Cod_Conta,1,8)) AS COD_CONTA,
                       GM.COD_GRUPO_CONTA,
                       FH.DESC_FIN_HIST,
                       TRUNC(GM.Data_Mvto) DATA_LCTO,
                       GM.COD_EMPRESA,
                       GM.COD_FILIAL,
                       GM.COD_SAFRA,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'01'  ,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL),0)) as Total_Jan,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'02'  ,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL),0)) as Total_Fev,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'03'  ,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL),0)) as Total_Mar,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'04'  ,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL),0)) as Total_Abr,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'05'  ,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL),0)) as Total_Mai,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'06'  ,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL),0)) as Total_Jun,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'07'  ,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL),0)) as Total_Jul,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'08'  ,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL),0)) as Total_Ago,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'09'  ,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL),0)) as Total_Set,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'10'  ,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL),0)) as Total_Out,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'11'  ,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL),0)) as Total_Nov,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'12'  ,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL),0)) as Total_Dez,
                       Sum(DECODE(TO_CHAR(GM.Data_Mvto,'MM'),'00',0,DECODE(GM.Seq_Pla_Pagar  ,NULL,DECODE(GM.Seq_Pla_Mov_Fin,NULL,DECODE(GM.FORM_LANCAMENTO,'DFMSAIDAFT',DECODE(PC.DEBITO_CREDITO,'D',-GM.VALOR_REAL,GM.VALOR_REAL),GM.VALOR_REAL),DECODE(FH.Deb_Cred,'D',-GM.VALOR_REAL,DECODE(GM.DEBITO_CREDITO,'D',DECODE(NVL(FM.VALOR_ANTECIPACAO,0),0,GM.VALOR_REAL,-GM.VALOR_REAL),GM.VALOR_REAL))),-GM.VALOR_REAL  ))) as Total_Ano
                  From AGRICOLA.Ger_Movimento    GM,
                       AGRICOLA.Fin_Movimento    FM,
                       AGRICOLA.Fin_Historicos   FH,
                       AGRICOLA.Fazendas         FZ,
                       AGRICOLA.Ger_Plano_Contas PC,
                       AGRICOLA.PAR_GERAL        PG
                 Where GM.Cod_Tabela = PC.Cod_Tabela
                   And GM.Cod_Conta  = PC.Cod_Conta
                   And GM.Seq_Pla_Fazenda = FZ.Seq_Pla_Fazenda(+)
                   --And TRUNC(GM.Data_Mvto) >= TO_DATE('01/01/2020')
                   --And TRUNC(GM.Data_Mvto) <= TO_DATE('31/12/2020')
                   And GM.Seq_Pla_Mov_Fin  = FM.Seq_Pla_Mov_Fin(+)
                   And FM.Seq_Pla_Fin_Hist = FH.Seq_Pla_Fin_Hist(+)
                   AND DECODE(GM.FORM_LANCAMENTO,NULL,'XXXX',UPPER(GM.FORM_LANCAMENTO)) <> UPPER('dfmBaixaReceber')
                   And DECODE(GM.FORM_LANCAMENTO,NULL,'XXXX',UPPER(GM.FORM_LANCAMENTO)) <> UPPER('dfmBaixaPagar5')
                   And DECODE(GM.FORM_LANCAMENTO,NULL,'XXXX',UPPER(GM.FORM_LANCAMENTO)) <> UPPER('dfmBaixaPagar1')
                   AND DECODE(GM.FORM_LANCAMENTO,NULL,'XXXX',UPPER(GM.FORM_LANCAMENTO)) <> UPPER('dfmBaixaAutPagar')
                   AND DECODE(GM.FORM_LANCAMENTO,NULL,'XXXX',UPPER(GM.FORM_LANCAMENTO)) <> UPPER('dfmBaixaAutReceber')
                   And DECODE(FM.CANCELADO,NULL,'N',FM.CANCELADO)='N'
                 GROUP BY GM.COD_TABELA,SUBSTR(GM.Cod_Conta,1,8), GM.COD_GRUPO_CONTA, FH.DESC_FIN_HIST, TRUNC(GM.Data_Mvto), GM.COD_EMPRESA, GM.COD_FILIAL, GM.COD_SAFRA
               ) PR
         WHERE PC.COD_TABELA                     = PR.COD_TABELA
           AND SUBSTR(PC.Cod_Conta,1,PC.TAMANHO) = PR.Cod_Conta
           AND (ABS(NVL(PR.TOTAL_JAN,0)) + ABS(NVL(PR.TOTAL_FEV,0)) + ABS(NVL(PR.TOTAL_MAR,0)) + ABS(NVL(PR.TOTAL_ABR,0)) + ABS(NVL(PR.TOTAL_MAI,0)) + ABS(NVL(PR.TOTAL_JUN,0))+
                ABS(NVL(PR.TOTAL_JUL,0)) + ABS(NVL(PR.TOTAL_AGO,0)) + ABS(NVL(PR.TOTAL_SET,0)) + ABS(NVL(PR.TOTAL_OUT,0)) + ABS(NVL(PR.TOTAL_NOV,0)) + ABS(NVL(PR.TOTAL_DEZ,0))) <> 0
           --AND PC.COD_TABELA = 2
           And PC.Grau <= '5';
