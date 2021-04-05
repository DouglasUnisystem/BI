CREATE MATERIALIZED VIEW GRAFICO_MEDIA_PRODUCAO
REFRESH FORCE ON DEMAND
START WITH SYSDATE  NEXT SYSDATE + 5/24/60 
AS
SELECT EP.COD_EMPRESA,
       EP.NOME_EMPRESA,
       FL.COD_FILIAL,
       FL.DESCRICAO_FILIAL,
       AG.SEQ_PLA_AGLOMERADO,
       AG.DESC_AGLOMERADO,
       SF.COD_SAFRA,
       SF.DESCRICAO_SAFRA,
       TT.SEQ_PLA_FAZENDA,
       TT.SIGLA_FAZENDA,
       TT.DESC_FAZENDA,
       TO_CHAR(AGNEW.WM_CONCAT(DISTINCT TT.SEQ_PLA_TALHAO)) AS SEQ_PLA_TALHAO,
       TO_CHAR(WM_CONCAT(DISTINCT TT.NUMERO_TALHAO)) AS NUMERO_TALHAO,
       TST.FINALIZADO,
       TT.SIGLA_UNIDADE,
       TT.DESC_VARIEDADE,
       TT.SEQ_PLA_VARIEDADE,
       TT.DESC_CULTURA,
       TT.SEQ_PLA_TIPO_CULT,
       SUM(TST.QTDE_HA) QTDE_HA,
       TST.KGS_PREV_PRODUCAO,
       TST.OBSERVACOES,
       TT.DESCRICAO_PRODUTO,
       TT.SEQ_PLA_PRODUTO,
       SUM(TT.ROMANEIOS) AS ROMANEIOS,
       SUM(TT.SUBTOTAL) SUBTOTAL,
       SUM(TT.PESO_BRUTO) PESO_BRUTO,
       SUM(TT.DES_UMIDADE) DES_UMIDADE,
       SUM(TT.DES_IMPUREZA) DES_IMPUREZA,
       SUM(TT.DES_AVARIADO) DES_AVARIADO,
       SUM(TT.DES_ARDIDO) DES_ARDIDO,
       SUM(TT.DES_OUTROS) DES_OUTROS,
       SUM(TT.DESCONTOS) DESCONTOS,
       SUM(TT.TOTPERUMI) TOTPERUMI,
       SUM(TT.TOTUMI) TOTUMI,
       SUM(DECODE(NVL(TT.TOTPERUMI, 0), 0, 0, TT.TOTUMI / TT.TOTPERUMI)) AS PER_UMIDADE,
       SUM(DECODE(NVL(TT.TOTPERIMP, 0), 0, 0, TT.TOTIMP / TT.TOTPERIMP)) AS PER_IMPUREZA,
       SUM(DECODE(NVL(TT.TOTPERAVA, 0), 0, 0, TT.TOTAVA / TT.TOTPERAVA)) AS PER_AVARIADO,
       SUM(DECODE(NVL(TT.TOTPERARD, 0), 0, 0, TT.TOTARD / TT.TOTPERARD)) AS PER_ARDIDO,
       SUM(DECODE(NVL(TT.TOTPEROUT, 0), 0, 0, TT.TOTOUT / TT.TOTPEROUT)) AS PER_OUTROS,
       SUM(TT.PESO_LIQUIDO) PESO_LIQUIDO,
       SUM(TT.LIQUIDO_SACAS) LIQUIDO_SACAS,
       (DECODE(SUM(NVL(TST.QTDE_HA, 0)),0,0,((SUM(TT.PESO_LIQUIDO) / SUM(TST.QTDE_HA)) / TO_NUMBER(1) ))) AS SACAS_HA
  FROM (SELECT --DECODE(OS.DATA_ENCERRAMENTO, NULL, 'N', 'S') AS FINALIZADO,
               OS.COD_SAFRA,
               OS.SEQ_PLA_FAZENDA,
               OS.SEQ_PLA_ORDEM,
               F.SIGLA_FAZENDA,
               F.DESC_FAZENDA,
               T.NUMERO_TALHAO,
               T.SEQ_PLA_TALHAO,
               TC.DESC_CULTURA,
               OS.SEQ_PLA_TIPO_CULT,
               UV.DESC_VARIEDADE,
               UV.SEQ_PLA_VARIEDADE,
               P.DESCRICAO_PRODUTO,
               P.SEQ_PLA_PRODUTO,
               UP.SIGLA_UNIDADE,
               SUM(1*VOR.PERCENTUAL/100) AS ROMANEIOS,
               SUM(ROUND(NVL(VTR.SUB_TOTAL, 0), 2)) AS SUBTOTAL,
               SUM(CASE
                     WHEN EER.TIPO_ENTRADA <> 'O' THEN
                      ROUND(NVL(EER.PESO_BRUTO, 0) - NVL(EER.PESO_TARA, 0), 3)
                     ELSE
                      ROUND(NVL(VTR.SUB_TOTAL, 0), 3)
                   END) AS PESO_BRUTO,
               SUM(ROUND(NVL(VTR.DES_UMIDADE, 0) , 0)) AS DES_UMIDADE,
               SUM(ROUND(NVL(VTR.DES_IMPUREZA, 0) , 0)) AS DES_IMPUREZA,
               SUM(ROUND(NVL(VTR.DES_AVARIADO, 0) , 0)) AS DES_AVARIADO,
               SUM(ROUND(NVL(VTR.DES_ARDIDO, 0) , 0)) AS DES_ARDIDO,
               SUM(ROUND(NVL(VTR.DES_OUTROS, 0) , 0)) AS DES_OUTROS,
               SUM(NVL(VTR.DES_UMIDADE, 0) + NVL(VTR.DES_IMPUREZA, 0) +
                   NVL(VTR.DES_AVARIADO, 0) + NVL(VTR.DES_ARDIDO, 0) +
                   NVL(VTR.DES_OUTROS, 0)) AS DESCONTOS,
               SUM(DECODE(NVL(PER_UMIDADE, 0),
                          0,
                          0,
                          ROUND((EER.PESO_BRUTO - EER.PESO_TARA) *
                                VOR.PERCENTUAL / 100,
                                0))) AS TOTUMI,
               SUM(DECODE(NVL(PER_UMIDADE, 0),
                          0,
                          0,
                          ROUND((EER.PESO_BRUTO - EER.PESO_TARA) *
                                VOR.PERCENTUAL / 100,
                                0) / NVL(PER_UMIDADE, 0))) AS TOTPERUMI,
               SUM(DECODE(NVL(PER_IMPUREZA, 0),
                          0,
                          0,
                          ROUND((EER.PESO_BRUTO - EER.PESO_TARA) *
                                VOR.PERCENTUAL / 100,
                                0))) AS TOTIMP,
               SUM(DECODE(NVL(PER_IMPUREZA, 0),
                          0,
                          0,
                          ROUND((EER.PESO_BRUTO - EER.PESO_TARA) *
                                VOR.PERCENTUAL / 100,
                                0) / NVL(PER_IMPUREZA, 0))) AS TOTPERIMP,
               SUM(DECODE(NVL(PER_AVARIADO, 0),
                          0,
                          0,
                          ROUND((EER.PESO_BRUTO - EER.PESO_TARA) *
                                VOR.PERCENTUAL / 100,
                                0))) AS TOTAVA,
               SUM(DECODE(NVL(PER_AVARIADO, 0),
                          0,
                          0,
                          ROUND((EER.PESO_BRUTO - EER.PESO_TARA) *
                                VOR.PERCENTUAL / 100,
                                0) / NVL(PER_AVARIADO, 0))) AS TOTPERAVA,
               SUM(DECODE(NVL(PER_ARDIDO, 0),
                          0,
                          0,
                          ROUND((EER.PESO_BRUTO - EER.PESO_TARA) *
                                VOR.PERCENTUAL / 100,
                                0))) AS TOTARD,
               SUM(DECODE(NVL(PER_ARDIDO, 0),
                          0,
                          0,
                          ROUND((EER.PESO_BRUTO - EER.PESO_TARA) *
                                VOR.PERCENTUAL / 100,
                                0) / NVL(PER_ARDIDO, 0))) AS TOTPERARD,
               SUM(DECODE(NVL(PER_OUTROS, 0),
                          0,
                          0,
                          ROUND((EER.PESO_BRUTO - EER.PESO_TARA) *
                                VOR.PERCENTUAL / 100,
                                0))) AS TOTOUT,
               SUM(DECODE(NVL(PER_OUTROS, 0),
                          0,
                          0,
                          ROUND((EER.PESO_BRUTO - EER.PESO_TARA) *
                                VOR.PERCENTUAL / 100,
                                0) / NVL(PER_OUTROS, 0))) AS TOTPEROUT,
               SUM(CASE
                     WHEN EER.TIPO_ENTRADA <> 'O' THEN
                      ROUND(NVL(EER.PESO_LIQUIDO, 0), 3)
                     ELSE
                      ROUND((VTR.SUB_TOTAL -
                            (NVL(VTR.DES_UMIDADE, 0) + NVL(VTR.DES_IMPUREZA, 0) +
                            NVL(VTR.DES_AVARIADO, 0) + NVL(VTR.DES_ARDIDO, 0) +
                            NVL(VTR.DES_OUTROS, 0))),
                            3) -
                      ROUND((VTR.SUB_TOTAL /
                            (SELECT NVL(SUM(V.SUB_TOTAL), 1)
                                FROM AGNEW.VINCULA_TALHAO_ROMANEIO V
                               WHERE V.SEQ_PLA_ENTRADA = VTR.SEQ_PLA_ENTRADA) *
                            NVL(EER.DESC_LONA, 0)),
                            3)
                   END) AS PESO_LIQUIDO,
               ((SUM(ROUND(VTR.SUB_TOTAL, 3)) -
               SUM(NVL(VTR.DES_UMIDADE, 0) + NVL(VTR.DES_IMPUREZA, 0) +
                     NVL(VTR.DES_AVARIADO, 0) + NVL(VTR.DES_ARDIDO, 0) +
                     NVL(VTR.DES_OUTROS, 0))) / TO_NUMBER(1)) AS LIQUIDO_SACAS,
               FI.COD_FILIAL,
               FI.DESCRICAO_FILIAL
          FROM AGNEW.ORDEM_SERVICO           OS,
               AGNEW.ORDEM_SERV_TALHOES      OST,
               AGNEW.TALHOES                 T,
               AGNEW.TALHOES_SAFRA           TS,
               AGNEW.TIPO_CULTURA            TC,
               AGNEW.UBS_VARIEDADES          UV,
               AGNEW.VINCULA_OS_ROMANEIO     VOR,
               AGNEW.VINCULA_TALHAO_ROMANEIO VTR,
               AGNEW.EST_ENTRADAS            EE,
               AGNEW.EST_ENTRADAS_ROM        EER,
               AGNEW.EST_ENTRADAS_ITENS      EEI,
               AGNEW.PRODUTOS                P,
               AGNEW.FAZENDAS                F,
               AGNEW.AGLOMERADOS             AG,
               AGNEW.FILIAIS                 FI,
               AGNEW.UNIDADE_PRODUTO         UP
         WHERE OS.SEQ_PLA_ORDEM = OST.SEQ_PLA_ORDEM
           AND OST.SEQ_PLA_TALHAO = TS.SEQ_PLA_TALHAO
           AND OST.SEQ_PLA_TALHAO_SAFRA = TS.SEQ_PLA_TALHAO_SAFRA
           AND TS.SEQ_PLA_TALHAO = T.SEQ_PLA_TALHAO
           AND OS.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT
           AND TS.SEQ_PLA_VARIEDADE = UV.SEQ_PLA_VARIEDADE
           AND OS.SEQ_PLA_ORDEM = VOR.SEQ_PLA_ORDEM
           AND VOR.SEQ_PLA_ENTRADA = EE.SEQ_PLA_ENTRADA
           AND EE.SEQ_PLA_ENTRADA = EER.SEQ_PLA_ENTRADA
           AND EE.SEQ_PLA_ENTRADA = EEI.SEQ_PLA_ENTRADA
           AND EEI.SEQ_PLA_PRODUTO = P.SEQ_PLA_PRODUTO
           AND OS.SEQ_PLA_FAZENDA = F.SEQ_PLA_FAZENDA
           AND F.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO
           AND AG.COD_FILIAL = FI.COD_FILIAL
           AND EEI.SEQ_PLA_UNIDADE = UP.SEQ_PLA_UNIDADE(+)
           AND ((EER.TIPO_CARGA = 'A') OR ('A' = 'A'))
           AND VTR.SEQ_PLA_ORDEM = OST.SEQ_PLA_ORDEM
           AND VTR.SEQ_PLA_ORDEM_TALHOES = T.SEQ_PLA_TALHAO
           AND VTR.SEQ_PLA_TALHAO_SAFRA = TS.SEQ_PLA_TALHAO_SAFRA
           AND VTR.SEQ_PLA_ENTRADA = EER.SEQ_PLA_ENTRADA
           AND EE.FINALIZADO = 'S'
           AND NVL(EE.CANCELADO, 'N') <> 'S'
           AND OS.SEQ_PLA_VINC_TAREFA IN
               (SELECT VTG.SEQ_PLA_VINC_TAREFA
                  FROM AGNEW.VINCULA_TAREFA_GRUPO VTG
                 WHERE VTG.SEQ_PLA_TAREFA IN
                       (SELECT T.SEQ_PLA_TAREFA
                          FROM AGNEW.TAREFAS T
                         WHERE T.COLHEITA = 'S'))
         GROUP BY OS.COD_SAFRA,
                  OS.SEQ_PLA_FAZENDA,
                  F.SIGLA_FAZENDA,
                  F.DESC_FAZENDA,
                  OS.SEQ_PLA_ORDEM,
                  T.NUMERO_TALHAO,
                  T.SEQ_PLA_TALHAO,
                  TC.DESC_CULTURA,
                  OS.SEQ_PLA_TIPO_CULT,
                  UV.DESC_VARIEDADE,
                  UV.SEQ_PLA_VARIEDADE,
                  UP.SIGLA_UNIDADE,
                  FI.COD_FILIAL,
                  FI.DESCRICAO_FILIAL,
                  P.DESCRICAO_PRODUTO,
                  P.SEQ_PLA_PRODUTO,
                  FI.COD_FILIAL,
                  FI.DESCRICAO_FILIAL
 UNION
SELECT DISTINCT EE.COD_SAFRA,
                FA.SEQ_PLA_FAZENDA,
                SA.SEQ_PLA_ORDEM,
                FA.SIGLA_FAZENDA,
                FA.DESC_FAZENDA,
                SA.NUMERO_TALHAO,
                SA.SEQ_PLA_TALHAO,
                TC.DESC_CULTURA,
                TC.SEQ_PLA_TIPO_CULT,
                VA.DESC_VARIEDADE,
                VA.SEQ_PLA_VARIEDADE,
                PD.DESCRICAO_PRODUTO,
                PD.SEQ_PLA_PRODUTO,
                UP.SIGLA_UNIDADE,
                0 AS ROMANEIOS,
                0 AS SUBTOTAL,
                sum(EI.QUANTIDADE) - (NVL(SUM(DEV.QTDE_DEV), 0)) AS PESO_BRUTO,
                0 AS DES_UMIDADE,
                0 AS DES_IMPUREZA,
                0 AS DES_AVARIADO,
                0 AS DES_ARDIDO,
                0 AS DES_OUTROS,
                0 AS DESCONTOS,
                0 AS TOTUMI,
                0 AS TOTPERUMI,
                0 AS TOTIMP,
                0 AS TOTPERIMP,
                0 AS TOTAVA,
                0 AS TOTPERAVA,
                0 AS TOTARD,
                0 AS TOTPERARD,
                0 AS TOTOUT,
                0 AS TOTPEROUT,
                sum(EI.QUANTIDADE) - (NVL(SUM(DEV.QTDE_DEV), 0)) AS PESO_LIQUIDO,
                0 AS LIQUIDO_SACAS,
                FI.COD_FILIAL,
                FI.DESCRICAO_FILIAL
  FROM AGNEW.EST_ENTRADAS       EE,
       AGNEW.EST_ENTRADAS_ITENS EI,
       AGNEW.FAZENDAS           FA,
       AGNEW.AGLOMERADOS        AG,
       AGNEW.FILIAIS            FI,
       AGNEW.VINCULA_OS_MODULO  VN,
       (SELECT DISTINCT OS.SEQ_PLA_ORDEM,
       TA.SEQ_PLA_TALHAO,
       TS.SEQ_PLA_TIPO_CULT,
       TA.NUMERO_TALHAO,
       TS.SEQ_PLA_VARIEDADE
       FROM AGNEW.ORDEM_SERVICO      OS,
       AGNEW.ORDEM_SERV_TALHOES OT,
       AGNEW.TALHOES_SAFRA      TS,
       AGNEW.TALHOES            TA
       WHERE OS.SEQ_PLA_ORDEM = OT.SEQ_PLA_ORDEM
       AND OT.SEQ_PLA_TALHAO_SAFRA = TS.SEQ_PLA_TALHAO_SAFRA
       AND EXISTS (SELECT S.SEQ_PLA_ORDEM FROM AGNEW.VINCULA_OS_MODULO S WHERE S.SEQ_PLA_ORDEM = OS.SEQ_PLA_ORDEM)
       AND TS.SEQ_PLA_TALHAO = TA.SEQ_PLA_TALHAO) SA,
       AGNEW.TIPO_CULTURA       TC,
       AGNEW.PRODUTOS           PD,
       AGNEW.UBS_VARIEDADES     VA,
       AGNEW.UNIDADE_PRODUTO    UP,
       (SELECT NVL(SUM(LG.QUANTIDADE), 0) AS QTDE_DEV,
               LG.SEQ_PLA_MODULO,
               LG.SEQ_PLA_ENTRADA
          FROM AGNEW.EST_ENTRADAS          EE,
               AGNEW.EST_ENTRADAS_ITENS    II,
               AGNEW.LIGA_ENTRADAS_MODULOS LG
         WHERE EE.SEQ_PLA_ENTRADA = II.SEQ_PLA_ENTRADA
           AND EE.SEQ_PLA_ENTRADA = LG.SEQ_PLA_ENTRADA
           AND UPPER(EE.FORM_LANCAMENTO) = 'DFMNFENTRADARECUSA'
           AND NVL(EE.CANCELADO, 'N') <> 'S'
         GROUP BY LG.SEQ_PLA_MODULO, LG.SEQ_PLA_ENTRADA) DEV
 WHERE 1 = 1
   AND EE.SEQ_PLA_ENTRADA = EI.SEQ_PLA_ENTRADA
   AND EE.SEQ_PLA_FAZENDA = FA.SEQ_PLA_FAZENDA
   AND FA.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO
   AND AG.COD_FILIAL = FI.COD_FILIAL
   AND EE.SEQ_PLA_ENTRADA = VN.SEQ_PLA_ENTRADA
   AND VN.SEQ_PLA_ORDEM = SA.SEQ_PLA_ORDEM
   AND SA.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT
   AND EI.SEQ_PLA_PRODUTO = PD.SEQ_PLA_PRODUTO
   AND SA.SEQ_PLA_VARIEDADE = VA.SEQ_PLA_VARIEDADE
   AND EI.SEQ_PLA_UNIDADE = UP.SEQ_PLA_UNIDADE
   AND EE.FORM_LANCAMENTO = 'DFMMODULOSCOLHIDOS'
   AND EE.SEQ_PLA_ENTRADA = DEV.SEQ_PLA_MODULO(+)
 GROUP BY EE.COD_SAFRA,
          FA.SEQ_PLA_FAZENDA,
          FA.SIGLA_FAZENDA,
          FA.DESC_FAZENDA,
          SA.NUMERO_TALHAO,
          SA.SEQ_PLA_TALHAO,
          TC.DESC_CULTURA,
          TC.SEQ_PLA_TIPO_CULT,
          VA.DESC_VARIEDADE,
          VA.SEQ_PLA_VARIEDADE,
          PD.DESCRICAO_PRODUTO,
          PD.SEQ_PLA_PRODUTO,
          SA.SEQ_PLA_ORDEM,
          UP.SIGLA_UNIDADE,
          FI.DESCRICAO_FILIAL,
          FI.COD_FILIAL

                 ) TT,
       (SELECT TS.SEQ_PLA_TALHAO,
               TL.SEQ_PLA_ORDEM,
               decode(sum(case when nvl(TS.FINALIZADO,'N') = 'N' then 1 else 0 end),0,'S','N') as FINALIZADO,
               TS.OBSERVACOES,
               TS.SEQ_PLA_TIPO_CULT,
               (TS.KGS_PREV_PRODUCAO / TO_NUMBER(1)) AS KGS_PREV_PRODUCAO,
               TS.SEQ_PLA_VARIEDADE,
               TS.COD_SAFRA,
               PD.SEQ_PLA_PRODUTO,
               SUM(AGNEW.fRetAreaMediaProducao(TL.Seq_Pla_Ordem, TS.Seq_Pla_Talhao, TL.Seq_Pla_Ordem_Talhoes, TS.Seq_Pla_Variedade)) AS QTDE_HA
          FROM AGNEW.TALHOES_SAFRA TS, AGNEW.ORDEM_SERV_TALHOES TL ,
               (SELECT DISTINCT TSS.SEQ_PLA_TALHAO,
                       TSS.SEQ_PLA_TIPO_CULT,
                       TSS.SEQ_PLA_VARIEDADE,
                       OS.COD_SAFRA,
                       OS.SEQ_PLA_ORDEM,
                       I.SEQ_PLA_PRODUTO
                  FROM AGNEW.VINCULA_OS_ROMANEIO R,
                       AGNEW.EST_ENTRADAS_ITENS  I,
                       AGNEW.ORDEM_SERVICO       OS,
                       AGNEW.ORDEM_SERV_TALHOES  OSV,
                       AGNEW.TALHOES_SAFRA       TSS
                 WHERE R.SEQ_PLA_ORDEM = OS.SEQ_PLA_ORDEM
                   AND OS.SEQ_PLA_ORDEM = OSV.SEQ_PLA_ORDEM
                   --AND OSV.SEQ_PLA_TALHAO = TS.SEQ_PLA_TALHAO
                   --AND OS.SEQ_PLA_TIPO_CULT = TS.SEQ_PLA_TIPO_CULT
                   AND OSV.SEQ_PLA_TALHAO_SAFRA = TSS.SEQ_PLA_TALHAO_SAFRA
                   --AND TSS.SEQ_PLA_VARIEDADE = TS.SEQ_PLA_VARIEDADE
                   AND R.SEQ_PLA_ENTRADA = I.SEQ_PLA_ENTRADA
                   --AND OS.COD_SAFRA = TS.COD_SAFRA
                   --AND OS.SEQ_PLA_ORDEM = TL.SEQ_PLA_ORDEM
                   ) pd
         WHERE 1 = 1
           AND TS.SEQ_PLA_TALHAO_SAFRA = TL.SEQ_PLA_TALHAO_SAFRA
           AND TS.SEQ_PLA_TALHAO = pd.SEQ_PLA_TALHAO
           AND TS.SEQ_PLA_TIPO_CULT = pd.SEQ_PLA_TIPO_CULT
           AND TS.SEQ_PLA_VARIEDADE = pd.SEQ_PLA_VARIEDADE
           AND TS.COD_SAFRA = pd.COD_SAFRA
           AND TL.SEQ_PLA_ORDEM = pd.SEQ_PLA_ORDEM
           AND EXISTS
         (SELECT *
                  FROM AGNEW.ORDEM_SERVICO OS, AGNEW.TAREFAS TA, AGNEW.VINCULA_TAREFA_GRUPO VT
                 WHERE OS.SEQ_PLA_VINC_TAREFA = VT.SEQ_PLA_VINC_TAREFA
                   AND VT.SEQ_PLA_TAREFA = TA.SEQ_PLA_TAREFA
                   AND TA.COLHEITA = 'S'
                   AND OS.SEQ_PLA_ORDEM = TL.SEQ_PLA_ORDEM)
           AND NOT EXISTS
         (SELECT OT.SEQ_PLA_ORDEM_TALHOES
                  FROM AGNEW.ORDEM_SERV_TALHOES OT, AGNEW.ORDEM_SERVICO OS
                 WHERE OT.SEQ_PLA_ORDEM = OS.SEQ_PLA_ORDEM
                   AND OT.SEQ_PLA_ORDEM_TALHOES = TS.SEQ_PLA_ORDEM_PLANTIO
                 HAVING
                 NVL((SELECT SUM(NVL(EI.QUANTIDADE, 0))
                             FROM AGNEW.EST_ENTRADAS       EE,
                                  AGNEW.EST_ENTRADAS_ITENS EI,
                                  AGNEW.PRODUTOS           P
                            WHERE EE.SEQ_PLA_ENTRADA = EI.SEQ_PLA_ENTRADA
                              AND EE.FORM_LANCAMENTO = 'DFMDEVOLUCAORETIRADA'
                              AND EI.SEQ_PLA_PRODUTO = P.SEQ_PLA_PRODUTO
                              AND NVL(P.SEMENTE, 'N') = 'S'
                              AND NVL(EE.FINALIZADO, 'N') = 'S'
                              AND EE.SEQ_PLA_ORIGEM = OS.SEQ_PLA_ORDEM),
                           0) =
                       NVL((SELECT SUM(NVL(SI.QUANTIDADE, 0))
                             FROM AGNEW.EST_SAIDAS       ES,
                                  AGNEW.EST_SAIDAS_ITENS SI,
                                  AGNEW.PRODUTOS         P
                            WHERE ES.SEQ_PLA_SAIDA = SI.SEQ_PLA_SAIDA
                              AND SI.SEQ_PLA_PRODUTO = P.SEQ_PLA_PRODUTO
                              AND ES.SEQ_PLA_ORIGEM = OS.SEQ_PLA_ORDEM
                              AND ES.FORM_LANCAMENTO = 'DFMRETIRAORDEM'
                              AND NVL(ES.FINALIZADO, 'N') = 'S'
                              AND NVL(P.SEMENTE, 'N') = 'S'),
                           0)
                 GROUP BY OS.SEQ_PLA_ORDEM, OT.SEQ_PLA_ORDEM_TALHOES)
           AND EXISTS
         (SELECT OT.SEQ_PLA_ORDEM_TALHOES
                  FROM AGNEW.ORDEM_SERV_TALHOES OT,
                       AGNEW.EST_SAIDAS         ES,
                       AGNEW.EST_SAIDAS_ITENS   SI,
                       AGNEW.PRODUTOS           PD
                 WHERE ES.FORM_LANCAMENTO = 'DFMRETIRAORDEM'
                   AND ES.SEQ_PLA_SAIDA = SI.SEQ_PLA_SAIDA
                   AND SI.SEQ_PLA_PRODUTO = PD.SEQ_PLA_PRODUTO
                   AND ES.SEQ_PLA_ORIGEM = OT.SEQ_PLA_ORDEM
                   AND OT.SEQ_PLA_ORDEM_TALHOES = TS.SEQ_PLA_ORDEM_PLANTIO
                   AND NVL(PD.SEMENTE, 'N') = 'S'
                 HAVING SUM(NVL(SI.QUANTIDADE, 0)) > 0
                 GROUP BY OT.SEQ_PLA_ORDEM_TALHOES)
         GROUP BY TS.SEQ_PLA_TALHAO,
                  TL.SEQ_PLA_ORDEM,
                  TS.OBSERVACOES,
                  TS.SEQ_PLA_TIPO_CULT,
                  TS.COD_SAFRA,
                  TS.SEQ_PLA_VARIEDADE,
                  PD.SEQ_PLA_PRODUTO,
                  (TS.KGS_PREV_PRODUCAO / TO_NUMBER(1))

        UNION
        SELECT TS.SEQ_PLA_TALHAO,
               TL.SEQ_PLA_ORDEM,
               decode(sum(case when nvl(TS.FINALIZADO,'N') = 'N' then 1 else 0 end),0,'S','N') as FINALIZADO,
               TS.OBSERVACOES,
               TS.SEQ_PLA_TIPO_CULT,
               (TS.KGS_PREV_PRODUCAO / TO_NUMBER(1)) AS KGS_PREV_PRODUCAO,
               TS.SEQ_PLA_VARIEDADE,
               TS.COD_SAFRA,
               PD.SEQ_PLA_PRODUTO,
               SUM(AGNEW.fRetAreaMediaProducao(TL.Seq_Pla_Ordem, TS.Seq_Pla_Talhao, TL.Seq_Pla_Ordem_Talhoes, TS.Seq_Pla_Variedade)) AS QTDE_HA
          FROM AGNEW.TALHOES_SAFRA TS, AGNEW.ORDEM_SERV_TALHOES TL ,
          (SELECT DISTINCT TSS.SEQ_PLA_TALHAO,
                       TSS.SEQ_PLA_TIPO_CULT,
                       TSS.SEQ_PLA_VARIEDADE,
                       OS.COD_SAFRA,
                       OS.SEQ_PLA_ORDEM,
                       I.SEQ_PLA_PRODUTO
                  FROM AGNEW.VINCULA_OS_ROMANEIO R,
                       AGNEW.EST_ENTRADAS_ITENS  I,
                       AGNEW.ORDEM_SERVICO       OS,
                       AGNEW.ORDEM_SERV_TALHOES  OSV,
                       AGNEW.TALHOES_SAFRA       TSS
                 WHERE R.SEQ_PLA_ORDEM = OS.SEQ_PLA_ORDEM
                   AND OS.SEQ_PLA_ORDEM = OSV.SEQ_PLA_ORDEM
                   --AND OSV.SEQ_PLA_TALHAO = TS.SEQ_PLA_TALHAO
                   --AND OS.SEQ_PLA_TIPO_CULT = TS.SEQ_PLA_TIPO_CULT
                   AND OSV.SEQ_PLA_TALHAO_SAFRA = TSS.SEQ_PLA_TALHAO_SAFRA
                   --AND TSS.SEQ_PLA_VARIEDADE = TS.SEQ_PLA_VARIEDADE
                   AND R.SEQ_PLA_ENTRADA = I.SEQ_PLA_ENTRADA
                   --AND OS.COD_SAFRA = TS.COD_SAFRA
                   --AND OS.SEQ_PLA_ORDEM = TL.SEQ_PLA_ORDEM
                   ) pd
         WHERE 1 = 1
           AND TS.SEQ_PLA_TALHAO_SAFRA = TL.SEQ_PLA_TALHAO_SAFRA
                      AND TS.SEQ_PLA_TALHAO_SAFRA = TL.SEQ_PLA_TALHAO_SAFRA
           AND TS.SEQ_PLA_TALHAO = pd.SEQ_PLA_TALHAO
           AND TS.SEQ_PLA_TIPO_CULT = pd.SEQ_PLA_TIPO_CULT
           AND TS.SEQ_PLA_VARIEDADE = pd.SEQ_PLA_VARIEDADE
           AND TS.COD_SAFRA = pd.COD_SAFRA
           AND TL.SEQ_PLA_ORDEM = pd.SEQ_PLA_ORDEM

           AND EXISTS
         (SELECT *
                  FROM AGNEW.ORDEM_SERVICO OS, AGNEW.TAREFAS TA, AGNEW.VINCULA_TAREFA_GRUPO VT
                 WHERE OS.SEQ_PLA_VINC_TAREFA = VT.SEQ_PLA_VINC_TAREFA
                   AND VT.SEQ_PLA_TAREFA = TA.SEQ_PLA_TAREFA
                   AND TA.COLHEITA = 'S'
                   AND OS.SEQ_PLA_ORDEM = TL.SEQ_PLA_ORDEM)
           AND NOT EXISTS
         (SELECT OT.SEQ_PLA_ORDEM_TALHOES
                  FROM AGNEW.ORDEM_SERV_TALHOES OT, AGNEW.ORDEM_SERVICO OS
                 WHERE OT.SEQ_PLA_ORDEM = OS.SEQ_PLA_ORDEM
                   AND OT.SEQ_PLA_ORDEM_TALHOES = TS.SEQ_PLA_ORDEM_PLANTIO
                 HAVING
                 NVL((SELECT SUM(NVL(EI.QUANTIDADE, 0))
                             FROM AGNEW.EST_ENTRADAS       EE,
                                  AGNEW.EST_ENTRADAS_ITENS EI,
                                  AGNEW.PRODUTOS           P
                            WHERE EE.SEQ_PLA_ENTRADA = EI.SEQ_PLA_ENTRADA
                              AND EE.FORM_LANCAMENTO = 'DFMDEVOLUCAORETIRADA'
                              AND EI.SEQ_PLA_PRODUTO = P.SEQ_PLA_PRODUTO
                              AND NVL(P.SEMENTE, 'N') = 'S'
                              AND NVL(EE.FINALIZADO, 'N') = 'S'
                              AND EE.SEQ_PLA_ORIGEM = OS.SEQ_PLA_ORDEM),
                           0) =
                       NVL((SELECT SUM(NVL(SI.QUANTIDADE, 0))
                             FROM AGNEW.EST_SAIDAS       ES,
                                  AGNEW.EST_SAIDAS_ITENS SI,
                                  AGNEW.PRODUTOS         P
                            WHERE ES.SEQ_PLA_SAIDA = SI.SEQ_PLA_SAIDA
                              AND SI.SEQ_PLA_PRODUTO = P.SEQ_PLA_PRODUTO
                              AND ES.SEQ_PLA_ORIGEM = OS.SEQ_PLA_ORDEM
                              AND ES.FORM_LANCAMENTO = 'DFMRETIRAORDEM'
                              AND NVL(ES.FINALIZADO, 'N') = 'S'
                              AND NVL(P.SEMENTE, 'N') = 'S'),
                           0)
                 GROUP BY OS.SEQ_PLA_ORDEM, OT.SEQ_PLA_ORDEM_TALHOES)
           AND EXISTS
         (SELECT OT.SEQ_PLA_ORDEM_TALHOES
                  FROM AGNEW.ORDEM_SERV_TALHOES OT,
                       AGNEW.EST_SAIDAS         ES,
                       AGNEW.EST_SAIDAS_ITENS   SI,
                       AGNEW.PRODUTOS           PD
                 WHERE ES.FORM_LANCAMENTO = 'DFMRETIRAORDEM'
                   AND ES.SEQ_PLA_SAIDA = SI.SEQ_PLA_SAIDA
                   AND SI.SEQ_PLA_PRODUTO = PD.SEQ_PLA_PRODUTO
                   AND ES.SEQ_PLA_ORIGEM = OT.SEQ_PLA_ORDEM
                   AND OT.SEQ_PLA_ORDEM_TALHOES = TS.SEQ_PLA_ORDEM_PLANTIO
                   AND NVL(PD.SEMENTE, 'N') = 'S'
                 HAVING SUM(NVL(SI.QUANTIDADE, 0)) > 0
                 GROUP BY OT.SEQ_PLA_ORDEM_TALHOES)
           AND EXISTS (select ts.seq_pla_talhao_safra
                         from AGNEW.talhoes_safra ts,
                              AGNEW.ordem_serv_talhoes ot,
                              AGNEW.vincula_os_modulo vc
                        where 1 = 1
                          and exists (select *
                                 from AGNEW.ordem_serv_talhoes   t,
                                      AGNEW.ordem_servico        os,
                                      AGNEW.tarefas              tr,
                                      AGNEW.vincula_tarefa_grupo v
                                where t.seq_pla_talhao_safra = ts.seq_pla_talhao_safra
                                  and t.seq_pla_ordem = os.seq_pla_ordem
                                  and os.seq_pla_vinc_tarefa = v.seq_pla_vinc_tarefa
                                  and v.seq_pla_tarefa = tr.seq_pla_tarefa
                                  and nvl(tr.colheita, 'N') = 'S')
                          and ts.seq_pla_talhao_safra = ot.seq_pla_talhao_safra
                          and ot.seq_pla_ordem = vc.seq_pla_ordem
                          and ot.seq_pla_ordem_talhoes = tl.seq_pla_ordem_talhoes)
         GROUP BY TS.SEQ_PLA_TALHAO,
                  TL.SEQ_PLA_ORDEM,
                  TS.OBSERVACOES,
                  TS.SEQ_PLA_TIPO_CULT,
                  TS.SEQ_PLA_VARIEDADE,
                  TS.COD_SAFRA,
                  PD.SEQ_PLA_PRODUTO,
                  (TS.KGS_PREV_PRODUCAO / TO_NUMBER(1))
        ) TST,
       AGNEW.SAFRAS SF,
       AGNEW.FAZENDAS FZ,
       AGNEW.AGLOMERADOS AG,
       AGNEW.EMPRESAS EP,
       AGNEW.FILIAIS FL
 WHERE TT.SEQ_PLA_TALHAO = TST.SEQ_PLA_TALHAO
   AND TT.SEQ_PLA_TIPO_CULT = TST.SEQ_PLA_TIPO_CULT
   AND TT.SEQ_PLA_VARIEDADE = TST.SEQ_PLA_VARIEDADE
   AND TT.SEQ_PLA_PRODUTO = TST.SEQ_PLA_PRODUTO
   AND TT.SEQ_PLA_ORDEM = TST.SEQ_PLA_ORDEM
   AND TT.COD_SAFRA = TST.COD_SAFRA
   AND TT.SEQ_PLA_FAZENDA = FZ.SEQ_PLA_FAZENDA
   AND TT.COD_SAFRA = SF.COD_SAFRA
   AND FZ.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO
   AND AG.COD_FILIAL = FL.COD_FILIAL
   AND AG.COD_EMPRESA = FL.COD_EMPRESA
   AND FL.COD_EMPRESA = EP.COD_EMPRESA
 GROUP BY EP.COD_EMPRESA,
          EP.NOME_EMPRESA,
          FL.COD_FILIAL,
          FL.DESCRICAO_FILIAL,
          AG.SEQ_PLA_AGLOMERADO,
          AG.DESC_AGLOMERADO,
          SF.COD_SAFRA,
          SF.DESCRICAO_SAFRA,
          TT.SEQ_PLA_FAZENDA,
          TT.SIGLA_FAZENDA,
          TT.DESC_FAZENDA,
          TT.NUMERO_TALHAO,
          TT.DESC_VARIEDADE,
          TT.SEQ_PLA_VARIEDADE,
          TT.DESC_CULTURA,
          TT.SEQ_PLA_TIPO_CULT,
          TST.FINALIZADO,
          TST.KGS_PREV_PRODUCAO,
          TST.OBSERVACOES,
          TT.DESCRICAO_PRODUTO,
          TT.SEQ_PLA_PRODUTO,
          TT.SIGLA_UNIDADE,
          TT.DESC_VARIEDADE,
          TT.SEQ_PLA_VARIEDADE;
