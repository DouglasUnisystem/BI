CREATE MATERIALIZED VIEW VW_CUSTO_COMB_FROTAS
REFRESH FORCE ON DEMAND
START WITH SYSDATE NEXT SYSDATE + 5/24/60 
AS
SELECT X.SIGLA_EMPRESA,
       X.SIGLA_FILIAL,
       X.SIGLA_FAZENDA,
       X.DATA_SAIDA,
       X.NR_DOCUMENTO,
       X.SEQ_PLA_CONTROLE_TEMPO_USO,
       X.SEQ_PLA_PATRIMONIO,
       X.DESC_FROTA,
       X.COD_IDENTIFICACAO,
       X.NR_PATRIMONIO,
       X.SEQ_PLA_MODELO,
       X.DESC_MODELO,
       X.SEQ_PLA_PRODUTO,
       X.CODIGO_REDUZIDO,
       X.DESCRICAO_PRODUTO,
       X.QUANTIDADE,
       X.CUSTO_TOTAL,
       X.DESCRICAO,
       X.COD_LCTO,
       X.VALOR_ANTERIOR,
       X.VALOR_ULTIMO,

       CASE
          WHEN X.VALOR_ANTERIOR = 0 THEN  X.VALOR_ANTERIOR
       ELSE    X.VALOR_ULTIMO-X.VALOR_ANTERIOR
       END AS RODADO,

       CASE WHEN X.COD_LCTO = 'HD' THEN CASE WHEN X.VALOR_ANTERIOR > 0 AND X.QUANTIDADE > 0 THEN ((X.VALOR_ULTIMO-X.VALOR_ANTERIOR)/X.QUANTIDADE) ELSE 0 END
            ELSE CASE WHEN (X.VALOR_ANTERIOR > 0) AND (X.VALOR_ULTIMO-X.VALOR_ANTERIOR) > 0 THEN (X.QUANTIDADE/(X.VALOR_ULTIMO-X.VALOR_ANTERIOR)) ELSE 0 END END AS MEDIA
  FROM (SELECT EP.SIGLA_EMPRESA,
               FL.SIGLA_FILIAL,
               FZ.SIGLA_FAZENDA,
               ES.DATA_SAIDA,
               ES.NR_DOCUMENTO,
               SC.SEQ_PLA_PATRIMONIO,
               PA.DESCRICAO AS DESC_FROTA,
               PA.COD_IDENTIFICACAO,
               PA.NR_PATRIMONIO,
               PM.SEQ_PLA_MODELO,
               PM.DESC_MODELO,
               CT.SEQ_PLA_CONTROLE_TEMPO_USO,
               PD.SEQ_PLA_PRODUTO,
               PD.CODIGO_REDUZIDO,
               PD.DESCRICAO_PRODUTO,
               SI.QUANTIDADE,
               TB.DESCRICAO,
               TB.COD_LCTO,
               NVL(SI.CUSTO_TOTAL,0) AS CUSTO_TOTAL,
               NVL((SELECT A.VALOR
                      FROM MVR.CTU_MOVIMENTACAO A
                     WHERE NVL(A.ABASTECIMENTO, 'N') = 'S'
                       AND A.SEQ_PLA_CONTROLE_TEMPO_USO = CT.SEQ_PLA_CONTROLE_TEMPO_USO
                       AND ROWNUM = 1
                       AND A.DATA_MVTO = (SELECT MAX(AA.DATA_MVTO)
                                            FROM MVR.CTU_MOVIMENTACAO AA
                                           WHERE NVL(AA.ABASTECIMENTO, 'N') = 'S'
                                             AND TRUNC(AA.DATA_MVTO) <= TRUNC(ES.DATA_SAIDA)
                                             AND AA.SEQ_PLA_CONTROLE_TEMPO_USO = A.SEQ_PLA_CONTROLE_TEMPO_USO)),0) AS VALOR_ULTIMO,
               NVL((SELECT A.VALOR
                      FROM MVR.CTU_MOVIMENTACAO A
                     WHERE NVL(A.ABASTECIMENTO, 'N') = 'S'
                       AND A.SEQ_PLA_CONTROLE_TEMPO_USO = CT.SEQ_PLA_CONTROLE_TEMPO_USO
                       AND ROWNUM = 1
                       AND A.DATA_MVTO = (SELECT MAX(AA.DATA_MVTO)
                                            FROM MVR.CTU_MOVIMENTACAO AA
                                           WHERE NVL(AA.ABASTECIMENTO, 'N') = 'S'
                                             AND AA.DATA_MVTO < (SELECT MAX(AA.DATA_MVTO)
                                                                   FROM MVR.CTU_MOVIMENTACAO AA
                                                                  WHERE NVL(AA.ABASTECIMENTO, 'N') = 'S'
                                                                    AND TRUNC(AA.DATA_MVTO) <= TRUNC(ES.DATA_SAIDA)
                                                                    AND AA.SEQ_PLA_CONTROLE_TEMPO_USO = A.SEQ_PLA_CONTROLE_TEMPO_USO)
                                             AND TRUNC(AA.DATA_MVTO) < TRUNC(ES.DATA_SAIDA)
                                             AND AA.SEQ_PLA_CONTROLE_TEMPO_USO = A.SEQ_PLA_CONTROLE_TEMPO_USO)),0) AS VALOR_ANTERIOR
          FROM MVR.EST_SAIDAS          ES,
               MVR.EST_SAIDAS_ITENS    SI,
               MVR.EST_SAIDAS_CONSUMO  SC,
               MVR.EMPRESAS            EP,
               MVR.FILIAIS             FL,
               MVR.FAZENDAS            FZ,
               MVR.PRODUTOS            PD,
               MVR.CONTROLE_TEMPO_USO  CT,
               MVR.TABELA_PADRAO_LCTOS TB,
               MVR.TABELA_PADRAO_LCTOS TS,
               MVR.PATRIMONIOS         PA,
               MVR.PAT_MODELOS         PM
         WHERE ES.SEQ_PLA_SAIDA = SI.SEQ_PLA_SAIDA
           AND ES.SEQ_PLA_SAIDA = SC.SEQ_PLA_SAIDA
           AND ES.COD_EMPRESA = EP.COD_EMPRESA
           AND ES.COD_FILIAL = FL.COD_FILIAL
           AND EP.COD_EMPRESA = FL.COD_EMPRESA
           AND ES.SEQ_PLA_FAZENDA = FZ.SEQ_PLA_FAZENDA
           AND SI.SEQ_PLA_PRODUTO = PD.SEQ_PLA_PRODUTO
           AND SC.SEQ_PLA_PATRIMONIO IS NOT NULL
           AND SC.SEQ_PLA_PATRIMONIO = CT.SEQ_PLA_PATRIMONIO
           AND CT.SEQ_TAB_PAD_TP_TEMPO_USO = TB.SEQ_PLA_TAB_PADRAO
           AND CT.SEQ_TAB_PAD_STATUS = TS.SEQ_PLA_TAB_PADRAO
           AND CT.SEQ_PLA_PATRIMONIO = PA.SEQ_PLA_PATRIMONIO
           AND PA.SEQ_PLA_MODELO = PM.SEQ_PLA_MODELO
           AND TS.COD_LCTO = 'S'
           AND NVL(SI.QUANTIDADE,0) > 0

           AND EXISTS (SELECT CT1.SEQ_PLA_CONTROLE_TEMPO_USO
                         FROM MVR.CTU_MOVIMENTACAO CT1
                        WHERE CT1.SEQ_PLA_CONTROLE_TEMPO_USO = CT.SEQ_PLA_CONTROLE_TEMPO_USO
                          AND TRUNC(CT1.DATA_MVTO) <= TRUNC(ES.DATA_SAIDA))
           AND EXISTS (SELECT PB.SEQ_PLA_PRODUTO FROM MVR.POSTO_BICOS PB
           WHERE PB.SEQ_PLA_PRODUTO = SI.SEQ_PLA_PRODUTO)
   ) X;
