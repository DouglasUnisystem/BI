CREATE OR REPLACE VIEW PLANTIO_PREVISAO AS
SELECT "SEQ_PLA_TIPO_CULT","DESC_CULTURA","COD_SAFRA","DESCRICAO_SAFRA","SEQ_PLA_AGLOMERADO","SIGLA_AGLOMERADO","DESC_AGLOMERADO","SEQ_PLA_FAZENDA","SIGLA_FAZENDA","DESC_FAZENDA","SEQ_PLA_TALHAO","NUMERO_TALHAO","DESC_TALHAO","QTDE_HA_PREVISAO","QTDE_HA_REALIZADO","DATA_PREVISAO_PLANTIO","DATA_PLANTIO"
  FROM (SELECT DISTINCT
               TC.SEQ_PLA_TIPO_CULT,
               TC.DESC_CULTURA,
               PS.COD_SAFRA,
               SA.DESCRICAO_SAFRA,
               AG.SEQ_PLA_AGLOMERADO,
               AG.SIGLA_AGLOMERADO,
               AG.DESC_AGLOMERADO,
               FA.SEQ_PLA_FAZENDA,
               FA.SIGLA_FAZENDA,
               FA.DESC_FAZENDA,
               TA.SEQ_PLA_TALHAO,
               TA.NUMERO_TALHAO,
               TA.DESC_TALHAO,
               PO.HECTARES AS QTDE_HA_PREVISAO,
               0 AS QTDE_HA_REALIZADO,
               PSE.DATA_PLANTIO AS DATA_PREVISAO_PLANTIO,
               NULL AS DATA_PLANTIO
          FROM AGNEW.PLANO_OPERACIONAL PO,
               AGNEW.PLANO_OPER_CENARIOS POC,
               AGNEW.PLANTIO_CENARIO_CULTURAS C,
               AGNEW.PLANTIO_CENARIO_REGIOES R,
               AGNEW.PLANTIO_CENARIO_SAFRAS PS,
               AGNEW.PLANO_OPER_SEMENTE PSE,
               AGNEW.TALHOES TA,
               AGNEW.FAZENDAS FA,
               AGNEW.SAFRAS SA,
               AGNEW.TIPO_CULTURA TC,
               AGNEW.AGLOMERADOS AG
         WHERE PO.SEQ_PLA_OPER_CENARIO = POC.SEQ_PLA_OPER_CENARIO
           AND PO.SEQ_PLA_PLANT_CULT = C.SEQ_PLA_PLANT_CULT
           AND C.SEQ_PLA_PLANT_REGIAO = R.SEQ_PLA_PLANT_REGIAO
           AND R.SEQ_PLA_CENARIO_SAFRA = PS.SEQ_PLA_CENARIO_SAFRA
           AND PO.SEQ_PLA_TALHAO = TA.SEQ_PLA_TALHAO
           AND TA.SEQ_PLA_FAZENDA = FA.SEQ_PLA_FAZENDA
           AND PO.SEQ_PLA_PLANO_OPER = PSE.SEQ_PLA_PLANO_OPER
           AND PS.COD_SAFRA = SA.COD_SAFRA
           AND TC.SEQ_PLA_TIPO_CULT = C.SEQ_PLA_TIPO_CULT
           AND FA.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO
           AND POC.DATA_CENARIO = (SELECT MAX(POC1.DATA_CENARIO)
                                     FROM AGNEW.PLANO_OPER_CENARIOS POC1,
                                          AGNEW.PLANO_OPERACIONAL PO1,
                                          AGNEW.PLANTIO_CENARIO_CULTURAS PC1,
                                          AGNEW.PLANTIO_CENARIO_REGIOES PR1,
                                          AGNEW.PLANTIO_CENARIO_SAFRAS PSS1
                                    WHERE POC1.SEQ_PLA_OPER_CENARIO = PO1.SEQ_PLA_OPER_CENARIO
                                      AND PO1.SEQ_PLA_PLANT_CULT = PC1.SEQ_PLA_PLANT_CULT
                                      AND PC1.SEQ_PLA_PLANT_REGIAO = PR1.SEQ_PLA_PLANT_REGIAO
                                      AND PR1.SEQ_PLA_CENARIO_SAFRA = PSS1.SEQ_PLA_CENARIO_SAFRA
                                      AND PO1.SEQ_PLA_TALHAO = PO.SEQ_PLA_TALHAO
                                      AND PSS1.COD_SAFRA = PS.COD_SAFRA
                                      AND PC1.SEQ_PLA_TIPO_CULT = C.SEQ_PLA_TIPO_CULT)
           AND PSE.DATA_PLANTIO IS NOT NULL
        UNION ALL

        SELECT DISTINCT
               TC.SEQ_PLA_TIPO_CULT,
               TC.DESC_CULTURA,
               OSS.COD_SAFRA,
               SA.DESCRICAO_SAFRA,
               AG.SEQ_PLA_AGLOMERADO,
               AG.SIGLA_AGLOMERADO,
               AG.DESC_AGLOMERADO,
               FA.SEQ_PLA_FAZENDA,
               FA.SIGLA_FAZENDA,
               FA.DESC_FAZENDA,
               TA.SEQ_PLA_TALHAO,
               TA.NUMERO_TALHAO,
               TA.DESC_TALHAO,
               0 AS QTDE_HA_PREVISAO,
               TS.QTDE_HA AS QTDE_HA_REALIZADO,
               NULL AS DATA_PREVISAO_PLANTIO,
               TS.DATA_PLANTIO AS DATA_PLANTIO
          FROM AGNEW.ORDEM_SERVICO        OSS,
               AGNEW.ORDEM_SERV_TALHOES   OST,
               AGNEW.TAREFAS              TAR,
               AGNEW.GRUPO_TAREFAS        GTAR,
               AGNEW.VINCULA_TAREFA_GRUPO VINTARGRU,
               AGNEW.TALHOES_SAFRA        TS,
               AGNEW.TALHOES              TA,
               AGNEW.FAZENDAS             FA,
               AGNEW.TIPO_CULTURA         TC,
               AGNEW.SAFRAS               SA,
               AGNEW.AGLOMERADOS          AG,
               AGNEW.UBS_VARIEDADES       V,
               AGNEW.REGIOES_GERENCIAIS   RE,
               AGNEW.EMPRESAS             EM,
               AGNEW.FILIAIS              FI
         WHERE OSS.SEQ_PLA_ORDEM = OST.SEQ_PLA_ORDEM
           AND OST.SEQ_PLA_ORDEM_TALHOES = TS.SEQ_PLA_ORDEM_PLANTIO
           AND TS.SEQ_PLA_TALHAO = TA.SEQ_PLA_TALHAO
           AND TA.SEQ_PLA_FAZENDA = FA.SEQ_PLA_FAZENDA
           AND TS.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT
           AND OSS.SEQ_PLA_VINC_TAREFA = VINTARGRU.SEQ_PLA_VINC_TAREFA
           AND VINTARGRU.SEQ_PLA_TAREFA = TAR.SEQ_PLA_TAREFA
           AND VINTARGRU.SEQ_PLA_GRUPO_TAREFA = GTAR.SEQ_PLA_GRUPO_TAREFA
           AND OSS.SEQ_PLA_ORDEM = OST.SEQ_PLA_ORDEM
           AND OST.SEQ_PLA_ORDEM_TALHOES = TS.SEQ_PLA_ORDEM_PLANTIO
           AND TS.SEQ_PLA_VARIEDADE = V.SEQ_PLA_VARIEDADE
           AND OSS.COD_SAFRA = SA.COD_SAFRA
           AND FA.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO
           AND AG.SEQ_PLA_REGIAO = RE.SEQ_PLA_REGIAO
           AND AG.COD_EMPRESA = EM.COD_EMPRESA
           AND AG.COD_EMPRESA = FI.COD_EMPRESA
           AND AG.COD_FILIAL  = FI.COD_FILIAL
           AND NOT EXISTS
         (SELECT *
                  FROM AGNEW.APONT_AREA_TAL_OS A
                 WHERE A.SEQ_PLA_ORDEM = OSS.SEQ_PLA_ORDEM
                   AND A.SEQ_PLA_TALHAO = TA.SEQ_PLA_TALHAO)
           AND TAR.PLANTIO = 'S'
           AND OST.EFETIVO = 'S'

       UNION ALL

       SELECT  DISTINCT
               TC.SEQ_PLA_TIPO_CULT,
               TC.DESC_CULTURA,
               OSS.COD_SAFRA,
               SA.DESCRICAO_SAFRA,
               AG.SEQ_PLA_AGLOMERADO,
               AG.SIGLA_AGLOMERADO,
               AG.DESC_AGLOMERADO,
               FA.SEQ_PLA_FAZENDA,
               FA.SIGLA_FAZENDA,
               FA.DESC_FAZENDA,
               TA.SEQ_PLA_TALHAO,
               TA.NUMERO_TALHAO,
               TA.DESC_TALHAO,
               0 AS QTDE_HA_PREVISAO,
               AA.AREA_APONTADA AS QTDE_HA_REALIZADO,
               NULL AS DATA_PREVISAO_PLANTIO,
               AA.DATA_APONTAMENTO AS DATA_PLANTIO
          FROM AGNEW.APONT_AREA_TAL_OS    AA,
               AGNEW.ORDEM_SERVICO        OSS,
               AGNEW.ORDEM_SERV_TALHOES   OST,
               AGNEW.TALHOES              TA,
               AGNEW.FAZENDAS             FA,
               AGNEW.TIPO_CULTURA         TC,
               AGNEW.SAFRAS               SA,
               AGNEW.AGLOMERADOS          AG,
               AGNEW.TAREFAS              TAR,
               AGNEW.GRUPO_TAREFAS        GTAR,
               AGNEW.VINCULA_TAREFA_GRUPO VINTARGRU,
               AGNEW.REGIOES_GERENCIAIS   RE,
               AGNEW.EMPRESAS           EM,
               AGNEW.FILIAIS            FI
         WHERE AA.SEQ_PLA_ORDEM = OSS.SEQ_PLA_ORDEM
           AND AA.SEQ_PLA_TALHAO = TA.SEQ_PLA_TALHAO
           AND TA.SEQ_PLA_FAZENDA = FA.SEQ_PLA_FAZENDA
           AND AA.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT
           AND AA.COD_SAFRA = SA.COD_SAFRA
           AND FA.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO
           AND AG.SEQ_PLA_REGIAO = RE.SEQ_PLA_REGIAO
           AND OSS.SEQ_PLA_VINC_TAREFA = VINTARGRU.SEQ_PLA_VINC_TAREFA
           AND VINTARGRU.SEQ_PLA_TAREFA = TAR.SEQ_PLA_TAREFA
           AND VINTARGRU.SEQ_PLA_GRUPO_TAREFA = GTAR.SEQ_PLA_GRUPO_TAREFA
           AND OSS.SEQ_PLA_ORDEM = OST.SEQ_PLA_ORDEM
           AND AG.COD_EMPRESA = EM.COD_EMPRESA
           AND AG.COD_EMPRESA = FI.COD_EMPRESA
           AND AG.COD_FILIAL = FI.COD_FILIAL
           AND TAR.PLANTIO = 'S'
                    ) TT;
