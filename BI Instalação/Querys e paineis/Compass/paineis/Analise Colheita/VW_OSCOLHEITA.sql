CREATE MATERIALIZED VIEW VW_OSCOLHEITA
REFRESH FORCE ON DEMAND
START WITH SYSDATE  NEXT SYSDATE + 5/24/60 
AS
SELECT X.*
  FROM ( SELECT OSS.NR_CONTROLE,
               EM.COD_EMPRESA,
               EM.NOME_EMPRESA,
               EM.SIGLA_EMPRESA,
               FI.COD_FILIAL,
               FI.DESCRICAO_FILIAL,
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
               V.SEQ_PLA_VARIEDADE,
               V.DESC_VARIEDADE,
               OST.QTD_HA_EFETIVO AS QTD_HA_EFETIVO,
               TS.QTDE_HA,
               NVL(OSS.DATA_FECHAMENTO, TS.DATA_PLANTIO) AS DATA_LANCAMENTO,
               NVL(TS.FINALIZADO,'N') AS FINALIZADO
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
         WHERE OSS.COD_SAFRA      = SA.COD_SAFRA
         AND OSS.SEQ_PLA_ORDEM   = OST.SEQ_PLA_ORDEM
         AND OST.SEQ_PLA_TALHAO_SAFRA = TS.SEQ_PLA_TALHAO_SAFRA
         AND TS.SEQ_PLA_TALHAO  = TA.SEQ_PLA_TALHAO
         AND TS.COD_SAFRA       = SA.COD_SAFRA
         AND TS.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT
         AND TS.SEQ_PLA_VARIEDADE = V.SEQ_PLA_VARIEDADE
         AND OSS.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT
         AND OSS.SEQ_PLA_FAZENDA   = FA.SEQ_PLA_FAZENDA
         AND FA.SEQ_PLA_FAZENDA   = TA.SEQ_PLA_FAZENDA
         AND FA.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO
         AND AG.SEQ_PLA_REGIAO = RE.SEQ_PLA_REGIAO
         AND OSS.SEQ_PLA_VINC_TAREFA = VINTARGRU.SEQ_PLA_VINC_TAREFA
         AND VINTARGRU.SEQ_PLA_TAREFA = TAR.SEQ_PLA_TAREFA
         AND VINTARGRU.SEQ_PLA_GRUPO_TAREFA = GTAR.SEQ_PLA_GRUPO_TAREFA
         AND AG.COD_EMPRESA = EM.COD_EMPRESA
         AND AG.COD_EMPRESA = FI.COD_EMPRESA
         AND AG.COD_FILIAL = FI.COD_FILIAL
         AND NOT EXISTS
         (SELECT *
                FROM AGNEW.APONT_AREA_TAL_OS A
                WHERE A.SEQ_PLA_ORDEM = OSS.SEQ_PLA_ORDEM
                 AND A.SEQ_PLA_TALHAO = TA.SEQ_PLA_TALHAO)
           AND TAR.COLHEITA = 'S'
           AND OST.EFETIVO = 'S'

        UNION ALL

        SELECT OSS.NR_CONTROLE,
               EM.COD_EMPRESA,
               EM.NOME_EMPRESA,
               EM.SIGLA_EMPRESA,
               FI.COD_FILIAL,
               FI.DESCRICAO_FILIAL,
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
               V.SEQ_PLA_VARIEDADE,
               V.DESC_VARIEDADE,
               AA.AREA_APONTADA AS QTD_HA_EFETIVO,
               TS.QTDE_HA,
               AA.DATA_APONTAMENTO AS DATA_LANCAMENTO,
               NVL(TS.FINALIZADO,'N') AS FINALIZADO
          FROM AGNEW.APONT_AREA_TAL_OS    AA,
               AGNEW.ORDEM_SERVICO        OSS,
               AGNEW.ORDEM_SERV_TALHOES   OST,
               AGNEW.TALHOES              TA,
               AGNEW.TALHOES_SAFRA        TS,
               AGNEW.FAZENDAS             FA,
               AGNEW.UBS_VARIEDADES       V,
               AGNEW.TIPO_CULTURA         TC,
               AGNEW.SAFRAS               SA,
               AGNEW.AGLOMERADOS          AG,
               AGNEW.TAREFAS              TAR,
               AGNEW.GRUPO_TAREFAS        GTAR,
               AGNEW.VINCULA_TAREFA_GRUPO VINTARGRU,
               AGNEW.REGIOES_GERENCIAIS   RE,
               AGNEW.EMPRESAS           EM,
                AGNEW.FILIAIS            FI
         WHERE AA.SEQ_PLA_TALHAO = TA.SEQ_PLA_TALHAO
   AND TA.SEQ_PLA_TALHAO  = TS.SEQ_PLA_TALHAO
   AND AA.COD_SAFRA      = TS.COD_SAFRA
   AND AA.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT
   AND AA.SEQ_PLA_VARIEDADE = V.SEQ_PLA_VARIEDADE
   AND TS.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT
   AND TS.SEQ_PLA_VARIEDADE = V.SEQ_PLA_VARIEDADE
   AND AA.COD_SAFRA = SA.COD_SAFRA
   AND AA.SEQ_PLA_ORDEM = OSS.SEQ_PLA_ORDEM
   AND OSS.SEQ_PLA_ORDEM = OST.SEQ_PLA_ORDEM
   AND OSS.SEQ_PLA_FAZENDA = FA.SEQ_PLA_FAZENDA
   AND FA.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO
   AND AG.SEQ_PLA_REGIAO = RE.SEQ_PLA_REGIAO
   AND OSS.SEQ_PLA_VINC_TAREFA = VINTARGRU.SEQ_PLA_VINC_TAREFA
   AND VINTARGRU.SEQ_PLA_TAREFA = TAR.SEQ_PLA_TAREFA
   AND VINTARGRU.SEQ_PLA_GRUPO_TAREFA = GTAR.SEQ_PLA_GRUPO_TAREFA
   AND AG.COD_EMPRESA = EM.COD_EMPRESA
   AND AG.COD_EMPRESA = FI.COD_EMPRESA
   AND AG.COD_FILIAL = FI.COD_FILIAL
           AND TAR.COLHEITA = 'S'    ) X;
