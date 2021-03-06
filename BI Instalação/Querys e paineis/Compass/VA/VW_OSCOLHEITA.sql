CREATE MATERIALIZED VIEW VW_OSCOLHEITA
REFRESH FORCE ON DEMAND
START WITH SYSDATE NEXT SYSDATE + 5/24/60 
AS
SELECT
       EM.COD_EMPRESA,
       EM.NOME_EMPRESA,
       EM.SIGLA_EMPRESA,
       FI.COD_FILIAL,
       FI.DESCRICAO_FILIAL,
       OS.COD_SAFRA,
       SA.DESCRICAO_SAFRA,
       TS.SEQ_PLA_TIPO_CULT,
       TC.DESC_CULTURA,
       OST.SEQ_PLA_TALHAO,
       TA.DESC_TALHAO,
       TA.NUMERO_TALHAO,
       UV.SEQ_PLA_VARIEDADE,
       UV.DESC_VARIEDADE,
       OS.SEQ_PLA_FAZENDA,
       FA.DESC_FAZENDA,
       FA.SIGLA_FAZENDA,
       RE.SEQ_PLA_REGIAO,
       RE.DESC_REGIAO,
       AG.SEQ_PLA_AGLOMERADO,
       AG.SIGLA_AGLOMERADO,
       OST.QTD_HA_EFETIVO AS QTD_HA_EFETIVO,
       TS.QTDE_HA,
       OST.DATA_LANCAMENTO,
       GTAR.SEQ_PLA_GRUPO_TAREFA,
       GTAR.DESC_GRUPO_TAREFA,
       NVL(TS.FINALIZADO,'N') AS FINALIZADO
  FROM MVR.ORDEM_SERVICO      OS,
       MVR.ORDEM_SERV_TALHOES OST,
       MVR.TALHOES            TA,
       MVR.TALHOES_SAFRA      TS,
       MVR.TIPO_CULTURA       TC,
       MVR.UBS_VARIEDADES     UV,
       MVR.FAZENDAS           FA,
       MVR.SAFRAS             SA,
       MVR.VINCULA_TAREFA_GRUPO VINTARGRU,
       MVR.TAREFAS            TAR,
       MVR.GRUPO_TAREFAS      GTAR,
       MVR.EMPRESAS           EM,
       MVR.FILIAIS            FI,
       MVR.AGLOMERADOS        AG,
       MVR.REGIOES_GERENCIAIS RE
 WHERE
      OS.COD_SAFRA      = SA.COD_SAFRA
   AND OS.SEQ_PLA_ORDEM   = OST.SEQ_PLA_ORDEM
   AND OST.SEQ_PLA_TALHAO_SAFRA = TS.SEQ_PLA_TALHAO_SAFRA
   AND TS.SEQ_PLA_TALHAO  = TA.SEQ_PLA_TALHAO
   AND TS.COD_SAFRA       = SA.COD_SAFRA
   AND TS.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT
   AND TS.SEQ_PLA_VARIEDADE = UV.SEQ_PLA_VARIEDADE
   AND OS.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT
   AND OS.SEQ_PLA_FAZENDA   = FA.SEQ_PLA_FAZENDA
   AND FA.SEQ_PLA_FAZENDA   = TA.SEQ_PLA_FAZENDA
   AND FA.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO
   AND AG.SEQ_PLA_REGIAO = RE.SEQ_PLA_REGIAO
   AND OS.SEQ_PLA_VINC_TAREFA = VINTARGRU.SEQ_PLA_VINC_TAREFA
   AND VINTARGRU.SEQ_PLA_TAREFA = TAR.SEQ_PLA_TAREFA
   AND VINTARGRU.SEQ_PLA_GRUPO_TAREFA = GTAR.SEQ_PLA_GRUPO_TAREFA
   AND AG.COD_EMPRESA = EM.COD_EMPRESA
   AND AG.COD_EMPRESA = FI.COD_EMPRESA
   AND AG.COD_FILIAL = FI.COD_FILIAL
   AND (TAR.COLHEITA = 'S')

   AND NOT EXISTS (
   SELECT
       APT.COD_EMPRESA,
       EM.NOME_EMPRESA,
       EM.SIGLA_EMPRESA,
       APT.COD_FILIAL,
       FI.DESCRICAO_FILIAL,
       APT.COD_SAFRA,
       SA.DESCRICAO_SAFRA,
       APT.SEQ_PLA_TIPO_CULT,
       TC.DESC_CULTURA,
       APT.SEQ_PLA_TALHAO,
       TA.DESC_TALHAO,
       TA.NUMERO_TALHAO,
       APT.SEQ_PLA_VARIEDADE,
       UV.DESC_VARIEDADE,
       OS2.SEQ_PLA_FAZENDA,
       FA.DESC_FAZENDA,
       FA.SIGLA_FAZENDA,
       RE.SEQ_PLA_REGIAO,
       RE.DESC_REGIAO,
       AG.SEQ_PLA_AGLOMERADO,
       AG.SIGLA_AGLOMERADO,
       APT.AREA_APONTADA,
       APT.DATA_APONTAMENTO,
       GTAR.SEQ_PLA_GRUPO_TAREFA,
       GTAR.DESC_GRUPO_TAREFA,
       OST.QUANTIDADE_HA AS HA_PREVISTO_TOTAL_TALHAO
  FROM MVR.APONT_AREA_TAL_OS APT,
       MVR.ORDEM_SERVICO      OS2,
       MVR.ORDEM_SERV_TALHOES OST,
       MVR.TALHOES            TA,
       MVR.TIPO_CULTURA       TC,
       MVR.UBS_VARIEDADES     UV,
       MVR.FAZENDAS           FA,
       MVR.SAFRAS             SA,
       MVR.VINCULA_TAREFA_GRUPO VINTARGRU,
       MVR.TAREFAS            TAR,
       MVR.GRUPO_TAREFAS      GTAR,
       MVR.EMPRESAS           EM,
       MVR.FILIAIS            FI,
       MVR.AGLOMERADOS        AG,
       MVR.REGIOES_GERENCIAIS RE
 WHERE APT.SEQ_PLA_TALHAO = TA.SEQ_PLA_TALHAO
   AND APT.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT(+)
   AND APT.SEQ_PLA_VARIEDADE = UV.SEQ_PLA_VARIEDADE(+)
   AND APT.COD_SAFRA = SA.COD_SAFRA
   AND APT.SEQ_PLA_ORDEM = OS2.SEQ_PLA_ORDEM
   AND OS2.SEQ_PLA_ORDEM = OST.SEQ_PLA_ORDEM
   AND OS2.SEQ_PLA_FAZENDA = FA.SEQ_PLA_FAZENDA
   AND FA.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO
   AND AG.SEQ_PLA_REGIAO = RE.SEQ_PLA_REGIAO
   AND OS2.SEQ_PLA_VINC_TAREFA = VINTARGRU.SEQ_PLA_VINC_TAREFA
   AND VINTARGRU.SEQ_PLA_TAREFA = TAR.SEQ_PLA_TAREFA
   AND VINTARGRU.SEQ_PLA_GRUPO_TAREFA = GTAR.SEQ_PLA_GRUPO_TAREFA
   AND AG.COD_EMPRESA = EM.COD_EMPRESA
   AND AG.COD_EMPRESA = FI.COD_EMPRESA
   AND AG.COD_FILIAL = FI.COD_FILIAL
   AND (TAR.COLHEITA = 'S')
   AND OS.SEQ_PLA_ORDEM = OS2.SEQ_PLA_ORDEM )

   UNION ALL
 --  BUSCA TABELA SECUNDARIA QUE ? ONDE O USUARIO CONSEGUE CADASTRAR VARIOS APONTAMENTOS DE COLHEITA PARA UM MESMO CAMPO
   SELECT
       APT.COD_EMPRESA,
       EM.NOME_EMPRESA,
       EM.SIGLA_EMPRESA,
       APT.COD_FILIAL,
       FI.DESCRICAO_FILIAL,
       APT.COD_SAFRA,
       SA.DESCRICAO_SAFRA,
       APT.SEQ_PLA_TIPO_CULT,
       TC.DESC_CULTURA,
       APT.SEQ_PLA_TALHAO,
       TA.DESC_TALHAO,
       TA.NUMERO_TALHAO,
       APT.SEQ_PLA_VARIEDADE,
       UV.DESC_VARIEDADE,
       OS.SEQ_PLA_FAZENDA,
       FA.DESC_FAZENDA,
       FA.SIGLA_FAZENDA,
       RE.SEQ_PLA_REGIAO,
       RE.DESC_REGIAO,
       AG.SEQ_PLA_AGLOMERADO,
       AG.SIGLA_AGLOMERADO,
       OST.QTD_HA_EFETIVO AS QTD_HA_EFETIVO,
       APT.AREA_APONTADA,
       APT.DATA_APONTAMENTO,
       GTAR.SEQ_PLA_GRUPO_TAREFA,
       GTAR.DESC_GRUPO_TAREFA,
       NVL(TS.FINALIZADO,'N') AS FINALIZADO
  FROM MVR.APONT_AREA_TAL_OS  APT,
       MVR.ORDEM_SERVICO      OS,
       MVR.ORDEM_SERV_TALHOES OST,
       MVR.TALHOES            TA,
       MVR.TALHOES_SAFRA      TS,
       MVR.TIPO_CULTURA       TC,
       MVR.UBS_VARIEDADES     UV,
       MVR.FAZENDAS           FA,
       MVR.SAFRAS             SA,
       MVR.VINCULA_TAREFA_GRUPO VINTARGRU,
       MVR.TAREFAS            TAR,
       MVR.GRUPO_TAREFAS      GTAR,
       MVR.EMPRESAS           EM,
       MVR.FILIAIS            FI,
       MVR.AGLOMERADOS        AG,
       MVR.REGIOES_GERENCIAIS RE
 WHERE APT.SEQ_PLA_TALHAO = TA.SEQ_PLA_TALHAO
   AND TA.SEQ_PLA_TALHAO  = TS.SEQ_PLA_TALHAO
   AND APT.COD_SAFRA      = TS.COD_SAFRA
   AND APT.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT
   AND APT.SEQ_PLA_VARIEDADE = UV.SEQ_PLA_VARIEDADE
   AND TS.SEQ_PLA_TIPO_CULT = TC.SEQ_PLA_TIPO_CULT
   AND TS.SEQ_PLA_VARIEDADE = UV.SEQ_PLA_VARIEDADE
   AND APT.COD_SAFRA = SA.COD_SAFRA
   AND APT.SEQ_PLA_ORDEM = OS.SEQ_PLA_ORDEM
   AND OS.SEQ_PLA_ORDEM = OST.SEQ_PLA_ORDEM
   AND OS.SEQ_PLA_FAZENDA = FA.SEQ_PLA_FAZENDA
   AND FA.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO
   AND AG.SEQ_PLA_REGIAO = RE.SEQ_PLA_REGIAO
   AND OS.SEQ_PLA_VINC_TAREFA = VINTARGRU.SEQ_PLA_VINC_TAREFA
   AND VINTARGRU.SEQ_PLA_TAREFA = TAR.SEQ_PLA_TAREFA
   AND VINTARGRU.SEQ_PLA_GRUPO_TAREFA = GTAR.SEQ_PLA_GRUPO_TAREFA
   AND AG.COD_EMPRESA = EM.COD_EMPRESA
   AND AG.COD_EMPRESA = FI.COD_EMPRESA
   AND AG.COD_FILIAL = FI.COD_FILIAL
   AND (TAR.COLHEITA = 'S');
