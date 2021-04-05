CREATE MATERIALIZED VIEW VW_GER_MOVIMENTO_RECEITA_SACA REFRESH FORCE ON DEMAND START WITH TO_DATE('24-03-2021 16:45:08', 'DD-MM-YYYY HH24:MI:SS') NEXT SYSDATE + 10/24/60 AS
SELECT DISTINCT EM.COD_EMPRESA, GE.COD_FILIAL, GE.COD_SAFRA,
        EM.NOME_EMPRESA,
        GP.SEQ_PLANILHA AS SEQ_PLA_CONTA,
        GP.COD_CONTA,
        GP.COD_TABELA,
        GP.NOME_CONTA,
        CL.NOME_CLIENTE,
        GE.SEQ_PLA_MOV_GER, GE.SEQ_PLA_FAZENDA,
        TRUNC(GE.DATA_MVTO)DATA_MVTO,
        GE.VALOR_REAL,
        TC.SEQ_PLA_TIPO_CULT,
        TC.DESC_CULTURA,
        '%' DESCRICAO_GRUPO
   FROM AGRICOLA.GER_PLANO_CONTAS   GP,
        AGRICOLA.GER_MOVIMENTO      GE,
        AGRICOLA.CONTAS_RECEBER     CR,
        AGRICOLA.EST_SAIDAS         ES,
        AGRICOLA.EST_SAIDAS_ITENS   EI,
        AGRICOLA.PRODUTOS           PD,
        AGRICOLA.SUB_GRUPO          SB,
        AGRICOLA.GRUPO              GR,
        AGRICOLA.TIPO_CULTURA       TC,
        AGRICOLA.EMPRESAS           EM,
        AGRICOLA.CLIENTES_ENDERECOS CE,
        AGRICOLA.CLIENTES           CL
  WHERE GP.COD_TABELA        = GE.COD_TABELA
    AND GP.COD_CONTA         = GE.COD_CONTA
    AND GE.SEQ_PLA_RECEBER   = CR.SEQ_PLA_RECEBER
    AND CR.SEQ_PLA_ORIGEM    = ES.SEQ_PLA_SAIDA
    AND ES.SEQ_PLA_SAIDA     = EI.SEQ_PLA_SAIDA
    AND EI.SEQ_PLA_PRODUTO   = TC.SEQ_PLA_PRODUTO
    AND EI.SEQ_PLA_PRODUTO   = PD.SEQ_PLA_PRODUTO
    AND PD.SEQ_PLA_SUB_GRUPO = SB.SEQ_PLA_SUB_GRUPO
    AND SB.SEQ_PLA_GRUPO     = GR.SEQ_PLA_GRUPO
    AND GE.COD_TABELA        = EM.COD_TABELA_GERENCIAL
    AND GE.COD_EMPRESA       = EM.COD_EMPRESA
    AND GE.SEQ_PLA_ENDERECO  = CE.SEQ_PLA_ENDERECO
    AND CE.SEQ_PLA_CLIENTE   = CL.SEQ_PLA_CLIENTE
    AND GP.DEBITO_CREDITO    = 'C'
    AND GE.SEQ_PLA_MOV_FIN IS NULL
--CR COM ORIGEM CONTRATO_SAIDA COM PRODUTO NA TIPO_CULTURA
UNION ALL
 SELECT DISTINCT EM.COD_EMPRESA, GE.COD_FILIAL, GE.COD_SAFRA,
        EM.NOME_EMPRESA,
        GP.SEQ_PLANILHA AS SEQ_PLA_CONTA,
        GP.COD_CONTA,
        GP.COD_TABELA,
        GP.NOME_CONTA,
        CL.NOME_CLIENTE,
        GE.SEQ_PLA_MOV_GER, GE.SEQ_PLA_FAZENDA,
        TRUNC(GE.DATA_MVTO)DATA_MVTO,
        GE.VALOR_REAL,
        TC.SEQ_PLA_TIPO_CULT,
        TC.DESC_CULTURA,
        '%' DESCRICAO_GRUPO
   FROM AGRICOLA.GER_PLANO_CONTAS GP,
        AGRICOLA.GER_MOVIMENTO    GE,
        AGRICOLA.CONTAS_RECEBER   CR,
        AGRICOLA.CONTRATO_SAIDA   CS,
        AGRICOLA.PRODUTOS         PD,
        AGRICOLA.SUB_GRUPO        SB,
        AGRICOLA.GRUPO            GR,
        AGRICOLA.TIPO_CULTURA     TC,
        AGRICOLA.EMPRESAS         EM,
        AGRICOLA.CLIENTES_ENDERECOS CE,
        AGRICOLA.CLIENTES           CL
  WHERE GP.COD_TABELA        = GE.COD_TABELA
    AND GP.COD_CONTA         = GE.COD_CONTA
    AND GE.SEQ_PLA_RECEBER   = CR.SEQ_PLA_RECEBER
    AND CR.SEQ_PLA_ORIGEM    = CS.SEQ_PLA_PED_VENDA
    AND CS.SEQ_PLA_PRODUTO   = TC.SEQ_PLA_PRODUTO
    AND CS.SEQ_PLA_PRODUTO   = PD.SEQ_PLA_PRODUTO
    AND PD.SEQ_PLA_SUB_GRUPO = SB.SEQ_PLA_SUB_GRUPO
    AND SB.SEQ_PLA_GRUPO     = GR.SEQ_PLA_GRUPO
    AND GE.COD_TABELA        = EM.COD_TABELA_GERENCIAL
    AND GE.COD_EMPRESA       = EM.COD_EMPRESA
    AND GE.SEQ_PLA_ENDERECO  = CE.SEQ_PLA_ENDERECO
    AND CE.SEQ_PLA_CLIENTE   = CL.SEQ_PLA_CLIENTE
    AND GP.DEBITO_CREDITO    = 'C'
    AND GE.SEQ_PLA_MOV_FIN IS NULL
--CR COM ORIGEM EST_SAIDAS SEM PRODUTO NA TIPO_CULTURA
UNION ALL
SELECT DISTINCT EM.COD_EMPRESA, GE.COD_FILIAL, GE.COD_SAFRA,
        EM.NOME_EMPRESA,
        GP.SEQ_PLANILHA AS SEQ_PLA_CONTA,
        GP.COD_CONTA,
        GP.COD_TABELA,
        GP.NOME_CONTA,
        CL.NOME_CLIENTE,
        GE.SEQ_PLA_MOV_GER, GE.SEQ_PLA_FAZENDA,
        TRUNC(GE.DATA_MVTO)DATA_MVTO,
        GE.VALOR_REAL,
        '' SEQ_PLA_TIPO_CULT,
        '' DESC_CULTURA,
        '%' DESCRICAO_GRUPO
   FROM AGRICOLA.GER_PLANO_CONTAS   GP,
        AGRICOLA.GER_MOVIMENTO      GE,
        AGRICOLA.CONTAS_RECEBER     CR,
        AGRICOLA.EST_SAIDAS         ES,
        AGRICOLA.EST_SAIDAS_ITENS   EI,
        AGRICOLA.PRODUTOS           PD,
        AGRICOLA.SUB_GRUPO          SB,
        AGRICOLA.GRUPO              GR,
        AGRICOLA.EMPRESAS           EM,
        AGRICOLA.CLIENTES_ENDERECOS CE,
        AGRICOLA.CLIENTES           CL
  WHERE GP.COD_TABELA        = GE.COD_TABELA
    AND GP.COD_CONTA         = GE.COD_CONTA
    AND GE.SEQ_PLA_RECEBER   = CR.SEQ_PLA_RECEBER
    AND CR.SEQ_PLA_ORIGEM    = ES.SEQ_PLA_SAIDA
    AND ES.SEQ_PLA_SAIDA     = EI.SEQ_PLA_SAIDA
    AND EI.SEQ_PLA_PRODUTO   = PD.SEQ_PLA_PRODUTO
    AND PD.SEQ_PLA_SUB_GRUPO = SB.SEQ_PLA_SUB_GRUPO
    AND SB.SEQ_PLA_GRUPO     = GR.SEQ_PLA_GRUPO
    AND GE.COD_TABELA        = EM.COD_TABELA_GERENCIAL
    AND GE.COD_EMPRESA       = EM.COD_EMPRESA
    AND GE.SEQ_PLA_ENDERECO  = CE.SEQ_PLA_ENDERECO
    AND CE.SEQ_PLA_CLIENTE   = CL.SEQ_PLA_CLIENTE
    AND NOT EXISTS (SELECT TC.SEQ_PLA_PRODUTO FROM AGRICOLA.TIPO_CULTURA TC WHERE EI.SEQ_PLA_PRODUTO = TC.SEQ_PLA_PRODUTO)
    AND GP.DEBITO_CREDITO    = 'C'
    AND GE.SEQ_PLA_MOV_FIN IS NULL
--CR COM ORIGEM CONTRATO_SAIDA SEM PRODUTO NA TIPO_CULTURA
UNION ALL
SELECT DISTINCT EM.COD_EMPRESA, GE.COD_FILIAL, GE.COD_SAFRA,
        EM.NOME_EMPRESA,
        GP.SEQ_PLANILHA AS SEQ_PLA_CONTA,
        GP.COD_CONTA,
        GP.COD_TABELA,
        GP.NOME_CONTA,
        CL.NOME_CLIENTE,
        GE.SEQ_PLA_MOV_GER, GE.SEQ_PLA_FAZENDA,
        TRUNC(GE.DATA_MVTO)DATA_MVTO,
        GE.VALOR_REAL,
        '' SEQ_PLA_TIPO_CULT,
        '' DESC_CULTURA,
        '%' DESCRICAO_GRUPO
   FROM AGRICOLA.GER_PLANO_CONTAS   GP,
        AGRICOLA.GER_MOVIMENTO      GE,
        AGRICOLA.CONTAS_RECEBER     CR,
        AGRICOLA.CONTRATO_SAIDA     CS,
        AGRICOLA.PRODUTOS           PD,
        AGRICOLA.SUB_GRUPO          SB,
        AGRICOLA.GRUPO              GR,
        AGRICOLA.EMPRESAS           EM,
        AGRICOLA.CLIENTES_ENDERECOS CE,
        AGRICOLA.CLIENTES           CL
  WHERE GP.COD_TABELA        = GE.COD_TABELA
    AND GP.COD_CONTA         = GE.COD_CONTA
    AND GE.SEQ_PLA_RECEBER   = CR.SEQ_PLA_RECEBER
    AND CR.SEQ_PLA_ORIGEM    = CS.SEQ_PLA_PED_VENDA
    AND CS.SEQ_PLA_PRODUTO   = PD.SEQ_PLA_PRODUTO
    AND PD.SEQ_PLA_SUB_GRUPO = SB.SEQ_PLA_SUB_GRUPO
    AND SB.SEQ_PLA_GRUPO     = GR.SEQ_PLA_GRUPO
    AND GE.COD_TABELA        = EM.COD_TABELA_GERENCIAL
    AND GE.COD_EMPRESA       = EM.COD_EMPRESA
    AND GE.SEQ_PLA_ENDERECO  = CE.SEQ_PLA_ENDERECO
    AND CE.SEQ_PLA_CLIENTE   = CL.SEQ_PLA_CLIENTE
    AND NOT EXISTS (SELECT TC.SEQ_PLA_PRODUTO FROM AGRICOLA.TIPO_CULTURA TC WHERE CS.SEQ_PLA_PRODUTO = TC.SEQ_PLA_PRODUTO)
    AND GP.DEBITO_CREDITO    = 'C'
    AND GE.SEQ_PLA_MOV_FIN IS NULL
--CR SEM ORIGEM, OU SEJA, N�O TEM PRODUTO NA TIPO_CULTURA
UNION ALL
SELECT DISTINCT EM.COD_EMPRESA, GE.COD_FILIAL, GE.COD_SAFRA,
        EM.NOME_EMPRESA,
        GP.SEQ_PLANILHA AS SEQ_PLA_CONTA,
        GP.COD_CONTA,
        GP.COD_TABELA,
        GP.NOME_CONTA,
        CL.NOME_CLIENTE,
        GE.SEQ_PLA_MOV_GER, GE.SEQ_PLA_FAZENDA,
        TRUNC(GE.DATA_MVTO)DATA_MVTO,
        GE.VALOR_REAL,
        '' SEQ_PLA_TIPO_CULT,
        '' DESC_CULTURA,
        '%' DESCRICAO_GRUPO
   FROM AGRICOLA.GER_PLANO_CONTAS   GP,
        AGRICOLA.GER_MOVIMENTO      GE,
        AGRICOLA.CONTAS_RECEBER     CR,
        AGRICOLA.EMPRESAS           EM,
        AGRICOLA.CLIENTES_ENDERECOS CE,
        AGRICOLA.CLIENTES           CL
  WHERE GP.COD_TABELA       = GE.COD_TABELA
    AND GP.COD_CONTA        = GE.COD_CONTA
    AND GE.SEQ_PLA_RECEBER  = CR.SEQ_PLA_RECEBER
    AND GE.COD_TABELA       = EM.COD_TABELA_GERENCIAL
    AND GE.COD_EMPRESA      = EM.COD_EMPRESA
    AND GE.SEQ_PLA_ENDERECO = CE.SEQ_PLA_ENDERECO
    AND CE.SEQ_PLA_CLIENTE  = CL.SEQ_PLA_CLIENTE
    AND CR.SEQ_PLA_ORIGEM  IS NULL
    AND GE.SEQ_PLA_MOV_FIN IS NULL
    AND GP.DEBITO_CREDITO   = 'C';
