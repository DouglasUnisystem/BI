CREATE MATERIALIZED VIEW VW_CONTAS_PAGREC
REFRESH FORCE ON DEMAND
START WITH SYSDATE NEXT SYSDATE + 5/24/60 
AS
/*CONTAS A RECEBER*/
SELECT
  CR.SEQ_PLA_RECEBER AS SEQ_PLA,        
  CR.DOCUMENTO,
  'RECEBER' AS TIPO,       
  CR.COD_EMPRESA, 
  EMP.NOME_EMPRESA,
  CR.COD_FILIAL, 
  FI.DESCRICAO_FILIAL,
  CR.SEQ_PLA_TIPO_DOC, 
  TD.DESC_TIPO_DOCUM,
  CR.COD_SAFRA, 
  SA.DESCRICAO_SAFRA,
  GE.SEQ_PLA_FAZENDA, 
  FA.DESC_FAZENDA,
  FA.SIGLA_FAZENDA,
  FA.SEQ_PLA_AGLOMERADO, 
  AG.DESC_AGLOMERADO,
  CR.SEQ_PLA_ENDERECO, 
  CLI.NOME_CLIENTE,
  CLI.SEQ_PLA_CLIENTE,
  CR.DATA_LCTO,
  CR.DATA_VCTO,
  GE.DATA_MVTO,
  CR.VALOR_DOCUM,
  CR.VALOR_REAL,
  0 AS VALOR_REAL_ORIGEM,
  CR.VALOR_JUROS,
  CR.VALOR_DESCONTO,
  0 AS VALOR_JUROS_OUTROS,
  0 AS VALOR_DESCONTO_OUTROS,
  CR.VALOR_COMPENSADO,
  CR.VALOR_RETENCAO,
  CR.VALOR_MULTA,
  0 AS VALOR_VARIACAO,
  CR.VALOR_RECEBIDO,
  MO.DESC_MOEDA,
  CR.INDICE_ORIGEM,
  CR.SEQ_PLA_MOEDA,
  GP.COD_TABELA,
  GP.COD_CONTA,
  GP.COD_REDUZIDO,
  GP.NOME_CONTA,
  GP.COD_GRUPO_CONTA,
 -- (SELECT GPC.NOME_CONTA FROM BANCO.GER_PLANO_CONTAS GPC WHERE GPC.COD_REDUZIDO = GP.COD_GRUPO_CONTA) AS NOME_GRUPO_CONTA,
  GP.ULTIMO_GRAU,
  GP.DEBITO_CREDITO
FROM 
  BANCO.CONTAS_RECEBER CR, 
  BANCO.GER_MOVIMENTO GE, 
  BANCO.GER_PLANO_CONTAS GP,
  BANCO.MOEDAS MO,
  BANCO.VW_CLIENTE_ENDERECO CLI,
  BANCO.SAFRAS SA,
  BANCO.FAZENDAS FA,
  BANCO.EMPRESAS EMP,
  BANCO.FILIAIS FI,
  BANCO.TIPO_DOCUMENTO TD,
  BANCO.AGLOMERADOS AG
WHERE
CR.SEQ_PLA_MOEDA = MO.SEQ_PLA_MOEDA AND
CR.SEQ_PLA_ENDERECO = CLI.SEQ_PLA_ENDERECO AND
CR.SEQ_PLA_TIPO_DOC = TD.SEQ_PLA_TIPO_DOC AND 
CR.COD_SAFRA = SA.COD_SAFRA AND
CR.COD_FILIAL = FI.COD_FILIAL AND
CR.COD_EMPRESA = EMP.COD_EMPRESA AND
FI.COD_EMPRESA = EMP.COD_EMPRESA AND
CR.SEQ_PLA_RECEBER = GE.SEQ_PLA_ORIGEM (+) AND
GE.COD_CONTA = GP.COD_CONTA (+) AND
GE.COD_TABELA = GP.COD_TABELA (+) AND
GE.SEQ_PLA_FAZENDA = FA.SEQ_PLA_FAZENDA (+) AND
FA.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO 

UNION ALL

/*CONTAS A PAGAR*/
SELECT 
  CP.SEQ_PLA_PAGAR  AS SEQ_PLA,
  CP.DOCUMENTO,
  'PAGAR' AS TIPO,
  CP.COD_EMPRESA, 
  EMP.NOME_EMPRESA,
  CP.COD_FILIAL, 
  FI.DESCRICAO_FILIAL,
  CP.SEQ_PLA_TIPO_DOC, 
  TD.DESC_TIPO_DOCUM,
  CP.COD_SAFRA, 
  SA.DESCRICAO_SAFRA,
  GE.SEQ_PLA_FAZENDA, 
  FA.DESC_FAZENDA,
  FA.SIGLA_FAZENDA,
  FA.SEQ_PLA_AGLOMERADO, 
  AG.DESC_AGLOMERADO,
  CP.SEQ_PLA_ENDERECO, 
  CLI.NOME_CLIENTE,
  CLI.SEQ_PLA_CLIENTE,  
  CP.DATA_LCTO,
  CP.DATA_VCTO,
  GE.DATA_MVTO,
  CP.VALOR_DOCUM,
  CP.VALOR_REAL,
  CP.VALOR_REAL_ORIGEM,
  CP.VALOR_JUROS,
  CP.VALOR_DESCONTO,
  CP.VALOR_JUROS_OUTROS,
  CP.VALOR_DESCONTO_OUTROS,
  CP.VALOR_COMPENSADO,
  CP.VALOR_RETENCAO,
  CP.VALOR_MULTA,
  CP.VALOR_VARIACAO,
  CP.VALOR_PAGO,
  MO.DESC_MOEDA,
  CP.INDICE_ORIGEM,
  CP.SEQ_PLA_MOEDA,
  GP.COD_TABELA,
  GP.COD_CONTA,
  GP.COD_REDUZIDO,
  GP.NOME_CONTA,
  GP.COD_GRUPO_CONTA,
-- (SELECT GPC.NOME_CONTA FROM BANCO.GER_PLANO_CONTAS GPC WHERE GPC.COD_REDUZIDO = GP.COD_GRUPO_CONTA) AS NOME_GRUPO_CONTA,
  GP.ULTIMO_GRAU,
  GP.DEBITO_CREDITO
FROM 
  BANCO.CONTAS_PAGAR CP, 
  BANCO.GER_MOVIMENTO GE, 
  BANCO.GER_PLANO_CONTAS GP,
  BANCO.MOEDAS MO,
  BANCO.VW_CLIENTE_ENDERECO CLI,
  BANCO.SAFRAS SA,
  BANCO.FAZENDAS FA,
  BANCO.EMPRESAS EMP,
  BANCO.FILIAIS FI,
  BANCO.TIPO_DOCUMENTO TD,
  BANCO.AGLOMERADOS AG
WHERE
CP.SEQ_PLA_MOEDA = MO.SEQ_PLA_MOEDA AND
CP.SEQ_PLA_ENDERECO = CLI.SEQ_PLA_ENDERECO AND
CP.SEQ_PLA_TIPO_DOC = TD.SEQ_PLA_TIPO_DOC AND 
CP.COD_SAFRA = SA.COD_SAFRA AND
CP.COD_FILIAL = FI.COD_FILIAL AND
CP.COD_EMPRESA = EMP.COD_EMPRESA AND
FI.COD_EMPRESA = EMP.COD_EMPRESA AND
CP.SEQ_PLA_PAGAR = GE.SEQ_PLA_ORIGEM (+) AND
GE.COD_CONTA = GP.COD_CONTA (+) AND
GE.COD_TABELA = GP.COD_TABELA (+) AND
GE.SEQ_PLA_FAZENDA = FA.SEQ_PLA_FAZENDA (+) AND
FA.SEQ_PLA_AGLOMERADO = AG.SEQ_PLA_AGLOMERADO 