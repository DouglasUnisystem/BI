CREATE OR REPLACE VIEW VW_NDF AS
SELECT
       EP.COD_EMPRESA,
       EP.SIGLA_EMPRESA,
       FI.COD_FILIAL,
       FI.SIGLA_FILIAL,
       TRUNC(CP.DATA_VCTO) AS DATA_VCTO,
       NVL(CP.VALOR_DOCUM,0)+NVL(CP.VALOR_JUROS,0)-NVL(CP.VALOR_DESCONTO,0) AS VALOR_DOLAR,
       'PAGAR ABERTO' AS STATUS,
       CL.NOME_CLIENTE,
       CP.DOCUMENTO,
       CP.OBSERVACOES,
       'Contas a Pagar' AS ORIGEM
  FROM AGNEW.CONTAS_PAGAR CP,
       AGNEW.PAR_GERAL PG,
       AGNEW.EMPRESAS EP,
       AGNEW.FILIAIS FI,
       AGNEW.TABELA_PADRAO_LCTOS TP,
       AGNEW.CLIENTES CL,
       AGNEW.CLIENTES_ENDERECOS CE
 WHERE CP.SEQ_PLA_MOEDA = PG.MOEDA_DOLAR
   AND CP.SEQ_PLA_ENDERECO = CE.SEQ_PLA_ENDERECO
   AND CE.SEQ_PLA_CLIENTE = CL.SEQ_PLA_CLIENTE
   AND NOT EXISTS (SELECT * FROM AGNEW.PAGAR_PAGTOS PP WHERE PP.SEQ_PLA_PAGAR = CP.SEQ_PLA_PAGAR)
   AND NOT EXISTS (SELECT * FROM AGNEW.FIN_MOVIMENTO FF WHERE FF.SEQ_PLA_MOV_FIN = CP.SEQ_PLA_ORIGEM)
   AND CP.COD_EMPRESA = EP.COD_EMPRESA
   AND CP.COD_EMPRESA = FI.COD_EMPRESA
   AND CP.COD_FILIAL = FI.COD_FILIAL
   AND NVL(CP.VALOR_DOCUM,0)+NVL(CP.VALOR_JUROS,0)-NVL(CP.VALOR_DESCONTO,0) > 0
   AND CP.T001_TIPO_PAGTO = TP.SEQ_PLA_TAB_PADRAO
   AND TP.COD_LCTO <> 'E'
UNION ALL
SELECT
       EP.COD_EMPRESA,
       EP.SIGLA_EMPRESA,
       FI.COD_FILIAL,
       FI.SIGLA_FILIAL,
       TRUNC(CR.DATA_VCTO) AS DATA_VCTO,
       NVL(CR.VALOR_DOCUM,0)+NVL(CR.VALOR_JUROS,0)-NVL(CR.VALOR_DESCONTO,0) AS VALOR_DOLAR,
       'RECEBER ABERTO' AS STATUS,
       CL.NOME_CLIENTE,
       CR.DOCUMENTO,
       CR.OBSERVACOES,
       'Contas a Receber' AS ORIGEM
  FROM AGNEW.CONTAS_RECEBER CR,
       AGNEW.PAR_GERAL PG,
       AGNEW.EMPRESAS EP,
       AGNEW.FILIAIS FI,
       AGNEW.TABELA_PADRAO_LCTOS TP,
       AGNEW.CLIENTES CL,
       AGNEW.CLIENTES_ENDERECOS CE
 WHERE CR.SEQ_PLA_MOEDA = PG.MOEDA_DOLAR
   AND CR.SEQ_PLA_ENDERECO = CE.SEQ_PLA_ENDERECO
   AND CE.SEQ_PLA_CLIENTE = CL.SEQ_PLA_CLIENTE
   AND NOT EXISTS (SELECT * FROM AGNEW.RECEBER_PAGTOS RP WHERE RP.SEQ_PLA_RECEBER = CR.SEQ_PLA_RECEBER)
   AND NOT EXISTS (SELECT * FROM AGNEW.PAGAR_PAGTOS PP WHERE PP.SEQ_PLA_PAGAR_LCTOS = CR.SEQ_PLA_ORIGEM)
   AND CR.COD_EMPRESA = EP.COD_EMPRESA
   AND CR.COD_EMPRESA = FI.COD_EMPRESA
   AND CR.COD_FILIAL = FI.COD_FILIAL
   AND NVL(CR.VALOR_DOCUM,0)+NVL(CR.VALOR_JUROS,0)-NVL(CR.VALOR_DESCONTO,0) > 0
   AND CR.T002_TIPO_RECTO = TP.SEQ_PLA_TAB_PADRAO
   AND TP.COD_LCTO <> 'E'
UNION ALL
SELECT EP.COD_EMPRESA,
       EP.SIGLA_EMPRESA,
       FI.COD_FILIAL,
       FI.SIGLA_FILIAL,
       TRUNC(P.DATA_VCTO) AS DATA_VCTO,
       P.VALOR_PREVISTO - NVL(BX.VALOR,0) AS VALOR_DOLAR,
       CASE WHEN P.OPERACAO = 'D' THEN 'PREVISAO PAGAR ABERTO' ELSE 'PREVISAO RECEBER ABERTO' END AS STATUS,
       CL.NOME_CLIENTE,
       TT.DOCUMENTO,
       P.OBSERVACOES,
       'Previs�o - '||TT.ORIGEM
  FROM AGNEW.PREVISOES_FINANCEIRAS P,
       AGNEW.CLIENTES_ENDERECOS CE,
       AGNEW.CLIENTES CL,
       AGNEW.EMPRESAS EP,
       AGNEW.FILIAIS FI,
       (SELECT ROUND(SUM(LPP.VALOR*DECODE(LPP.TIPO_OPERACAO,'D',-1,DECODE(LPP.TIPO_OPERACAO,'R',-1,1))),2) AS VALOR,
               LP.SEQ_PLA_PREVISAO
          FROM AGNEW.LIGA_PRODUTO_PROVISAO LPP,
               AGNEW.LIGA_PREVISAO_PRODUTO LP,
               AGNEW.PREVISOES_FINANCEIRAS PF
         WHERE NVL(LPP.COMPENSADO,'N') = 'N'
           AND PF.SEQ_PLA_PREVISAO = LP.SEQ_PLA_PREVISAO
           AND LP.SEQ_PLA_LIGA = LPP.SEQ_PLA_PREV_PROD
         GROUP BY LP.SEQ_PLA_PREVISAO) BX,
       (SELECT PC.SEQ_PLA_PEDIDO AS SEQ_PLA_ORIGEM, PC.COD_EMPRESA, PC.COD_FILIAL, PC.NR_PEDIDO AS DOCUMENTO, PC.SEQ_PLA_ENDERECO, 'Pedido de Compra' AS ORIGEM FROM AGNEW.PEDIDO_COMPRAS PC
         UNION ALL
        SELECT PV.SEQ_PLA_PEDIDO AS SEQ_PLA_ORIGEM, PV.COD_EMPRESA, PV.COD_FILIAL, PV.NR_PEDIDO AS DOCUMENTO, PV.SEQ_PLA_ENDERECO, 'Pedido de Venda' AS ORIGEM FROM AGNEW.PEDIDO_VENDAS PV
         UNION ALL
        SELECT CP.SEQ_PLA_CONTRATO AS SEQ_PLA_ORIGEM, CP.COD_EMPRESA, CP.COD_FILIAL, CP.NR_CONTRATO AS DOCUMENTO, CP.SEQ_PLA_ENDERECO, 'Contrato de Venda' AS ORIGEM  FROM AGNEW.CONT_PRINCIPAL CP
         UNION ALL
        SELECT IE.SEQ_PLA_INSTRUCAO AS SEQ_PLA_ORIGEM, IE.COD_EMPRESA, IE.COD_FILIAL, TO_CHAR(IE.NR_INSTRUCAO) AS DOCUMENTO, IE.SEQ_PLA_ENDERECO, 'Instru��o de embarque' AS ORIGEM FROM AGNEW.INSTRUCAO_EMBARQUE IE
         UNION ALL
        SELECT NG.SEQ_PLA_NEGOCIACAO AS SEQ_PLA_ORIGEM, NG.COD_EMPRESA, NG.COD_FILIAL, TO_CHAR(NG.NR_NEGOCIACAO) AS DOCUMENTO, NG.SEQ_PLA_CLI_NEGOCIACAO, 'Negocia��o de Compra de Gr�os' AS ORIGEM FROM AGNEW.NEGOCIACAO_GRAOS NG
         UNION ALL
        SELECT NA.SEQ_PLA_NEGOCIACAO AS SEQ_PLA_ORIGEM, NA.COD_EMPRESA, NA.COD_FILIAL, TO_CHAR(NA.NR_NEGOCIACAO) AS DOCUMENTO, NA.SEQ_PLA_CLI_NEGOCIACAO, 'Negocia��o de Compra de Algod�o' AS ORIGEM FROM AGNEW.NEGOCIACAO_ALGODAO NA
         UNION ALL
        SELECT NF.SEQ_PLA_NEGOCIACAO AS SEQ_PLA_ORIGEM, NF.COD_EMPRESA, NF.COD_FILIAL, NF.NR_NEGOCIACAO AS DOCUMENTO, NF.SEQ_PLA_CLI_NEGOCIACAO, 'Negociacao de Compra Forrageira' AS ORIGEM FROM AGNEW.NEGOCIACAO_FORRAGEIRA NF) TT
 WHERE NVL(P.FINALIZADO,'N') = 'N'
   AND P.SEQ_PLA_PREVISAO = BX.SEQ_PLA_PREVISAO(+)
   AND TT.COD_EMPRESA = EP.COD_EMPRESA(+)
   AND TT.COD_EMPRESA = FI.COD_EMPRESA(+)
   AND TT.COD_FILIAL = FI.COD_FILIAL(+)
   AND P.SEQ_PLA_PEDIDO_COMPRA||P.SEQ_PLA_PEDIDO_VENDA||P.SEQ_PLA_CONTRATO||P.SEQ_PLA_INSTRUCAO||P.SEQ_PLA_NEGOCIACAO_ALG||P.SEQ_PLA_NEGOCIACAO_GRA||P.SEQ_PLA_NEG_FORRAGEIRA = TT.SEQ_PLA_ORIGEM(+)
   AND TT.SEQ_PLA_ENDERECO = CE.SEQ_PLA_ENDERECO(+)
   AND CE.SEQ_PLA_CLIENTE = CL.SEQ_PLA_CLIENTE(+)
   AND P.VALOR_PREVISTO - NVL(BX.VALOR,0) > 0
   AND P.SEQ_PLA_MOEDA = (SELECT G.MOEDA_DOLAR FROM AGNEW.PAR_GERAL G)
UNION ALL
SELECT
       EP.COD_EMPRESA,
       EP.SIGLA_EMPRESA,
       FI.COD_FILIAL,
       FI.SIGLA_FILIAL,
       TRUNC(OM.DATA_VENCIMENTO) AS DATA_VCTO,
       OM.VALOR_OPERACAO,
       CASE WHEN OM.TIPO_OPERACAO = 'C' THEN 'COMPRA NDF' ELSE 'VENDA NDF' END AS STATUS,
       NULL AS NOME_CLIENTE,
       OC.NR_DOCUMENTO,
       NULL AS OBSERVACOES,
       NULL AS ORIGEM
  FROM AGNEW.OPERACAO_CONTRATO OC,
       AGNEW.OPERACAO_MOVIMENTO OM,
       AGNEW.EMPRESAS EP,
       AGNEW.FILIAIS FI
 WHERE OC.SEQ_PLA_CONTRATO = OM.SEQ_PLA_CONTRATO
   AND OC.COD_EMPRESA = EP.COD_EMPRESA
   AND OC.COD_EMPRESA = FI.COD_EMPRESA
   AND OC.COD_FILIAL = FI.COD_FILIAL
   AND OM.POSICAO IN ('V','C') --Somente o que foi comprado ou vendido;
