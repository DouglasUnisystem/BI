CREATE MATERIALIZED VIEW VW_ACORDO_P_REALIZADO_RECEBIDO REFRESH FORCE ON DEMAND START WITH SYSDATE NEXT SYSDATE + 10/24/60 AS
SELECT AP.SEQ_PLA_ACORDO_POTENCIAL,
       AP.COD_EMPRESA,
       AP.COD_SAFRA,
       AP.SEQ_PLA_PRODUTO,
       TRUNC(AP.DATA_ACORDO) DATA_ACORDO,      
       DECODE(AP.TIPO_CTR,'C','COMPRA','VENDA') TIPO_CTR,
       PD.DESCRICAO_PRODUTO,
       CL.NOME_CLIENTE AS NOME_AGENTE_INTERNO,
       AG.SEQ_PLA_AGENTE,
       AP.SEQ_PLA_AGENTE_IN,       
       NVL(AP.QUANTIDADE,0) AS QUANTIDADE,
       ROUND(NVL(AP.UNITARIO,0),2) AS UNITARIO,
       ROUND(NVL(AP.VALOR_TOTAL,0),2) AS VALOR_TOTAL,
       NVL(CR.VALOR_RECEBIDO,0) VLR_PG_RB                          
       
  FROM AGRICOLA.ACORDO_POTENCIAL   AP,
       AGRICOLA.PRODUTOS           PD,
       --agente interno
       AGRICOLA.AGENTES            AG,
       AGRICOLA.CLIENTES           CL,
       AGRICOLA.CLIENTES_ENDERECOS CE,
       --CTR SAIDA
       AGRICOLA.CONTRATO_SAIDA     CS,
       AGRICOLA.CONTAS_RECEBER     CR  

 WHERE AP.SEQ_PLA_AGENTE_IN          = AG.SEQ_PLA_AGENTE
   AND AG.SEQ_PLA_ENDERECO           = CE.SEQ_PLA_ENDERECO
   AND CE.SEQ_PLA_CLIENTE            = CL.SEQ_PLA_CLIENTE   
   AND AP.SEQ_PLA_PRODUTO            = PD.SEQ_PLA_PRODUTO
   AND AP.SEQ_PLA_ACORDO_POTENCIAL   = CS.SEQ_PLA_ACORDO_POTENCIAL(+)
   AND CS.SEQ_PLA_PED_VENDA          = CR.Seq_Pla_Origem(+)   
   AND NVL(AP.STATUS,'A')            = 'F'
   AND AP.TIPO_CTR                   = 'V'
ORDER BY 5,11
