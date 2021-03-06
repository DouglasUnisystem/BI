
SELECT AP.SEQ_PLA_ACORDO_POTENCIAL,
       AP.COD_EMPRESA,
       AP.COD_SAFRA,
       AC.NR_CONFIRMACAO,
       TRUNC(AP.DATA_ACORDO) DATA_ACORDO,      
       DECODE(AP.TIPO_CTR,'C','COMPRA','VENDA') TIPO_CTR,
       AP.SEQ_PLA_PRODUTO,
       PD.DESCRICAO_PRODUTO,
       AP.SEQ_PLA_UNIDADE,
       UN.SIGLA_UNIDADE,
       UN.QTDE_UNIDADE,
       CL.NOME_CLIENTE AS NOME_AGENTE_INTERNO,
       AG.SEQ_PLA_AGENTE,
       AP.SEQ_PLA_AGENTE_IN,       
       NVL(AP.QUANTIDADE,0) AS QUANTIDADE,
       ROUND(NVL(AP.UNITARIO,0),2) AS UNITARIO,
       ROUND(NVL(AP.VALOR_TOTAL,0),2) AS VALOR_TOTAL,
       NVL(CR.VALOR_RECEBIDO,0) VLR_PG_RB,       
       --CASE WHEN (NVL(DECODE(CP.DATA_VCTO,NULL,0, TRUNC(CP.DATA_VCTO) - (SELECT TRUNC(SYSDATE) FROM DUAL)),0)) < 0 then CASE WHEN (NVL(CP.VALOR_DOCUM,0) - NVL(CP.VALOR_PAGO,0))     = 0 then 'REGULAR' else 'IRREGULAR' end else 'IRREGULAR' end SITUACAO_CP,
       CASE WHEN (NVL(DECODE(CR.DATA_VCTO,NULL,0, TRUNC(CR.DATA_VCTO) - (SELECT TRUNC(SYSDATE) FROM DUAL)),0)) < 0 then CASE WHEN (NVL(CR.VALOR_DOCUM,0) - NVL(CR.VALOR_RECEBIDO,0)) = 0 then 'REGULAR' else 'IRREGULAR' end else 'REGULAR' end SITUACAO_FINAN,
       ROUND(NVL(AP.UNITARIO,0),2) -
       
       --UNITARIO DA ULTIMA COMPRA ANTES DA VENDA DO MESMO PRODUTO/UNIDADE/AGENTE_INTERNO
       ROUND(NVL((SELECT AVG(A.UNT) UNT
                    FROM (SELECT NVL(AP2.UNITARIO,0) UNT, TRUNC(AP2.DATA_ACORDO) DATA_ACORDO, AP2.SEQ_PLA_PRODUTO, AP2.SEQ_PLA_UNIDADE, AP2.SEQ_PLA_AGENTE_IN
                            FROM CERES1002.ACORDO_POTENCIAL   AP2
                           WHERE AP2.TIPO_CTR = 'C'
                           ORDER BY TRUNC(AP2.DATA_ACORDO) DESC
                         )A                   

                   WHERE A.SEQ_PLA_PRODUTO     = AP.SEQ_PLA_PRODUTO
                     AND A.SEQ_PLA_UNIDADE     = AP.SEQ_PLA_UNIDADE
                     AND A.SEQ_PLA_AGENTE_IN   = AP.SEQ_PLA_AGENTE_IN           
                     AND TO_DATE(TRUNC(A.DATA_ACORDO),'DD/MM/RRRR') >= TO_DATE('01/01/2021', 'DD/MM/RRRR') --FILTRAR COM A DATA DA ULTIMA SAIDA QUE SEJA MENOR QUE A ATUAL
                     AND TRUNC(A.DATA_ACORDO) <= TRUNC(AP.DATA_ACORDO)           
                        
       ),0),2) as LUCRO_VENDA,
       -- 
       NVL(AP.PERC_COMISSAO_AGENTE_IN,0) AS PERC_COMISSAO_AGENTE_IN,
       NVL(AP.COMISSAO_TOT_AGENTE_IN,0) AS COMISSAO_AGENTE_INT
       
  FROM CERES1002.ACORDO_POTENCIAL   AP,
       CERES1002.ACORDO_CONFIRMACAO AC,
       CERES1002.PRODUTOS           PD,
       CERES1002.UNIDADE_PRODUTO    UN,
       --agente interno
       CERES1002.AGENTES            AG,
       CERES1002.CLIENTES           CL,
       CERES1002.CLIENTES_ENDERECOS CE,
       --CTR SAIDA
       CERES1002.CONTRATO_SAIDA     CS,
       --CONTAS_PAGAR       CP,
       CERES1002.CONTAS_RECEBER     CR  

 WHERE AP.SEQ_PLA_ACORDO_POTENCIAL   = AC.SEQ_PLA_ACORDO_POTENCIAL
   --agente interno
   AND AP.SEQ_PLA_AGENTE_IN          = AG.SEQ_PLA_AGENTE
   AND AG.SEQ_PLA_ENDERECO           = CE.SEQ_PLA_ENDERECO
   AND CE.SEQ_PLA_CLIENTE            = CL.SEQ_PLA_CLIENTE   
   --fim agente interno
   AND AP.SEQ_PLA_PRODUTO            = PD.SEQ_PLA_PRODUTO
   AND AP.Seq_Pla_Unidade            = UN.SEQ_PLA_UNIDADE
   --CTR SAIDA      
   AND AP.SEQ_PLA_ACORDO_POTENCIAL   = CS.SEQ_PLA_ACORDO_POTENCIAL(+)
   --AND CS.SEQ_PLA_PED_VENDA          = CP.Seq_Pla_Origem(+)
   AND CS.SEQ_PLA_PED_VENDA          = CR.Seq_Pla_Origem(+)   
   --fim CTR SAIDA
   AND NVL(AP.STATUS,'A')            = 'F'
   AND AP.TIPO_CTR                   = 'V'
   AND TO_DATE(TRUNC(AP.DATA_ACORDO),'DD/MM/RRRR') >= TO_DATE('01/01/2021', 'DD/MM/RRRR')   
   AND AP.SEQ_PLA_PRODUTO   = '  33948001'
   AND AP.SEQ_PLA_UNIDADE   = '     41601'
   AND AP.SEQ_PLA_AGENTE_IN = '  38573301'
   AND AP.SEQ_PLA_ACORDO_POTENCIAL = ' 226680401'   

ORDER BY 5,11;

(SELECT AP2.DATA_ACORDO, AP2.UNITARIO, AP2.TIPO_CTR
   FROM CERES1002.ACORDO_POTENCIAL   AP2
  WHERE AP2.SEQ_PLA_PRODUTO   = '  33948001'
    AND AP2.SEQ_PLA_UNIDADE   = '     41601'
    AND AP2.SEQ_PLA_AGENTE_IN = '  38573301'
--    AND TO_DATE(TRUNC(AP2.DATA_ACORDO),'DD/MM/RRRR') >= TO_DATE('12/01/2021', 'DD/MM/RRRR')
--    AND TO_DATE(TRUNC(AP2.DATA_ACORDO),'DD/MM/RRRR') <= TO_DATE('01/02/2021', 'DD/MM/RRRR')   
    AND TO_DATE(TRUNC(AP2.DATA_ACORDO),'DD/MM/RRRR') >= TO_DATE('01/01/2021', 'DD/MM/RRRR')    
  
)
    
