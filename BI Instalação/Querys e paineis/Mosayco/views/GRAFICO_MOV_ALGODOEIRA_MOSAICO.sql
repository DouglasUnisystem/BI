CREATE MATERIALIZED VIEW GRAFICO_MOV_ALGODOEIRA
REFRESH FORCE ON DEMAND
START WITH TO_DATE('10-09-2020 14:40:27', 'DD-MM-YYYY HH24:MI:SS') NEXT SYSDATE + 5/24/60 
AS
SELECT FA.COD_EMPRESA,
       FA.COD_FILIAL,
       FA.COD_SAFRA,
       SF.DESCRICAO_SAFRA AS SAFRA,
       EP.COD_EMPRESA||FL.COD_FILIAL AS ID_ALGODOEIRA,
       EP.SIGLA_EMPRESA||' - '||FL.SIGLA_FILIAL AS ALGODOEIRA,
       CL.SEQ_PLA_CLIENTE,
       CL.NOME_CLIENTE                          AS PRODUTOR,
       ET.DATA_ENTRADA,
       BE.DATA_BENEFICIAMENTO,
       SN.DATA_SAIDA,
       ET.NUM_CARGA                             AS FARDAO,
       ET.NUM_CARGA                             AS FARDAO_ROLINHO,
       ET.PESO_LIQUIDO                          AS PESO_FARDAO,
       FA.NUM_FARDO                             AS FARDINHO,
       FA.PESO_FARDO                            AS PESO_FARDINHO,
       FA.COD_MAQUINA                           AS PRENSA,
       TI.APELIDO                               AS TIPO,
       SN.NOTA_FISCAL,
       DECODE(FA.SEQ_SAIDA_DEPOSITO,NULL,'N','S') AS TAKEUP,
       BE.QTD_CAROCO                            AS CAROCO,
       TU.DESCRICAO                             AS TURNO
  FROM
       AGRICOLA.FARDOS             FA,
       AGRICOLA.BENEFICIAMENTO     BE,
       AGRICOLA.TURNOS             TU,
       AGRICOLA.SAFRAS             SF,
       AGRICOLA.EMPRESAS           EP,
       AGRICOLA.FILIAIS            FL,
       AGRICOLA.CLIENTES           CL,
       AGRICOLA.CLIENTES_ENDERECOS CE,
       AGRICOLA.ENTRADAS           ET,
       AGRICOLA.TIPOS              TI,
       AGRICOLA.CONTRATO_ALGODAO   CA,
       AGRICOLA.SAIDA_NOTAS        SN

 WHERE
   --Beneficiamento
   FA.COD_EMPRESA             = BE.COD_EMPRESA
   AND FA.COD_FILIAL          = BE.COD_FILIAL
   AND FA.COD_SAFRA           = BE.COD_SAFRA
   AND FA.SEQ_BENEFICIAMENTO  = BE.SEQ_BENEFICIAMENTO
   AND FA.COD_SAFRA           = SF.COD_SAFRA
   AND BE.SEQ_PLA_TURNO       = TU.SEQ_PLA_TURNO      (+)
   --Empresa e filial
   AND FA.COD_EMPRESA         = EP.COD_EMPRESA
   AND FA.COD_FILIAL          = FL.COD_FILIAL
   AND FA.COD_EMPRESA         = FL.COD_EMPRESA
   --Produtor
   AND FA.SEQ_PLA_ENDERECO    = CE.SEQ_PLA_ENDERECO
   AND FA.SEQ_PLA_CLIENTE     = CL.SEQ_PLA_CLIENTE
   -- Entrada
   AND FA.SEQ_ENTRADA         = ET.SEQ_ENTRADA
   AND FA.COD_SAFRA           = ET.COD_SAFRA
   AND FA.COD_EMPRESA         = ET.COD_EMPRESA
   AND FA.COD_FILIAL          = ET.COD_FILIAL
   --TIPO
   AND FA.COD_TIPO       = TI.COD_TIPO (+)
   --SAIDA
   AND FA.COD_TIPO            = TI.COD_TIPO           (+)
   AND FA.EMPRESA_ESTOQUE     = SN.COD_EMPRESA        (+)
   AND FA.FILIAL_ESTOQUE      = SN.COD_FILIAL         (+)
   AND FA.COD_SAFRA           = SN.COD_SAFRA          (+)
   AND FA.SEQ_SAIDA           = SN.SEQ_SAIDA          (+)
   AND SN.SEQ_PLA_CONTRATO    = CA.SEQ_PLA_CONTRATO   (+)
