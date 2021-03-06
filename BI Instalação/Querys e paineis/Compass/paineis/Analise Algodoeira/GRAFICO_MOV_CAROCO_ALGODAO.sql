CREATE MATERIALIZED VIEW GRAFICO_MOV_CAROCO_ALGODAO 
REFRESH FORCE ON DEMAND
START WITH SYSDATE NEXT SYSDATE + 5/24/60 
AS
SELECT
  TT.PRODUTO,
  TT.COD_EMPRESA,
  TT.COD_FILIAL,
  TT.COD_SAFRA,
  TT.SAFRA,
  TT.ALGODOEIRA,
  TT.PRODUTOR,
  TT.SEQ_PLA_CLIENTE,
  TT.DATA_SAIDA AS DATA_OPERACAO,
  TT.PESO_TOTAL
FROM (
SELECT
     'FARDAO_ENTRADA' AS PRODUTO,
     X.COD_EMPRESA,
     X.COD_FILIAL,
     X.COD_SAFRA,
     X.DESCRICAO_SAFRA AS SAFRA,
     X.SIGLA_EMPRESA||' - '|| X.SIGLA_FILIAL AS ALGODOEIRA,
     X.SEQ_PLA_CLIENTE,
     X.NOME_CLIENTE                          AS PRODUTOR,
     X.DATA_ENTRADA AS DATA_SAIDA,
     X.PESO_LIQUIDO AS PESO_TOTAL
FROM (
SELECT
       EE.COD_SAFRA,
       EE.COD_EMPRESA,
       EE.COD_FILIAL,
       CL.NOME_CLIENTE AS PRODUTOR,
       EP.SIGLA_EMPRESA||' - '|| FI.SIGLA_FILIAL AS ALGODOEIRA,
       CE.SEQ_PLA_CLIENTE,
       CE.SEQ_PLA_ENDERECO,
       ER.PLACA_VEICULO,
       ER.NR_FARDAO,
       EE.DATA_ENTRADA,
       FI.SIGLA_FILIAL,
       EP.SIGLA_EMPRESA,
       SF.DESCRICAO_SAFRA,      
       ER.PESO_BRUTO,
       ER.PESO_TARA,
       ER.PESO_LIQUIDO,
       ER.TIKET_BALANCA,
       er.qtd_vol_transportado,
       EE.NR_DOCUMENTO,
       ER.PLANILHA,
       CASE WHEN (NVL(ER.PESO_BRUTO,0)-NVL(ER.PESO_TARA,0)) = 0 THEN 0 ELSE (NVL(ER.PESO_BRUTO,0)-NVL(ER.PESO_TARA,0))-NVL(ER.PESO_LIQUIDO,0) END AS DESCONTOS,
       CL.NOME_CLIENTE,
       'x'as RELATORIO,
       FA.SEQ_PLA_FAZENDA,
       FA.DESC_FAZENDA,
       ER.TIPO_CARGA_ALGODAO,
       DECODE(ER.SEMENTE,'S','Semente',NULL)||DECODE(ER.BORDADURA,'S',', ')||
       DECODE(ER.BORDADURA,'S','Bordadura')||DECODE(ER.TOMBADO,'S',', ')||
       DECODE(ER.TOMBADO,'S','Tombado')||DECODE(ER.QUEBRADO,'S',', ')||
       DECODE(ER.QUEBRADO,'S','Quabrado')||DECODE(ER.IMPUREZA,'S',', ')||
       DECODE(ER.IMPUREZA,'S','Impureza')||DECODE(ER.SUSP_FOGO,'S',', ')||
       DECODE(ER.SUSP_FOGO,'S','Susp. Fogo') AS CARACTERISTICAS,
       ER.SEMENTE,
       ER.BORDADURA
  FROM BANCO.EST_ENTRADAS        EE,
       BANCO.EST_ENTRADAS_ITENS  EI,
       BANCO.EST_ENTRADAS_ROM    ER,      
       BANCO.CLIENTES_ENDERECOS  CE,
       BANCO.CLIENTES            CL,
       BANCO.PAR_ROMANEIOS_ALGODAO PR,
       BANCO.FAZENDAS FA,
       BANCO.FILIAIS FI,
       BANCO.EMPRESAS EP,
       BANCO.SAFRAS SF
 WHERE UPPER(EE.FORM_LANCAMENTO) = 'DFMROMENTALGCAROCO'
   AND EE.COD_SAFRA              = SF.COD_SAFRA
   AND EE.SEQ_PLA_ENTRADA        = EI.SEQ_PLA_ENTRADA
   AND EE.COD_FILIAL             = FI.COD_FILIAL
   AND FI.COD_EMPRESA            = EP.COD_EMPRESA
   and ei.seq_pla_produto        = pr.seq_pla_alg_caroco
   and pr.cod_empresa            = ee.cod_empresa
   and pr.cod_filial             = ee.cod_filial
   and pr.cod_safra              = ee.cod_safra
   and ee.seq_pla_Fazenda        = fa.seq_pla_Fazenda
   AND EE.SEQ_PLA_FAZENDA        = PR.SEQ_PLA_FAZENDA
   AND EE.SEQ_PLA_ENTRADA        = ER.SEQ_PLA_ENTRADA
   AND EE.SEQ_PLA_ENDERECO       = CE.SEQ_PLA_ENDERECO
   AND CE.SEQ_PLA_CLIENTE        = CL.SEQ_PLA_CLIENTE
   AND NOT EXISTS (SELECT *
                     FROM BANCO.EST_SAIDAS ES,
                          BANCO.EST_SAIDAS_ROM ESR
                    WHERE ES.SEQ_PLA_ORIGEM = EE.SEQ_PLA_ENTRADA
                      AND ES.SEQ_PLA_SAIDA  = ESR.SEQ_PLA_SAIDA
 AND ES.COD_SAFRA   = EE.COD_SAFRA
 AND ES.COD_EMPRESA = EE.COD_EMPRESA
 AND ES.COD_FILIAL  = EE.COD_FILIAL
                    AND NVL(ESR.CANCELADO,'N') = 'N')
  AND NOT EXISTS (SELECT VC.SEQ_PLA_ROM_MODULO FROM BANCO.VINC_ROM_MODULO_COLHIDO VC WHERE VC.SEQ_PLA_ENT_CARGA = EE.SEQ_PLA_ENTRADA AND VC.SEQ_PLA_ENTRADA IS NOT NULL)
  AND NVL(ER.CANCELADO,'N') <> 'S' ) X

UNION ALL

SELECT
     'FARDAO_BENEFICIADO' AS PRODUTO,
     X.COD_EMPRESA,
     X.COD_FILIAL,
     X.COD_SAFRA,
     X.DESCRICAO_SAFRA AS SAFRA,
     X.SIGLA_EMPRESA||' - '|| X.SIGLA_FILIAL AS ALGODOEIRA,
     X.SEQ_PLA_CLIENTE,
     X.NOME_CLIENTE                          AS PRODUTOR,
     X.DATA_ENTRADA AS DATA_SAIDA,
     X.PESO_LIQUIDO AS PESO_TOTAL
FROM (
SELECT
       EE.COD_SAFRA,
       EE.COD_EMPRESA,
       EE.COD_FILIAL,
       CL.NOME_CLIENTE AS PRODUTOR,
       EP.SIGLA_EMPRESA||' - '|| FI.SIGLA_FILIAL AS ALGODOEIRA,
       CE.SEQ_PLA_CLIENTE,
       CE.SEQ_PLA_ENDERECO,
       ER.PLACA_VEICULO,
       ER.NR_FARDAO,
       EE.DATA_ENTRADA,
       FI.SIGLA_FILIAL,
       EP.SIGLA_EMPRESA,
       SF.DESCRICAO_SAFRA,      
       ER.PESO_BRUTO,
       ER.PESO_TARA,
       ER.PESO_LIQUIDO,
       ER.TIKET_BALANCA,
       er.qtd_vol_transportado,
       EE.NR_DOCUMENTO,
       ER.PLANILHA,
       CASE WHEN (NVL(ER.PESO_BRUTO,0)-NVL(ER.PESO_TARA,0)) = 0 THEN 0 ELSE (NVL(ER.PESO_BRUTO,0)-NVL(ER.PESO_TARA,0))-NVL(ER.PESO_LIQUIDO,0) END AS DESCONTOS,
       CL.NOME_CLIENTE,
       'x'as RELATORIO,
       FA.SEQ_PLA_FAZENDA,
       FA.DESC_FAZENDA,
       ER.TIPO_CARGA_ALGODAO,
       DECODE(ER.SEMENTE,'S','Semente',NULL)||DECODE(ER.BORDADURA,'S',', ')||
       DECODE(ER.BORDADURA,'S','Bordadura')||DECODE(ER.TOMBADO,'S',', ')||
       DECODE(ER.TOMBADO,'S','Tombado')||DECODE(ER.QUEBRADO,'S',', ')||
       DECODE(ER.QUEBRADO,'S','Quabrado')||DECODE(ER.IMPUREZA,'S',', ')||
       DECODE(ER.IMPUREZA,'S','Impureza')||DECODE(ER.SUSP_FOGO,'S',', ')||
       DECODE(ER.SUSP_FOGO,'S','Susp. Fogo') AS CARACTERISTICAS,
       ER.SEMENTE,
       ER.BORDADURA
  FROM BANCO.EST_ENTRADAS        EE,
       BANCO.EST_ENTRADAS_ITENS  EI,
       BANCO.EST_ENTRADAS_ROM    ER,      
       BANCO.CLIENTES_ENDERECOS  CE,
       BANCO.CLIENTES            CL,
       BANCO.PAR_ROMANEIOS_ALGODAO PR,
       BANCO.FAZENDAS FA,
       BANCO.FILIAIS FI,
       BANCO.EMPRESAS EP,
       BANCO.SAFRAS SF
 WHERE UPPER(EE.FORM_LANCAMENTO) = 'DFMROMENTALGCAROCO'
   AND EE.COD_SAFRA              = SF.COD_SAFRA
   AND EE.SEQ_PLA_ENTRADA        = EI.SEQ_PLA_ENTRADA
   AND EE.COD_FILIAL             = FI.COD_FILIAL
   AND FI.COD_EMPRESA            = EP.COD_EMPRESA
   and ei.seq_pla_produto        = pr.seq_pla_alg_caroco
   and pr.cod_empresa            = ee.cod_empresa
   and pr.cod_filial             = ee.cod_filial
   and pr.cod_safra              = ee.cod_safra
   and ee.seq_pla_Fazenda        = fa.seq_pla_Fazenda
   AND EE.SEQ_PLA_FAZENDA        = PR.SEQ_PLA_FAZENDA
   AND EE.SEQ_PLA_ENTRADA        = ER.SEQ_PLA_ENTRADA
   AND EE.SEQ_PLA_ENDERECO       = CE.SEQ_PLA_ENDERECO
   AND CE.SEQ_PLA_CLIENTE        = CL.SEQ_PLA_CLIENTE
   AND NOT EXISTS (SELECT *
                     FROM BANCO.EST_SAIDAS ES,
                          BANCO.EST_SAIDAS_ROM ESR
                    WHERE ES.SEQ_PLA_ORIGEM = EE.SEQ_PLA_ENTRADA
                      AND ES.SEQ_PLA_SAIDA  = ESR.SEQ_PLA_SAIDA
 AND ES.COD_SAFRA   = EE.COD_SAFRA
 AND ES.COD_EMPRESA = EE.COD_EMPRESA
 AND ES.COD_FILIAL  = EE.COD_FILIAL
                    AND NVL(ESR.CANCELADO,'N') = 'N')
  AND NOT EXISTS (SELECT VC.SEQ_PLA_ROM_MODULO FROM BANCO.VINC_ROM_MODULO_COLHIDO VC WHERE VC.SEQ_PLA_ENT_CARGA = EE.SEQ_PLA_ENTRADA AND VC.SEQ_PLA_ENTRADA IS NOT NULL)
  AND NVL(ER.CANCELADO,'N') <> 'S' 
  AND EE.SEQ_PLA_ENTRADA  IN (SELECT LEB.SEQ_PLA_ENTRADA             
  FROM BANCO.LIGA_ENTRADA_BENEFICIAMENTO LEB) ) X

UNION ALL

SELECT
     'CAROCO_SAIDA' AS PRODUTO,
     ES.COD_EMPRESA,
     ES.COD_FILIAL,
     SA.COD_SAFRA,
     SA.DESCRICAO_SAFRA AS SAFRA,
     EP.SIGLA_EMPRESA||' - '||FI.SIGLA_FILIAL AS ALGODOEIRA,
     CL.SEQ_PLA_CLIENTE,
     CL.NOME_CLIENTE  AS PRODUTOR,
     ES.DATA_SAIDA,
     ESI.QUANTIDADE AS PESO_TOTAL
FROM BANCO.EST_SAIDAS ES,
     BANCO.EST_SAIDAS_ITENS ESI,
     BANCO.EMPRESAS EP,
     BANCO.FILIAIS FI,
     BANCO.CLIENTES CL,
     BANCO.CLIENTES_ENDERECOS CE,
     BANCO.SAFRAS SA,
     BANCO.TIPOS_LANCTOS TL,
     BANCO.ARMAZENS AR,
     BANCO.ARMAZEM_TIPO ART
WHERE ES.SEQ_PLA_SAIDA = ESI.SEQ_PLA_SAIDA
 AND ES.COD_EMPRESA = EP.COD_EMPRESA
 AND ES.COD_EMPRESA = FI.COD_EMPRESA
 AND ES.COD_FILIAL = FI.COD_FILIAL
 AND FI.SEQ_PLA_ENDERECO = CE.SEQ_PLA_ENDERECO
 AND CE.SEQ_PLA_CLIENTE= CL.SEQ_PLA_CLIENTE
 AND ES.COD_SAFRA = SA.COD_SAFRA
 AND ESI.COD_TIPO_LCTO = TL.COD_TIPO_LCTO
 AND ESI.SEQ_PLA_ARMAZEM = AR.SEQ_PLA_ARMAZEM
 AND AR.SEQ_PLA_TIPO_ARMAZEM = ART.SEQ_PLA_TIPO_ARMAZEM
 AND EXISTS (SELECT * FROM BANCO.BENEFICIAMENTO B WHERE B.COD_EMPRESA = ES.COD_EMPRESA AND B.COD_FILIAL = ES.COD_FILIAL)
 AND ART.TIPO <> 1
 AND ((NVL(TL.ESTOQUE_FISCAL,'N') = 'S') OR (NVL(TL.ESTOQUE_DE_TERCEIROS,'N') = 'S'))
 AND ESI.COMITADO = 'S'
 AND ESI.SEQ_PLA_PRODUTO IN (SELECT distinct P.SEQ_PLA_CAROCO FROM BANCO.PAR_ROMANEIOS_ALGODAO P WHERE P.SEQ_PLA_CAROCO IS NOT NULL)
 AND NVL(ES.CANCELADO,'N') <> 'S'

UNION ALL

SELECT
     'CAROCO_ENTRADA' AS PRODUTO,
     EE.COD_EMPRESA,
     EE.COD_FILIAL,
     SA.COD_SAFRA,
     SA.DESCRICAO_SAFRA AS SAFRA,
     EP.SIGLA_EMPRESA||' - '||FI.SIGLA_FILIAL AS ALGODOEIRA,
     CL.SEQ_PLA_CLIENTE,
     CL.NOME_CLIENTE  AS PRODUTOR,
     EE.DATA_ENTRADA AS DATA_OPERACAO,
     EEA.QUANTIDADE  AS PESO_TOTAL
FROM BANCO.EST_ENTRADAS EE,
     BANCO.EST_ENTRADAS_ITENS EEI,
     BANCO.EST_ENTRADAS_FAZENDA EEF,
     BANCO.EST_ENTRADAS_ARMAZEM EEA,
     BANCO.EMPRESAS EP,
     BANCO.FILIAIS FI,
     BANCO.CLIENTES CL,
     BANCO.CLIENTES_ENDERECOS CE,
     BANCO.SAFRAS SA,
     BANCO.TIPOS_LANCTOS TL,
     BANCO.ARMAZENS AR,
     BANCO.ARMAZEM_TIPO ART
WHERE EE.SEQ_PLA_ENTRADA = EEI.SEQ_PLA_ENTRADA
 AND EEI.SEQ_PLA_ENT_ITEM = EEF.SEQ_PLA_ENT_ITEM
 AND EEF.SEQ_PLA_ENT_FAZENDA = EEA.SEQ_PLA_ENT_FAZENDA
 AND EE.COD_EMPRESA = EP.COD_EMPRESA
 AND EE.COD_EMPRESA = FI.COD_EMPRESA
 AND EE.COD_FILIAL = FI.COD_FILIAL
 AND FI.SEQ_PLA_ENDERECO = CE.SEQ_PLA_ENDERECO
 AND CE.SEQ_PLA_CLIENTE= CL.SEQ_PLA_CLIENTE
 AND EE.COD_SAFRA = SA.COD_SAFRA
 AND EEI.COD_TIPO_LCTO = TL.COD_TIPO_LCTO
 AND EEA.SEQ_PLA_ARMAZEM = AR.SEQ_PLA_ARMAZEM
 AND AR.SEQ_PLA_TIPO_ARMAZEM = ART.SEQ_PLA_TIPO_ARMAZEM
 AND EXISTS (SELECT * FROM BANCO.BENEFICIAMENTO B WHERE B.SEQ_PLA_ENT_CAR_CLI = EE.SEQ_PLA_ENTRADA
              UNION
             SELECT * FROM BANCO.BENEFICIAMENTO B WHERE B.SEQ_PLA_ENT_CAR_ALG = EE.SEQ_PLA_ENTRADA )
 AND ART.TIPO <> 1
 AND ((NVL(TL.ESTOQUE_FISCAL,'N') = 'S') OR (NVL(TL.ESTOQUE_DE_TERCEIROS,'N') = 'S'))
 AND EEA.COMITADO = 'S'
 AND EEI.SEQ_PLA_PRODUTO IN (SELECT distinct P.SEQ_PLA_CAROCO FROM BANCO.PAR_ROMANEIOS_ALGODAO P WHERE P.SEQ_PLA_CAROCO IS NOT NULL)
 AND NVL(EE.CANCELADO,'N') <> 'S'

 UNION ALL

 SELECT
     'FIBRILHA_ENTRADA' AS PRODUTO,
     EE.COD_EMPRESA,
     EE.COD_FILIAL,
     SA.COD_SAFRA,
     SA.DESCRICAO_SAFRA AS SAFRA,
     EP.SIGLA_EMPRESA||' - '||FI.SIGLA_FILIAL AS ALGODOEIRA,
     CL.SEQ_PLA_CLIENTE,
     CL.NOME_CLIENTE   AS PRODUTOR,
     EE.DATA_ENTRADA,
     EEA.QUANTIDADE  AS PESO_TOTAL
FROM BANCO.EST_ENTRADAS EE,
     BANCO.EST_ENTRADAS_ITENS EEI,
     BANCO.EST_ENTRADAS_FAZENDA EEF,
     BANCO.EST_ENTRADAS_ARMAZEM EEA,
     BANCO.EMPRESAS EP,
     BANCO.FILIAIS FI,
     BANCO.CLIENTES CL,
     BANCO.CLIENTES_ENDERECOS CE,
     BANCO.SAFRAS SA,
     BANCO.TIPOS_LANCTOS TL,
     BANCO.ARMAZENS AR,
     BANCO.ARMAZEM_TIPO ART
WHERE EE.SEQ_PLA_ENTRADA = EEI.SEQ_PLA_ENTRADA
 AND EEI.SEQ_PLA_ENT_ITEM = EEF.SEQ_PLA_ENT_ITEM
 AND EEF.SEQ_PLA_ENT_FAZENDA = EEA.SEQ_PLA_ENT_FAZENDA
 AND EE.COD_EMPRESA = EP.COD_EMPRESA
 AND EE.COD_EMPRESA = FI.COD_EMPRESA
 AND EE.COD_FILIAL = FI.COD_FILIAL
 AND FI.SEQ_PLA_ENDERECO = CE.SEQ_PLA_ENDERECO
 AND CE.SEQ_PLA_CLIENTE= CL.SEQ_PLA_CLIENTE
 AND EE.COD_SAFRA = SA.COD_SAFRA
 AND EEI.COD_TIPO_LCTO = TL.COD_TIPO_LCTO
 AND EEA.SEQ_PLA_ARMAZEM = AR.SEQ_PLA_ARMAZEM
 AND AR.SEQ_PLA_TIPO_ARMAZEM = ART.SEQ_PLA_TIPO_ARMAZEM
 AND EXISTS (SELECT * FROM BANCO.BENEFICIAMENTO B WHERE B.SEQ_PLA_ENT_FIB_PESO = EE.SEQ_PLA_ENTRADA
              UNION
             SELECT * FROM BANCO.BENEFICIAMENTO B WHERE B.SEQ_PLA_ENT_FIBRILHA = EE.SEQ_PLA_ENTRADA )
 AND ART.TIPO <> 1
 AND ((NVL(TL.ESTOQUE_FISCAL,'N') = 'S') OR (NVL(TL.ESTOQUE_DE_TERCEIROS,'N') = 'S'))
 AND EEA.COMITADO = 'S'
 AND EEI.SEQ_PLA_PRODUTO IN (SELECT distinct P.SEQ_PLA_FIBRILHA FROM BANCO.PAR_ROMANEIOS_ALGODAO P WHERE P.SEQ_PLA_FIBRILHA IS NOT NULL)
 AND NVL(EE.CANCELADO,'N') <> 'S'

 UNION ALL

 SELECT
     'RESIDUO_ENTRADA' AS PRODUTO,
     EE.COD_EMPRESA,
     EE.COD_FILIAL,
     SA.COD_SAFRA,
     SA.DESCRICAO_SAFRA AS SAFRA,
     EP.SIGLA_EMPRESA||' - '||FI.SIGLA_FILIAL AS ALGODOEIRA,
     CL.SEQ_PLA_CLIENTE,
     CL.NOME_CLIENTE  AS PRODUTOR,
     EE.DATA_ENTRADA,
     EEA.QUANTIDADE  AS PESO_TOTAL
FROM BANCO.EST_ENTRADAS EE,
     BANCO.EST_ENTRADAS_ITENS EEI,
     BANCO.EST_ENTRADAS_FAZENDA EEF,
     BANCO.EST_ENTRADAS_ARMAZEM EEA,
     BANCO.EMPRESAS EP,
     BANCO.FILIAIS FI,
     BANCO.CLIENTES CL,
     BANCO.CLIENTES_ENDERECOS CE,
     BANCO.SAFRAS SA,
     BANCO.TIPOS_LANCTOS TL,
     BANCO.ARMAZENS AR,
     BANCO.ARMAZEM_TIPO ART
WHERE EE.SEQ_PLA_ENTRADA = EEI.SEQ_PLA_ENTRADA
 AND EEI.SEQ_PLA_ENT_ITEM = EEF.SEQ_PLA_ENT_ITEM
 AND EEF.SEQ_PLA_ENT_FAZENDA = EEA.SEQ_PLA_ENT_FAZENDA
 AND EE.COD_EMPRESA = EP.COD_EMPRESA
 AND EE.COD_EMPRESA = FI.COD_EMPRESA
 AND EE.COD_FILIAL = FI.COD_FILIAL
 AND FI.SEQ_PLA_ENDERECO = CE.SEQ_PLA_ENDERECO
 AND CE.SEQ_PLA_CLIENTE= CL.SEQ_PLA_CLIENTE
 AND EE.COD_SAFRA = SA.COD_SAFRA
 AND EEI.COD_TIPO_LCTO = TL.COD_TIPO_LCTO
 AND EEA.SEQ_PLA_ARMAZEM = AR.SEQ_PLA_ARMAZEM
 AND AR.SEQ_PLA_TIPO_ARMAZEM = ART.SEQ_PLA_TIPO_ARMAZEM
 AND EXISTS (SELECT * FROM BANCO.BENEFICIAMENTO B WHERE B.SEQ_PLA_ENT_RESIDUO = EE.SEQ_PLA_ENTRADA )
 AND ART.TIPO <> 1
 AND ((NVL(TL.ESTOQUE_FISCAL,'N') = 'S') OR (NVL(TL.ESTOQUE_DE_TERCEIROS,'N') = 'S'))
 AND EEA.COMITADO = 'S'
 AND EEI.SEQ_PLA_PRODUTO IN (SELECT distinct P.SEQ_PLA_PROD_RESIDUO FROM BANCO.PAR_ROMANEIOS_ALGODAO P WHERE P.SEQ_PLA_PROD_RESIDUO IS NOT NULL)
 AND NVL(EE.CANCELADO,'N') <> 'S'
 ) TT;
