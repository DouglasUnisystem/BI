CREATE OR REPLACE VIEW UNIDADE_PRODUTO AS
SELECT 
  SEQ_PLA_UNIDADE,
  DESCRICAO_UNIDADE,     
  SIGLA_UNIDADE, 
  QTDE_UNIDADE
  FROM BANCO.UNIDADE_PRODUTO
 ORDER BY SIGLA_UNIDADE;
