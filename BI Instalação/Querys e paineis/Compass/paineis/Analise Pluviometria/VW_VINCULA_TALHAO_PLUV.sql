CREATE OR REPLACE VIEW VW_VINCULA_TALHAO_PLUV AS
SELECT
SEQ_PLA_VINC_TALHAO,
SEQ_PLA_PLUVIOMETRO,
T.SEQ_PLA_TALHAO,
T.NUMERO_TALHAO
FROM
agnew.Vincula_Talhao_Pluv VP,
AGNEW.TALHOES T
WHERE
VP.SEQ_PLA_TALHAO = T.SEQ_PLA_TALHAO;
