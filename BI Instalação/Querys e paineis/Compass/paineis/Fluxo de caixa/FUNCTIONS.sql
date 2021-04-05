create or replace function FINDICEBAIXA_PAGAR(MSEQ_PLA_PAGAR CHAR) return number is
  Result number;
begin
  select nvl(max(i.Valor_Indice),0) INTO RESULT
    from Compass_PRD.Pagar_Pagtos i
   where i.Seq_Pla_Pagar = MSEQ_PLA_PAGAR;

 return(Result);

end FINDICEBAIXA_PAGAR;

create or replace function FINDICEBAIXA_RECEBER(MSEQ_PLA_PAGAR CHAR) return number is
  Result number;
begin
  select nvl(max(i.Valor_Indice),0) INTO RESULT
    from Compass_PRD.Pagar_Pagtos i
   where i.Seq_Pla_Pagar = MSEQ_PLA_PAGAR;

 return(Result);

end FINDICEBAIXA_RECEBER;

create or replace function FINDICEMOEDA(MSEQ_PLA_MOEDA CHAR, PD_DATA DATE) return number is
  Result number;
begin
   SELECT NVL(VALOR,0) INTO RESULT 
 FROM 
 (SELECT * FROM COMPASS_PRD.MOEDAS_INDICES 
 where SEQ_PLA_MOEDA = MSEQ_PLA_MOEDA AND data_do_indice <= PD_DATA 
 ORDER BY DATA_DO_INDICE DESC) 
 WHERE ROWNUM = 1;

 return(Result);

end FINDICEMOEDA;

create or replace function FRETORNA_DATA_BAIXA_PAGAR(MSEQ_PLA_PAGAR CHAR) return date is
  Result date;
begin
 SELECT PP.DATA_PGTO INTO RESULT
 FROM COMPASS_PRD.PAGAR_PAGTOS PP 
 WHERE PP.SEQ_PLA_PAGAR = MSEQ_PLA_PAGAR 
 AND PP.LIB_PGTO = 'B';
 return(Result);

end FRETORNA_DATA_BAIXA_PAGAR;

create or replace function FRETORNA_DATA_BAIXA_RECEBER(MSEQ_PLA_RECEBER CHAR) return date is
  Result date;
begin
 SELECT RP.DATA_REC INTO RESULT
 FROM COMPASs_PRD.RECEBER_PAGTOS RP
 WHERE RP.SEQ_PLA_RECEBER = MSEQ_PLA_RECEBER;
 return(Result);

end FRETORNA_DATA_BAIXA_RECEBER;