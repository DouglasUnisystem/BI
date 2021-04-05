/*CRIA OWER PARA O UNI BI*/
create user UNI_BI
identified by "000c2954f75f"
default tablespace USERS 
temporary tablespace TEMP 
profile DEFAULT; 
/* Grant/Revoke role privileges */
grant connect to UNI_BI;
grant dba to UNI_BI;
grant resource to UNI_BI;
/*Grant/Revoke system privileges*/
grant unlimited tablespace to UNI_BI;
/* Libera acessos total para o UNI BI*/
grant all privileges to UNI_BI;