%sql
CREATE DATABASE IF NOT EXISTS consumo_agua;
USE consumo_agua;

CREATE TABLE IF NOT EXISTS cliente (
  id_cliente BIGINT,
  nome STRING,
  cidade STRING,
  bairro STRING,
  regiao STRING
);

CREATE TABLE IF NOT EXISTS conta_consumo (
  id_conta BIGINT,
  id_cliente BIGINT,
  mes_referencia STRING,
  data_vencimento DATE,
  consumo_m3 DOUBLE,
  valor_conta DOUBLE,
  valor_imposto DOUBLE,
  tempo_atraso_dias INT
);

CREATE TABLE IF NOT EXISTS corte (
  id_corte BIGINT,
  id_cliente BIGINT,
  data_corte DATE,
  status_corte STRING
);
