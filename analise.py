#Aqui estou realizando a importação

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, upper, trim, col, to_date, expr
import seaborn as sns
import matplotlib.pyplot as plt

# Leitura dos arquivos a partir do catalogo 

corte_df = spark.read.csv('/FileStore/corte_agua.csv', header=True, inferSchema=True) 
consumo_df = spark.read.csv('/FileStore/consumo_agua.csv', header=True, inferSchema=True)

# Padronização das colunas

corte_df = corte_df.withColumn('Nome do Cliente', upper(trim(corte_df['Nome do Cliente'])))
consumo_df = consumo_df.withColumn('Nome do Cliente', upper(trim(consumo_df['Nome do Cliente'])))

#  Convertendo datas

corte_df = corte_df.withColumn('Data de Corte', to_date('Data de Corte', 'yyyy-MM-dd'))
consumo_df = consumo_df.withColumn('Data de Vencimento', to_date('Data de Vencimento', 'yyyy-MM-dd'))

# Join dos DataFrames 

joined_df = consumo_df.join(corte_df, on='Nome do Cliente', how='left')

# Exploração inicial

print("\nSchema do DataFrame conjunto:")
joined_df.printSchema()

print("\nPrimeiras linhas do DataFrame conjunto:")
joined_df.select('Nome do Cliente', 'Cidade', 'Bairro', 'Consumo por m³', 'Valor da Conta', 'Tempo de Atraso da Conta', 'Status do Corte').show(10)


# Análises gerais

print("\nClientes com maior consumo e atraso:")
joined_df.orderBy(col('Consumo por m³').desc(), col('Tempo de Atraso da Conta').desc()).show(10)

print("\nMédia de consumo por bairro:")
joined_df.groupBy('Bairro').agg(avg('Consumo por m³').alias('Media Consumo')).orderBy('Media Consumo', ascending=False).show()

print("\nValor médio da conta com corte efetivado:")
joined_df.filter(col('Status do Corte') == 'Efetivado').agg(avg('Valor da Conta').alias('Media Valor Conta')).show()

print("\nPercentual médio de imposto:")
joined_df = joined_df.withColumn('Percentual Imposto', (col('Valor de Imposto') / col('Valor da Conta')) * 100)
joined_df.agg(avg('Percentual Imposto').alias('Media Percentual Imposto')).show()

print("\nRegiões com mais cortes:")
joined_df.groupBy('Região').count().orderBy('count', ascending=False).show()