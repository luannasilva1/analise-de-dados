# Imports podem ser agrupados no início do notebook ou por célula, conforme preferência.
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

print(f"--- Iniciando Ingestão para Camada Bronze: {caminho_csv_corte_agua} ---")

# Ler o CSV 'corte_agua.csv' com o cabeçalho original
df_corte_agua_csv = (spark.read
                               .format("csv")
                               .option("header", "true")
                               # .option("encoding", "UTF-8") # Descomente se necessário
                               .load(caminho_csv_corte_agua))

print("\nSchema original do CSV 'corte_agua.csv':")
df_corte_agua_csv.printSchema()

# Renomear colunas para nomes válidos usando .toDF()
# Colunas originais no CSV: Nome do Cliente, Tempo de Atraso da Conta, Data de Corte, Status do Corte, Localidade
df_corte_agua_bronze_renomeado = df_corte_agua_csv.toDF(
    "nome_cliente",
    "tempo_atraso_conta", # Será Integer na Silver
    "data_corte_str",     # Será Date na Silver (formato dd/MM/yy)
    "status_corte",
    "localidade"
)

print("\nSchema APÓS renomeação com .toDF() (DataFrame: df_corte_agua_bronze_renomeado):")
df_corte_agua_bronze_renomeado.printSchema()
print("Amostra dos dados com colunas renomeadas (corte_agua):")
df_corte_agua_bronze_renomeado.show(3, truncate=False)

# Salvar o DataFrame COM COLUNAS RENOMEADAS como tabela Delta na camada Bronze
try:
    print(f"\nSalvando DataFrame renomeado em: {caminho_delta_bronze_corte_agua}...")
    (df_corte_agua_bronze_renomeado.write
     .format("delta")
     .mode("overwrite")
     .save(caminho_delta_bronze_corte_agua))
    print(f" Tabela Delta '{nome_tabela_bronze_corte_agua}' salva com sucesso!")
except Exception as e:
    print(f" Erro ao salvar a tabela Delta '{nome_tabela_bronze_corte_agua}': {e}")