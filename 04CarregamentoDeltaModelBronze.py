print("--- Carregando dados da Camada Bronze para iniciar a Camada Silver ---")

# Carregar a tabela 'corte_agua_raw' da camada Bronze
try:
    df_corte_agua_silver_entrada = spark.read.format("delta").load(caminho_delta_bronze_corte_agua)
    print(f"\nDataFrame 'df_corte_agua_silver_entrada' carregado de: {caminho_delta_bronze_corte_agua}")
    print("Schema de df_corte_agua_silver_entrada:")
    df_corte_agua_silver_entrada.printSchema()
except Exception as e:
    print(f" Erro ao carregar a tabela Delta de corte_agua para a Silver: {e}")
    df_corte_agua_silver_entrada = None

# Carregar a tabela 'consumo_agua_raw' da camada Bronze
try:
    df_consumo_agua_silver_entrada = spark.read.format("delta").load(caminho_delta_bronze_consumo_agua)
    print(f"\nDataFrame 'df_consumo_agua_silver_entrada' carregado de: {caminho_delta_bronze_consumo_agua}")
    print("Schema de df_consumo_agua_silver_entrada:")
    df_consumo_agua_silver_entrada.printSchema()
except Exception as e:
    print(f" Erro ao carregar a tabela Delta de consumo_agua para a Silver: {e}")
    df_consumo_agua_silver_entrada = None

if df_corte_agua_silver_entrada is not None and df_consumo_agua_silver_entrada is not None:
    print("\n Tabelas da Bronze carregadas e prontas para transformações da Silver.")
else:
    print("\n Atenção: Uma ou ambas as tabelas da Bronze não puderam ser carregadas para a Silver.")