# Caminhos para os arquivos CSV de origem
caminho_csv_corte_agua = "/FileStore/tables/corte_agua.csv"
caminho_csv_consumo_agua = "/FileStore/tables/consumo_agua.csv"

# Caminho base para a camada Bronze no DBFS
caminho_base_bronze = "dbfs:/FileStore/tables/Bronze"

# Nomes das tabelas Delta na camada Bronze
nome_tabela_bronze_corte_agua = "corte_agua_raw"
nome_tabela_bronze_consumo_agua = "consumo_agua_raw"

# Caminhos completos para as tabelas Delta na camada Bronze
caminho_delta_bronze_corte_agua = f"{caminho_base_bronze}/{nome_tabela_bronze_corte_agua}"
caminho_delta_bronze_consumo_agua = f"{caminho_base_bronze}/{nome_tabela_bronze_consumo_agua}"

print(f"Caminho CSV Corte de Água: {caminho_csv_corte_agua}")
print(f"Caminho CSV Consumo de Água: {caminho_csv_consumo_agua}")
print(f"Caminho Base da Camada Bronze: {caminho_base_bronze}")
print(f"Caminho Delta Bronze para Corte de Água: {caminho_delta_bronze_corte_agua}")
print(f"Caminho Delta Bronze para Consumo de Água: {caminho_delta_bronze_consumo_agua}")