print(f"\n--- Iniciando Ingestão para Camada Bronze: {caminho_csv_consumo_agua} ---")

# Ler o CSV 'consumo_agua.csv' com o cabeçalho original
df_consumo_agua_csv = (spark.read
                                 .format("csv")
                                 .option("header", "true")
                                 # .option("encoding", "UTF-8") # Descomente se necessário para 'RegiÃ£o', etc.
                                 .load(caminho_csv_consumo_agua))

print("\nSchema original do CSV 'consumo_agua.csv':")
df_consumo_agua_csv.printSchema()

# Renomear colunas para nomes válidos usando .toDF()
# Colunas originais: RegiÃ£o, Cidade, Bairro, Consumo por mÂ³, Valor da Conta, Valor de Imposto, Nome do Cliente, MÃªs de ReferÃªncia, Data de Vencimento
df_consumo_agua_bronze_renomeado = df_consumo_agua_csv.toDF(
    "regiao",
    "cidade",
    "bairro",
    "consumo_m3_str",      # Será DecimalType na Silver
    "valor_conta_str",     # Será DecimalType na Silver
    "valor_imposto_str",   # Será DecimalType na Silver
    "nome_cliente",
    "mes_referencia_str",
    "data_vencimento_str" # Será DateType na Silver
)

print("\nSchema APÓS renomeação com .toDF() (DataFrame: df_consumo_agua_bronze_renomeado):")
df_consumo_agua_bronze_renomeado.printSchema()
print("Amostra dos dados com colunas renomeadas (consumo_agua):")
df_consumo_agua_bronze_renomeado.show(3, truncate=False)

# Salvar o DataFrame COM COLUNAS RENOMEADAS como tabela Delta na camada Bronze
try:
    print(f"\nSalvando DataFrame renomeado em: {caminho_delta_bronze_consumo_agua}...")
    (df_consumo_agua_bronze_renomeado.write
     .format("delta")
     .mode("overwrite")
     .save(caminho_delta_bronze_consumo_agua))
    print(f" Tabela Delta '{nome_tabela_bronze_consumo_agua}' salva com sucesso!")
except Exception as e:
    print(f" Erro ao salvar a tabela Delta '{nome_tabela_bronze_consumo_agua}': {e}")