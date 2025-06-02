# Definir caminhos para a camada Silver
caminho_base_silver = "dbfs:/FileStore/tables/Silver" # Ou o caminho que preferir

nome_tabela_silver_corte_agua = "corte_agua_silver"
nome_tabela_silver_consumo_agua = "consumo_agua_silver"

caminho_delta_silver_corte_agua = f"{caminho_base_silver}/{nome_tabela_silver_corte_agua}"
caminho_delta_silver_consumo_agua = f"{caminho_base_silver}/{nome_tabela_silver_consumo_agua}"

# Salvar df_corte_agua_silver_final
if 'df_corte_agua_silver_final' in locals() and df_corte_agua_silver_final is not None:
    try:
        print(f"\nSalvando tabela Silver '{nome_tabela_silver_corte_agua}' em: {caminho_delta_silver_corte_agua}...")
        (df_corte_agua_silver_final.write
         .format("delta")
         .mode("overwrite")
         .save(caminho_delta_silver_corte_agua))
        print(f"✅ Tabela Delta Silver '{nome_tabela_silver_corte_agua}' salva com sucesso!")
    except Exception as e:
        print(f"Erro ao salvar a tabela Delta Silver '{nome_tabela_silver_corte_agua}': {e}")
else:
    print(f"DataFrame df_corte_agua_silver_final não disponível para salvar.")

# Salvar df_consumo_agua_silver_final
if 'df_consumo_agua_silver_final' in locals() and df_consumo_agua_silver_final is not None:
    try:
        print(f"\nSalvando tabela Silver '{nome_tabela_silver_consumo_agua}' em: {caminho_delta_silver_consumo_agua}...")
        (df_consumo_agua_silver_final.write
         .format("delta")
         .mode("overwrite")
         .save(caminho_delta_silver_consumo_agua))
        print(f" Tabela Delta Silver '{nome_tabela_silver_consumo_agua}' salva com sucesso!")
    except Exception as e:
        print(f" Erro ao salvar a tabela Delta Silver '{nome_tabela_silver_consumo_agua}': {e}")
else:
    print(f"DataFrame df_consumo_agua_silver_final não disponível para salvar.")






