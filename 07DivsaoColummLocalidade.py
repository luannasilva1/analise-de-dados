from pyspark.sql.functions import col, split, regexp_replace, trim, when, size # size importado para clareza

# Continuar a partir do DataFrame df_corte_agua_silver_final da Célula 6
if 'df_corte_agua_silver_final' in locals() and df_corte_agua_silver_final is not None:
    print("\n--- Iniciando Divisão e Limpeza da Coluna 'localidade' em df_corte_agua_silver_final (Mapeamento Ajustado) ---")

    # Visualizar a coluna 'localidade' antes da transformação
    print("Amostra da coluna 'localidade' ANTES da divisão:")
    df_corte_agua_silver_final.select("localidade").show(10, truncate=False)

    # Dividir a coluna 'localidade' pelo delimitador " - "
    df_com_localidade_splitada = df_corte_agua_silver_final.withColumn(
        "localidade_splitada",
        split(col("localidade"), " - ")
    )

    # Criar as novas colunas 'cidade_corte_derivada' e 'bairro_corte_derivado'
    # Mapeamento: Parte 1 -> cidade, Parte 2 -> bairro
    # Remover todos os espaços das partes resultantes.
    df_corte_agua_silver_atualizado = df_com_localidade_splitada.withColumn(
        "cidade_corte_derivada", # Parte 1 do split
        when(col("localidade_splitada").isNotNull() & (size(col("localidade_splitada")) >= 1),
             trim(regexp_replace(col("localidade_splitada").getItem(0), " ", "")))
        .otherwise(None) # Se não houver " - ", esta parte pode ser a 'localidade' inteira ou nulo, dependendo da regra.
                         # Aqui, se não houver split, não teremos uma "parte 1" clara para cidade e "parte 2" para bairro.
                         # Se a localidade original sem " - " for a cidade, você precisaria de lógica adicional.
                         # Por ora, se não houver split claro em duas partes, a segunda parte (bairro) será nula.
    ).withColumn(
        "bairro_corte_derivado", # Parte 2 do split
        when(col("localidade_splitada").isNotNull() & (size(col("localidade_splitada")) >= 2),
             trim(regexp_replace(col("localidade_splitada").getItem(1), " ", "")))
        .otherwise(None) # Se não houver segunda parte, fica nulo
    )

    # Lógica adicional se 'localidade' não contiver " - ":
    # Se 'localidade' não tem " - ", talvez ela represente apenas a cidade ou apenas o bairro.
    # Você precisaria definir como tratar esses casos.
    # Exemplo: se não houver " - ", assumir que 'localidade' original é a 'cidade_corte_derivada' e 'bairro_corte_derivado' é nulo.
    df_corte_agua_silver_atualizado = df_corte_agua_silver_atualizado.withColumn(
        "cidade_corte_derivada",
        when(size(col("localidade_splitada")) == 1, trim(regexp_replace(col("localidade_splitada").getItem(0), " ", ""))) # Se só tem 1 parte, é cidade
        .otherwise(col("cidade_corte_derivada")) # Mantém o valor do when anterior (se tinha 2 partes)
    ).withColumn(
        "bairro_corte_derivado",
        when(size(col("localidade_splitada")) == 1, None) # Se só tem 1 parte, bairro é nulo
        .otherwise(col("bairro_corte_derivado")) # Mantém o valor do when anterior (se tinha 2 partes)
    )


    # Opcional: Remover a coluna array intermediária e a original 'localidade'
    df_corte_agua_silver_atualizado = df_corte_agua_silver_atualizado.drop("localidade_splitada", "localidade")

    print("\nSchema APÓS divisão e limpeza da 'localidade' (Mapeamento Ajustado):")
    df_corte_agua_silver_atualizado.printSchema()

    print("\nAmostra dos dados com 'cidade_corte_derivada' e 'bairro_corte_derivado' (Mapeamento Ajustado):")
    df_corte_agua_silver_atualizado.select(
        "nome_cliente",
        "cidade_corte_derivada",
        "bairro_corte_derivado",
        "data_corte",
        "status_corte"
    ).show(20, truncate=False) # Mostrar mais linhas para verificar diversos casos

    # Atualizar a variável df_corte_agua_silver_final para refletir estas mudanças
    df_corte_agua_silver_final = df_corte_agua_silver_atualizado
    print("\nDataFrame 'df_corte_agua_silver_final' foi atualizado com as novas colunas de localidade (Mapeamento Ajustado).")

else:
    print(" DataFrame 'df_corte_agua_silver_final' não está disponível. A divisão da localidade não pode ser realizada.")




