from pyspark.sql.functions import col, coalesce

print("--- Iniciando Criação da Tabela Geral de Clientes na Camada Gold (Full Outer Join) ---")

# DataFrames de entrada da Camada Silver (já limpos e com tipos corrigidos)
# df_consumo_agua_silver_final (com nome_cliente, cidade, bairro, etc.)
# df_corte_agua_silver_final (com nome_cliente, cidade_corte_derivada, bairro_corte_derivado, etc.)

df_consumo_pronto = 'df_consumo_agua_silver_final' in locals() and df_consumo_agua_silver_final is not None
df_corte_pronto = 'df_corte_agua_silver_final' in locals() and df_corte_agua_silver_final is not None

if df_consumo_pronto and df_corte_pronto:
    print("DataFrames 'df_consumo_agua_silver_final' e 'df_corte_agua_silver_final' estão prontos.")

    # Condição de junção composta
    condicao_join_gold = (
        (df_consumo_agua_silver_final["nome_cliente"] == df_corte_agua_silver_final["nome_cliente"]) &
        (df_consumo_agua_silver_final["cidade"] == df_corte_agua_silver_final["cidade_corte_derivada"]) &
        (df_consumo_agua_silver_final["bairro"] == df_corte_agua_silver_final["bairro_corte_derivado"])
    )

    try:
        # Realizar o 'full_outer join'
        df_tabela_geral_gold_intermediaria = df_consumo_agua_silver_final.join(
            df_corte_agua_silver_final,
            condicao_join_gold,
            "full_outer" # Mantém todos os registros de ambas as tabelas
        )

        print("\nSchema após full_outer join (antes de tratar colunas chave):")
        df_tabela_geral_gold_intermediaria.printSchema()
        # df_tabela_geral_gold_intermediaria.show(5, truncate=False) # Para depuração

        # Tratar as colunas chave para criar versões finais (coalesce)
        # e selecionar as demais colunas desejadas de ambas as tabelas.
        # As colunas de df_consumo_agua_silver_final são referenciadas diretamente.
        # As colunas de df_corte_agua_silver_final precisam ser referenciadas através do objeto DataFrame original
        # para evitar ambiguidade se você não usou alias no join.
        # No entanto, Spark >= 3.0 é melhor em resolver isso, mas ser explícito é mais seguro.

        df_tabela_geral_gold_final = df_tabela_geral_gold_intermediaria.select(
            # Chaves finais usando coalesce para pegar o valor não nulo de qualquer uma das tabelas
            coalesce(df_consumo_agua_silver_final["nome_cliente"], df_corte_agua_silver_final["nome_cliente"]).alias("nome_cliente_unico"),
            coalesce(df_consumo_agua_silver_final["cidade"], df_corte_agua_silver_final["cidade_corte_derivada"]).alias("cidade_unica"),
            coalesce(df_consumo_agua_silver_final["bairro"], df_corte_agua_silver_final["bairro_corte_derivado"]).alias("bairro_unico"),

            # Colunas da tabela de consumo (excluindo as chaves já tratadas se você preferir)
            df_consumo_agua_silver_final["regiao"],
            df_consumo_agua_silver_final["consumo_m3"],
            df_consumo_agua_silver_final["valor_conta"],
            df_consumo_agua_silver_final["valor_imposto"],
            df_consumo_agua_silver_final["mes_referencia"],
            df_consumo_agua_silver_final["data_vencimento"],
            # Adicione outras colunas específicas de consumo_agua que você quer manter

            # Colunas da tabela de corte (excluindo as chaves já tratadas se você preferir)
            # Note que se a coluna já existia em consumo (além das chaves), ela será duplicada.
            # No nosso caso, as colunas de dados são distintas.
            df_corte_agua_silver_final["tempo_atraso_dias"],
            df_corte_agua_silver_final["data_corte"],
            df_corte_agua_silver_final["status_corte"]
            # Adicione outras colunas específicas de corte_agua que você quer manter
        )
        
        # Renomear as colunas chave unificadas para nomes mais simples, se desejar
        df_tabela_geral_gold_final = df_tabela_geral_gold_final.withColumnRenamed("nome_cliente_unico", "nome_cliente") \
                                                              .withColumnRenamed("cidade_unica", "cidade") \
                                                              .withColumnRenamed("bairro_unico", "bairro")


        print("\nSchema da Tabela Geral Gold Final (após coalesce e seleção):")
        df_tabela_geral_gold_final.printSchema()

        print("\nAmostra da Tabela Geral Gold Final:")
        df_tabela_geral_gold_final.show(20, truncate=False)

        contagem_total_gold = df_tabela_geral_gold_final.count()
        print(f"\nNúmero total de linhas na Tabela Geral Gold: {contagem_total_gold}")

        # --- Salvando a Tabela Geral Gold ---
        caminho_base_gold = "dbfs:/FileStore/tables/Gold"
        nome_tabela_gold_geral_clientes = "geral_clientes_consumo_cortes_gold" # Nome mais descritivo
        caminho_delta_gold_geral_clientes = f"{caminho_base_gold}/{nome_tabela_gold_geral_clientes}"

        print(f"\nSalvando Tabela Geral Gold em: {caminho_delta_gold_geral_clientes}...")
        (df_tabela_geral_gold_final.write
         .format("delta")
         .mode("overwrite")
         .save(caminho_delta_gold_geral_clientes))
        print(f" Tabela Delta '{nome_tabela_gold_geral_clientes}' salva com sucesso na Camada Gold!")

    except Exception as e:
        print(f" Erro durante a criação ou salvamento da Tabela Geral Gold: {e}")
else:
    print(" DataFrames Silver finais ('df_consumo_agua_silver_final' ou 'df_corte_agua_silver_final') não estão disponíveis ou são None.")



caminho = "dbfs:/FileStore/tables/Gold/geral_clientes_consumo_cortes_gold"
df_gold_visualizacao = spark.read.format("delta").load(caminho)


df_gold_visualizacao.count()
