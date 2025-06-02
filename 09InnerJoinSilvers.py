from pyspark.sql.functions import col

print("--- Iniciando Junção Composta das Tabelas da Camada Silver ---")

# Verificar se os DataFrames de entrada da Silver estão disponíveis e não são None
df_consumo_pronto = 'df_consumo_agua_silver_final' in locals() and df_consumo_agua_silver_final is not None
df_corte_pronto = 'df_corte_agua_silver_final' in locals() and df_corte_agua_silver_final is not None

if df_consumo_pronto and df_corte_pronto:
    print("DataFrames 'df_consumo_agua_silver_final' e 'df_corte_agua_silver_final' estão prontos para a junção composta.")

    # Definir as condições da junção
    # Queremos que nome_cliente, cidade e bairro sejam iguais.
    # Lembre-se que em df_corte_agua_silver_final, as colunas são cidade_corte_derivada e bairro_corte_derivado.
    condicao_join = (
        (df_consumo_agua_silver_final["nome_cliente"] == df_corte_agua_silver_final["nome_cliente"]) &
        (df_consumo_agua_silver_final["cidade"] == df_corte_agua_silver_final["cidade_corte_derivada"]) &
        (df_consumo_agua_silver_final["bairro"] == df_corte_agua_silver_final["bairro_corte_derivado"])
    )

    try:
        # Realizar o 'inner join'
        # Um 'inner join' manterá apenas as linhas onde a condição_join é verdadeira para ambas as tabelas.
        df_dados_consolidados_silver = df_consumo_agua_silver_final.join(
            df_corte_agua_silver_final,
            condicao_join,
            "inner" # Tipo de junção: apenas registros que existem em AMBOS com as chaves correspondentes
        )

        # Após o join com condição explícita, as colunas chave de ambas as tabelas são mantidas.
        # Vamos remover as colunas chave duplicadas da tabela de corte para evitar redundância,
        # mantendo as da tabela de consumo como as "principais".
        df_dados_consolidados_silver = df_dados_consolidados_silver.drop(
            df_corte_agua_silver_final["nome_cliente"]
        ).drop(
            df_corte_agua_silver_final["cidade_corte_derivada"]
        ).drop(
            df_corte_agua_silver_final["bairro_corte_derivado"]
        )
        
        # Opcional: Renomear as colunas chave que sobraram (da tabela de consumo) se desejar nomes "finais"
        # df_dados_consolidados_silver = df_dados_consolidados_silver.withColumnRenamed("nome_cliente", "nome_cliente_final") \
        #                                                          .withColumnRenamed("cidade", "cidade_final") \
        #                                                          .withColumnRenamed("bairro", "bairro_final")


        print("\nSchema do DataFrame consolidado após junção composta:")
        df_dados_consolidados_silver.printSchema()

        print("\nAmostra dos dados consolidados (junção composta):")
        df_dados_consolidados_silver.show(10, truncate=False)

        contagem_consolidados = df_dados_consolidados_silver.count()
        print(f"\nNúmero de linhas no DataFrame consolidado (após inner join): {contagem_consolidados}")
        
        # Este DataFrame df_dados_consolidados_silver agora contém apenas os registros
        # que tinham correspondência de nome, cidade e bairro em ambas as fontes originais.
        # Ele está pronto para ser salvo como uma tabela Silver integrada ou usado para a camada Gold.

    except Exception as e:
        print(f" Erro durante a operação de junção composta: {e}")

else:
    print(" DataFrames Silver finais ('df_consumo_agua_silver_final' ou 'df_corte_agua_silver_final') não estão disponíveis ou são None. A junção não pode ser realizada.")

print("\n--- Junção Composta da Camada Silver concluída ---")