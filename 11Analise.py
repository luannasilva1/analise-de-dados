# Imports atualizados para incluir 'count' e 'lit' para a nova Análise 3
from pyspark.sql.functions import avg, col, desc, when, count, lit

# Caminho para a tabela Gold consolidada
caminho_tabela_gold_geral_clientes = "dbfs:/FileStore/tables/Gold/geral_clientes_consumo_cortes_gold"

print(f"--- Carregando a Tabela Gold: {caminho_tabela_gold_geral_clientes} ---")

try:
    df_gold = spark.read.format("delta").load(caminho_tabela_gold_geral_clientes)
    print("Tabela Gold carregada com sucesso!\n")
    # df_gold.printSchema() # Descomente se quiser ver o schema novamente

    # --- Início das Análises Adaptadas para Nulos ---

    print("Análise 1: Clientes com maior consumo e atraso (excluindo nulos nesses campos)")
    # Filtra para que ambos consumo_m3 e tempo_atraso_dias não sejam nulos para a ordenação fazer sentido
    df_gold.filter(col('consumo_m3').isNotNull() & col('tempo_atraso_dias').isNotNull()) \
           .orderBy(col('consumo_m3').desc(), col('tempo_atraso_dias').desc()) \
           .select("nome_cliente", "cidade", "bairro", "consumo_m3", "tempo_atraso_dias", "status_corte") \
           .show(10, truncate=False)

    print("\nAnálise 2: Média de consumo por bairro (excluindo bairros nulos)")
    # Filtra bairros nulos antes de agrupar. avg() já ignora consumo_m3 nulos.
    df_gold.filter(col('bairro').isNotNull()) \
           .groupBy('bairro') \
           .agg(avg('consumo_m3').alias('media_consumo_m3')) \
           .orderBy(col('media_consumo_m3').desc()) \
           .show(truncate=False)

    # --- ANÁLISE 3 SUBSTITUÍDA ---
    print("\nAnálise 3 (Ajustada): Valor médio da conta por cada status_corte existente")
    # Filtramos para garantir que tanto 'status_corte' quanto 'valor_conta' não sejam nulos
    df_analise_status_geral = df_gold.filter(
        col('status_corte').isNotNull() & 
        col('valor_conta').isNotNull()
    )
    # Agrupamos por 'status_corte' e calculamos a média de 'valor_conta'
    df_media_conta_por_status = df_analise_status_geral.groupBy('status_corte') \
        .agg(
            avg('valor_conta').alias('media_valor_conta'),
            count(lit(1)).alias('quantidade_registros') # Contar quantos registros contribuem para cada média
        ) \
        .orderBy(col('media_valor_conta').desc()) # Ordenar pela média
    
    # Verificamos se o DataFrame resultante da agregação tem alguma linha
    if df_media_conta_por_status.count() > 0:
        df_media_conta_por_status.show(truncate=False)
    else:
        print("Não foram encontrados dados para calcular o valor médio da conta por status_corte.")
    # --- FIM DA ANÁLISE 3 SUBSTITUÍDA ---

    print("\nAnálise 4: Percentual médio de imposto (considerando apenas onde o percentual pôde ser calculado)")
    df_gold_com_perc_imposto = df_gold.withColumn(
        'percentual_imposto',
        when(col('valor_conta').isNotNull() & (col('valor_conta') != 0) & col('valor_imposto').isNotNull(),
             (col('valor_imposto') / col('valor_conta')) * 100)
        .otherwise(None)
    )
    df_gold_com_perc_imposto.filter(col('percentual_imposto').isNotNull()) \
                            .agg(avg('percentual_imposto').alias('media_percentual_imposto')) \
                            .show(truncate=False)

    print("\nAnálise 5: Regiões com mais cortes (excluindo regiões nulas e considerando apenas cortes válidos)")
    # Filtra por data_corte não nula (indicador de corte) e regiao não nula.
    df_gold.filter(col("data_corte").isNotNull() & col("regiao").isNotNull()) \
           .groupBy('regiao') \
           .count() \
           .orderBy(col('count').desc()) \
           .withColumnRenamed("count", "numero_de_cortes") \
           .show(truncate=False)

except Exception as e:
    print(f" Erro ao carregar ou analisar a tabela Gold: {e}")


