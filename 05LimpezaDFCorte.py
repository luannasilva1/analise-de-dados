from pyspark.sql.functions import col, to_date, trim, lower, upper, when, sum
from pyspark.sql.types import IntegerType, DateType

# ANTES de executar as transformações, se você configurou para LEGACY para teste,
# você pode tentar voltar para o padrão (CORRECTED) para ver se o formato correto já resolve:
# spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED") # ou comente a linha que define como LEGACY

# Usar o DataFrame carregado na célula anterior: df_corte_agua_silver_entrada
if 'df_corte_agua_silver_entrada' in locals() and df_corte_agua_silver_entrada is not None:
    print("\n--- Iniciando Limpeza e Padronização (Silver) para 'corte_agua' (Data CORRIGIDA) ---")

    df_corte_agua_silver_transformado = df_corte_agua_silver_entrada.select(
        trim(lower(col("nome_cliente"))).alias("nome_cliente"),
        col("tempo_atraso_conta").cast(IntegerType()).alias("tempo_atraso_dias"),

        # CORREÇÃO AQUI: Usar "dd/MM/yyyy" para o formato com ano de 4 dígitos
        to_date(col("data_corte_str"), "dd/MM/yyyy").alias("data_corte"),

        trim(lower(col("status_corte"))).alias("status_corte"),
        trim(lower(col("localidade"))).alias("localidade")
    )

    print("\nContagem de nulos em 'data_corte' e 'tempo_atraso_dias' após conversão:")
    # Esta ação (.show()) pode disparar o erro se a conversão ainda falhar.
    try:
        df_corte_agua_silver_transformado.select(
            when(col("data_corte").isNull(), 1).otherwise(0).alias("eh_nulo_data_corte"),
            when(col("tempo_atraso_dias").isNull(), 1).otherwise(0).alias("eh_nulo_tempo_atraso_dias")
        ).groupBy().agg(
            sum("eh_nulo_data_corte").alias("total_nulos_data_corte"),
            sum("eh_nulo_tempo_atraso_dias").alias("total_nulos_tempo_atraso_dias")
        ).show()

        print("\nSchema de df_corte_agua_silver_transformado:")
        df_corte_agua_silver_transformado.printSchema()
        print("Amostra de df_corte_agua_silver_transformado:")
        df_corte_agua_silver_transformado.show(5, truncate=False)

        df_corte_agua_silver_final = df_corte_agua_silver_transformado # Atribuindo para uso futuro

    except Exception as e:
        print(f" Ocorreu um erro durante a transformação ou ao mostrar os dados: {e}")
        print("Verifique se todas as datas na coluna 'data_corte_str' seguem o formato 'dd/MM/yyyy'.")
        print("DataFrame 'df_corte_agua_silver_transformado' pode não ter sido completamente definido ou pode conter erros.")

else:
    print("DataFrame 'df_corte_agua_silver_entrada' não está carregado ou é None. Pule esta etapa.")