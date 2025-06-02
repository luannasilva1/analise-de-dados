# Imports já devem estar carregados
from pyspark.sql.functions import col, to_date, trim, lower, upper, when, sum, regexp_replace # regexp_replace adicionado
from pyspark.sql.types import DecimalType

# Usar o DataFrame carregado na Célula 5: df_consumo_agua_silver_entrada
if 'df_consumo_agua_silver_entrada' in locals() and df_consumo_agua_silver_entrada is not None:
    print("\n--- Iniciando Limpeza e Padronização (Silver) para 'consumo_agua' (Data de Vencimento CORRIGIDA) ---")

    escala_monetaria = DecimalType(18, 2)
    escala_consumo = DecimalType(18, 3)

    # Para visualização antes da transformação (descomente para depurar)
    # print("Amostra de 'data_vencimento_str' ANTES da limpeza e conversão:")
    # df_consumo_agua_silver_entrada.select("data_vencimento_str").show(10, truncate=False)

    df_consumo_agua_silver_transformado = df_consumo_agua_silver_entrada.select(
        trim(lower(col("regiao"))).alias("regiao"),
        trim(lower(col("cidade"))).alias("cidade"),
        trim(lower(col("bairro"))).alias("bairro"),
        col("consumo_m3_str").cast(escala_consumo).alias("consumo_m3"),
        col("valor_conta_str").cast(escala_monetaria).alias("valor_conta"),
        col("valor_imposto_str").cast(escala_monetaria).alias("valor_imposto"),
        trim(lower(col("nome_cliente"))).alias("nome_cliente"),
        trim(col("mes_referencia_str")).alias("mes_referencia"),

        # CORREÇÃO para data_vencimento_str:
        # 1. Remover o "dd" (ou "\"dd\"") do final da string.
        #    Se o final for literalmente 'dd' (sem aspas), usamos 'dd$'
        #    Se o final for literalmente '"dd"' (com aspas), usamos "\"dd\"$"
        #    Assumindo que seja 'dd' sem aspas, como em "19/10/2024dd"
        to_date(
            regexp_replace(col("data_vencimento_str"), "dd$", ""), # Remove 'dd' do final da string
            "dd/MM/yyyy" # Formato da parte da data que sobra (ex: "19/10/2024")
        ).alias("data_vencimento")
    )

    print("\nContagem de nulos em colunas numéricas e de data após conversão (consumo):")
    df_consumo_agua_silver_transformado.select(
        when(col("consumo_m3").isNull(), 1).otherwise(0).alias("eh_nulo_consumo_m3"),
        when(col("valor_conta").isNull(), 1).otherwise(0).alias("eh_nulo_valor_conta"),
        when(col("data_vencimento").isNull(), 1).otherwise(0).alias("eh_nulo_data_vencimento")
    ).groupBy().agg(
        sum("eh_nulo_consumo_m3").alias("total_nulos_consumo_m3"),
        sum("eh_nulo_valor_conta").alias("total_nulos_valor_conta"),
        sum("eh_nulo_data_vencimento").alias("total_nulos_data_vencimento")
    ).show()

    print("\nSchema de df_consumo_agua_silver_transformado:")
    df_consumo_agua_silver_transformado.printSchema()
    print("Amostra de df_consumo_agua_silver_transformado (verifique 'data_vencimento'):")
    df_consumo_agua_silver_transformado.show(5, truncate=False)

    df_consumo_agua_silver_final = df_consumo_agua_silver_transformado # Atribuindo para uso futuro
else:
    print("DataFrame 'df_consumo_agua_silver_entrada' não está carregado ou é None. Pule esta etapa.")