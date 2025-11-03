from pyspark import SparkContext

sc = SparkContext(appName="personaYMetodosDePago")

input_file = "/user/dani/entrada.txt"
output_folder_mayor = "/user/dani/comprasConTDCMayorDe1500"
output_folder_menor = "/user/dani/comprasConTDCMenoroIgualDe1500"

rdd = sc.textFile(input_file)

def process_line(line):
    parts = line.strip().split(";")
    if len(parts) == 3:
        persona = parts[0].strip()
        metodo = parts[1].strip().lower()
        try:
            gasto = float(parts[2].strip())
        except ValueError:
            gasto = 0.0
        return (persona, metodo, gasto)
    else:
        return None

def not_none(x):
    return x is not None

def filter_tarjeta(x):
    return "tarjeta" in x[1]

def gasto_mayor_1500(x):
    return x[2] > 1500

def gasto_menor_igual_1500(x):
    return x[2] <= 1500

def map_persona_uno(x):
    return (x[0], 1)

def sum_values(a, b):
    return a + b

def format_output(x):
    return f"{x[0]};{x[1]}"

processed_rdd = rdd.map(process_line).filter(not_none)

# Cacheamos porque lo usamos 2 veces
tdc_rdd = processed_rdd.filter(filter_tarjeta).cache()

# a) Compras con TDC > 1500
mayor_rdd = (
    tdc_rdd
    .filter(gasto_mayor_1500)
    .map(map_persona_uno)
    .reduceByKey(sum_values)
)

# b) Compras con TDC ≤ 1500
menor_rdd = (
    tdc_rdd
    .filter(gasto_menor_igual_1500)
    .map(map_persona_uno)
    .reduceByKey(sum_values)
)

# Convertir a texto
mayor_rdd_text = mayor_rdd.map(format_output)
menor_rdd_text = menor_rdd.map(format_output)

# Guardar resultados
mayor_rdd_text.saveAsTextFile(output_folder_mayor)
menor_rdd_text.saveAsTextFile(output_folder_menor)

# Liberar memoria
tdc_rdd.unpersist()

sc.stop()
