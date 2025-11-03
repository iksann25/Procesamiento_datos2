from pyspark import SparkContext

def parse_line(line):
    parts = line.strip().split(";")
    if len(parts) == 3:
        persona, metodo, gasto = parts
        return (persona, metodo, float(gasto))
    else:
        return None

def not_none(record):
    return record is not None

def gasto_sin_tarjeta(record):
    persona, metodo, gasto = record
    if metodo == "Tarjeta de crédito":
        return (persona, 0.0)
    else:
        return (persona, gasto)

def sum_gastos(a, b):
    return a + b

def format_output(record):
    return f"{record[0]};{int(record[1])}"

if __name__ == "__main__":
    sc = SparkContext(appName="personaGastosSinTarjetaCredito")

    lines = sc.textFile("hdfs:///user/dani/datasets/casoDePrueba1.txt")

    data = lines.map(parse_line).filter(not_none)
    print("Particiones en data:", data.getNumPartitions())

    gastos_sin_tdc = data.map(gasto_sin_tarjeta)
    print("Particiones en gastos_sin_tdc:", gastos_sin_tdc.getNumPartitions())

    suma_por_persona = gastos_sin_tdc.reduceByKey(sum_gastos)
    print("Particiones en suma_por_persona:", suma_por_persona.getNumPartitions())

    salida = suma_por_persona.map(format_output)

    salida.saveAsTextFile("hdfs:///user/dani/output/personaGastosSinTarjetaCredito")

    sc.stop()
