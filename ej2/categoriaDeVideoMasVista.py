from pyspark import SparkContext

sc = SparkContext(appName="CategoriaDeVideosMasVista")

input_folder = "/user/dani/0302/[0-9]*.txt"
output_folder = "/user/dani/categoriaDeVideoMasVista"

rdd = sc.textFile(input_folder)

def process_line(line):
    columns = line.split("\t")
    if len(columns) > 6:
        category = columns[3].strip()
        try:
            views = float(columns[5])
        except ValueError:
            views = 0.0
        return (category, views)
    else:
        return None

def filter_none_and_empty(x):
    return x is not None and x[0] != ""

def sum_views(x, y):
    return x + y

def sort_by_views(item):
    # Para orden descendente, devolvemos el negativo
    return -item[1]

def take_first_partition(index, iterator):
    if index == 0:
        for i, row in enumerate(iterator):
            if i == 0:
                yield f"{row[0]};{int(row[1])}"
    # las demás particiones no devuelven nada

processed_rdd = rdd.map(process_line).filter(filter_none_and_empty)

category_views_rdd = processed_rdd.reduceByKey(sum_views)

# Ordenamos por visitas descendente
ordered_rdd = category_views_rdd.sortBy(sort_by_views)

# Tomamos solo la primera fila en el RDD
top_rdd = ordered_rdd.mapPartitionsWithIndex(take_first_partition)

# Guardamos el resultado
top_rdd.saveAsTextFile(output_folder)

sc.stop()
