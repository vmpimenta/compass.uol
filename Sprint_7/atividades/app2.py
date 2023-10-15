# Tarefa: Apache Spark - Contador de Palavras

from pyspark.sql import SparkSession
import string
if __name__ == "__main__":
    spark = SparkSession.builder.appName("teste").getOrCreate()
    arquivo = open("README.md")
    texto = arquivo.read().lower()
    # troquei os caracteres especiais por espaço em branco.
    for c in string.punctuation:
        texto = texto.replace(c, ' ')
    texto = texto.split() #separando por palavras
    counter = {}
    for palavras in texto:
        counter[palavras] = counter.get(palavras, 0) + 1
    print(counter)
    spark.stop()
    type(counter)