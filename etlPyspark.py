from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("ETL_MySQL_to_PostgreSQL") \
    .config("spark.jars", "file:///C:/conectores/mysql-connector-j-8.0.31.jar,file:///C:/conectores/postgresql-42.7.4.jar") \
    .getOrCreate()

# Simulando credenciales de conexión
mysql_url = "jdbc:mysql://localhost:3306/chilecompra_etl"
mysql_properties = {
    "user": "root",
    "password": "123qwe",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Cargar datos desde MySQL
usuarios_df = spark.read \
    .jdbc(url=mysql_url, table="usuarios", properties=mysql_properties)

# Mostrar las primeras filas para verificar los datos
usuarios_df.show()

# Realizar una operación sencilla en el DataFrame
# Ejemplo: Supongamos que queremos agregar una columna 'edad_doble' multiplicando una columna 'edad' por 2
usuarios_con_edad_doble_df = usuarios_df.withColumn("rut_dv", col("rut_dv") * 2)

# Mostrar el DataFrame modificado
usuarios_con_edad_doble_df.show()

# Simulando la conexión a PostgreSQL
postgres_url = "jdbc:postgresql://localhost:5432/postgres"
postgres_properties = {
    "user": "postgres",
    "password": "123qwe",
    "driver": "org.postgresql.Driver"
}

# Guardar el DataFrame resultante en PostgreSQL
usuarios_con_edad_doble_df.write \
    .jdbc(url=postgres_url, table="usuarios", mode="overwrite", properties=postgres_properties)



spark.stop()
