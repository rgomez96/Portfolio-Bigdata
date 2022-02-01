## 1. Creación de tablas en formato texto.

**1.1 Crear Base de datos "datos_padron".**

    create database datos_padron;

    use datos_padron;


**1.2 Crear la tabla de datos padron_txt con todos los campos del fichero CSV y cargar los datos mediante el comando LOAD DATA LOCAL INPATH. La tabla tendrá formato texto y tendrá como delimitador de campo el caracter ';' y los campos que en el documento original están encerrados en comillas dobles '"' no deben estar envueltos en estos caracteres en la tabla de Hive (es importante indicar esto utilizando el serde de OpenCSV, si no la importación de las variables que hemos indicado como numéricas fracasará ya que al estar envueltos en comillas los toma como strings) y se deberá omitir la cabecera del fichero de datos al crear la tabla.**



    create table padron_txt(
        COD_DISTRITO int,
        DESC_DISTRITO string,
        COD_DIST_BARRIO int,
        DESC_BARRIO string,
        COD_BARRIO int,
        COD_DIST_SECCION string,
        COD_SECCION int,
        COD_EDAD_INT int,
        EspanolesHombres int,
        EspanolesMujeres int,
        ExtranjerosHombres int,
        ExtranjerosMujeres int)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES ("separatorChar" = "\;", "quoteChar" = "\"")
        tblproperties("skip.header.line.count"="1");

Relevante: OpenCSVSerde sólo puede trabajar con String así que transforma los datos que originalmente eran int a String. Habrá que crear otra tabla a posterior y trasvasar los datos haciendo cast()

Como nos pode lectura *LOAD DATA LOCAL INPATH* no es necesario cargar el archivo CSV al HDFS (porque es local).

    load data local inpath 
    '/home/cloudera/Downloads/Rango_Edades_Seccion_202112.csv' into table padron_txt;

**1.3 Hacer trim sobre lo datos para eliminar los espacios innecesarios guardando la tabla resultado como padron_txt_2. (Este apartado se puede hacer creando la tabla con una secuencia CTAS)**

    create table padron_txt_2 as select
        cast(cod_distrito as int),
        trim(desc_distrito) as desc_distrito,
        cast(cod_dist_barrio as int),
        trim(desc_barrio) as desc_barrio,
        cast(cod_barrio as int),
        cast(cod_dist_seccion as int),
        cast(cod_seccion as int),
        cast(cod_edad_int as int),
        cast(EspanolesHombres as int),
        cast(EspanolesMujeres  as int),
        cast(ExtranjerosHombres as int),
        cast(ExtranjerosMujeres as int) 
    from padron_txt; 

**1.4 Investigar y entender la diferencia de incluir la palabra LOCAL en el comando LOAD DATA**

Si la lectura es LOCAL se puede leer un archivo directamente del sistema de ficheros de Linux, en lugar del HDFS.

**1.5 En este momento te habrás dado cuenta de un aspecto importante, los datos nulos de nuestras tablas vienen representados por un espacio vacío y no por un identificador de nulos comprensible para la tabla. Esto puede ser un problema para el tratamiento posterior de los datos. Podrías solucionar esto creando una nueva tabla utiliando sentencias case when que sustituyan espacios en blanco por 0. Para esto primero comprobaremos que solo hay espacios en blanco en las variables numéricas correspondientes a las últimas 4 variables de nuestra tabla (podemos hacerlo con alguna sentencia de HiveQL) y luego aplicaremos las sentencias case when para sustituir por 0 los espacios en blanco. (Pista: es útil darse cuenta de que un espacio vacío es un campo con longitud 0). Haz esto solo para la tabla padron_txt.**

    create table padron_txt_3 as select
    case
        when length(cod_distrito) == 0 then '0'
        else cod_distrito
    end as cod_distrito,
    desc_distrito,
    case
        when length(cod_dist_barrio) == 0 then '0'
        else cod_dist_barrio
    end as cod_dist_barrio,
    desc_barrio,
    case
        when length(cod_barrio) == 0 then '0'
        else cod_barrio
    end as cod_barrio,
    case
        when length(cod_dist_seccion) == 0 then '0'
    end as cod_dist_seccion,
    case
        when length(cod_seccion) == 0 then '0'
        else cod_seccion
    end as cod_seccion,
    case
        when length(cod_edad_int) == 0 then '0'
        else cod_edad_int
    end as cod_edad_int,
    case
        when length(espanoleshombres) == 0 then '0'
        else espanoleshombres
    end as espanoleshombres,
    case
        when length(espanolesmujeres) == 0 then '0'
        else espanolesmujeres
    end as espanolesmujeres,
    case
        when length(extranjeroshombres) == 0 then '0'
        else extranjeroshombres
    end as extranjeroshombres,
    case
        when length(extranjerosmujeres) == 0 then '0'
        else extranjerosmujeres
    end as extranjerosmujeres
    from padron_txt;

Mucho texto

**1.6 Una manera tremendamente potente de solucionar todos los problemas previos (tanto las comillas como los campos vacíos que no son catalogados como null y los espacios innecesarios) es utilizar expresiones regulares (regex) que nos proporciona OpenCSV.
Para ello utilizamos :**

 **ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
 WITH SERDEPROPERTIES ('input.regex'='XXXXXXX')**

 **Donde XXXXXX representa una expresión regular que debes completar y que identifique el formato exacto con el que debemos interpretar cada una de las filas de nuestro CSV de entrada. Para ello puede ser útil el portal "regex101". Utiliza este método para crear de nuevo la tabla padron_txt_2.**

 **Una vez finalizados todos estos apartados deberíamos tener una tabla padron_txt que conserve los espacios innecesarios, no tenga comillas envolviendo los campos y los campos nulos sean tratados como valor 0 y otra tabla padron_txt_2 sin espacios innecesarios, sin comillas envolviendo los campos y con los campos nulos como valor 0. Idealmente esta tabla ha sido creada con las regex de OpenCSV.**


    create table if not exists padron_txt1(
        COD_DISTRITO int,
        DESC_DISTRITO string,
        COD_DIST_BARRIO int,
        DESC_BARRIO string,
        COD_BARRIO int,
        COD_DIST_SECCION int,
        COD_SECCION int,
        COD_EDAD_INT int,
        EspanolesHombres int,
        EspanolesMujeres int,
        ExtranjerosHombres int,
        ExtranjerosMujeres int)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    WITH SERDEPROPERTIES ('input.regex'='"(.*)";"([A-Za-z]*) *";"(.*)";"([A-Za-z]*) *";"(.*)";"(.*?)";"(.*?)";"(.*?)";"(.*?)";"(.*?)";"(.*?)";"(.*?)"')
    stored as textfile
    tblproperties("skip.header.line.count"="1");

    load data local inpath '/home/cloudera/Downloads/Rango_Edades_Seccion_202112.csv' into table padron_txt1;

## 2. Investigamos a fondo el formato columnar parquet.

**2.1 ¿Qué es CTAS?**

CREATE TABLE AS SELECT datos FROM tabla

**2.2 Crear tabla Hive padron_parquet(cuyos datos serán almacenados en el formato columnar parquet) a través de la tabla padron_txt mediante un CTAS**

    create table padron_parquet stored as parquet as select * from padron_txt_espacios;

**2.3 Crear tabla Hive padron_parquet_2 a través de la tabla padron_txt_2 mediante un CTAS. En este punto deberíamos tener 4 tablas, 2 en txt (padron_txt y padron_txt_2, la primera con espacios innecesrios y la segunda sin espacios innecesarios) y otras 2 tablas en formato parquet (padron_parquet y padron_parquet_2, la primera con espacios y la segunda sin ellos).**

    create table padron_parquet_2 stored as parquet as select * from padron_txt_2;

**2.4 Opcionalmente se pueden crear las tablas directamente desde 0 (en lugar de mediante CTAS) en formato parquet igual que lo hicimos para el formato txt incluyendo la secuencia STORED AS PARQUET**

Ya tenemos las tablas y es copia pega añadiendo una línea.

**2.5 Investigar en qué consiste el formato columnar parquet y las ventajas de trabajar con este tipo de formatos**

Parquet es un formato columnar multiplataforma que almacena los datos en formato columna y también almacena metadatos.

Como utiliza formato columnar los valores del mismo tipo se almacenan juntos, permitiendo una mejor optimización que cuando almacenamos los datos en filas.

**2.6 comparar el tamaño de los ficheros de lo datos de las tablas padron_txt (txt), padron_txt_2 (txt sin espacios), padron_parquet y padron_parquet_2 (alojados hdfs cuya ruta se puede obtener de la propiedad location de cada tabla por ejemplo haciendo "show create table").**

padron_txt: 21.584 MB
padron_txt2: 12.131 MB
padron_parquet: 858.72KB
padron_parquet_2: 856.68KB

## 3. Juguemos con Impala

**3.1 ¿Qué es Impala?**

Un motor de SQL de alto rendimiento pensado para operar sobre grandes volúmenes de datos.

**3.2 ¿En qué se diferencia de Hive?**

Ejecuta las queries directamente sobre el cluster en lugar de utilizar MapReduce haciendolo 5 veces más eficiente. Está optimizado para hacer queries pero hay algunas funcionalidade que Impala no tiene y Hive sí.

**3.3 Comando INVALIDATE METADATA, ¿en qué consiste?**

Marca los metadatos de la tabla como 'stale' (¿caducados?). La próxima vez que Impala ejecute una query sobre esa tabla se recargan los metadatos antes de realizar el query. (Es muy caro, utilizar REFRESH en lugar de INVALIDATE METADATA cuando sea posible)

**3.4 Hacer invalidate metadata en impala de la base de datos datos_padron**

    INVALIDATE METADATA;

Si no recibe ningún argumento se aplica a toda la DB actual. 

**3.5 Calcular el total de EspanolesHombres, espanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO**

    select desc_distrito,desc_barrio,sum(espanoleshombres),
    sum(espanolesmujeres),sum(extranjeroshombres),sum(extranjerosmujeres) 
    from padron_parquet_2
    group by desc_distrito,desc_barrio;

(1.26 secs en padron_parquet_2)

**3.6 Llevar a cabo las consultas en Hive en las tablas padron_txt_2 y padron_parquet_2. ¿Alguna conclusión?**

¿Las consultas? La única consulta del ejercicio anterior?

padron_parquet_2: 31.29s
padron_txt_2: 30.73s

No aparenta haber ninguna diferencia real.

**3.7 Llevar a cabo la misma consulta sobre las mismas tablas en impala. ¿Alguna conclusión?**

parquet: 1.26 secs (consulta hecha antes)
txt: 7.1 s

Parquet aparenta darnos un rendimiento bastante mayor en Impala.

**3.8 ¿Se percibe alguna diferencia de rendimiento entre Hive y Impala?**

Comparado con Impala, Hive es más lento que el caballo del malo aunque no hay que olvidar que Hive tiene funciones que no tiene Impala y es más robusto.

## 4. Sobre tablas particionadas

**4.1 Crear tabla (Hive) padron_particionado particionada por campos DESC_DISTRITO y DESC_BARRIO cuyos datos estén en formato parquet.**

    CREATE TABLE datos_padron.padron_particionado(COD_DISTRITO INT, COD_DIST_BARRIO INT, 
                                    COD_BARRIO INT, COD_DIST_SECCION INT,
                                    COD_SECCION INT, COD_EDAD_INT INT,
                                    EspanolesHombres INT, EspanolesMujeres INT,
                                    ExtranjerosHombres INT, ExtranjerosMujeres INT)
    PARTITIONED BY(DESC_DISTRITO STRING, DESC_BARRIO STRING)
    STORED AS PARQUET;

**4.2 Insertar datos (en cada partición) dinámicamente (con Hive) en la tabla recién creada a partir de un select de la tabla padron_parquet_2.**

Le damos más memoria porsi acaso a mapreduce.

    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=non-strict;
    SET hive.exec.max.dynamic.partitions = 10000;
    SET hive.exec.max.dynamic.partitions.pernode = 1000;

    SET mapreduce.map.memory.mb = 2048;
    SET mapreduce.reduce.memory.mb = 2048;
    SET mapreduce.map.java.opts=-Xmx1800m;


    FROM datos_padron.padron_parquet_2
    INSERT OVERWRITE TABLE datos_padron.padron_particionado
    PARTITION(DESC_DISTRITO, DESC_BARRIO)
    SELECT COD_DISTRITO, COD_DIST_BARRIO, COD_BARRIO, COD_DIST_SECCION, COD_SECCION, COD_EDAD_INT, 
        EspanolesHombres,EspanolesMujeres, ExtranjerosHombres,ExtranjerosMujeres, DESC_DISTRITO, DESC_BARRIO;



**4.3 Hacer invalidate metadata en Impala de la base de datos padron_particionado**

    INVALIDATE METADATA;

**4.4 Calcular el total de EspanolesHombres, EspanolesMujeres, ExtranjerosHombres, ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO para los distritos CENTRO,LATINA,CHAMARTIN,TETUAN,VICALVARO Y BARAJAS**

    select desc_distrito,desc_barrio,sum(espanoleshombres),
    sum(espanolesmujeres),sum(extranjeroshombres),sum(extranjerosmujeres) 
    from padron_parquet_2
    where desc_distrito LIKE "CENTRO" || desc_barrio Like "LATINA" || desc_barrio like "CHAMARTIN" || desc_barrio like "TETUAN
    group by desc_distrito,desc_barrio;


**4.5 Llevar a cabo la consulta en hive en las tablas padron_parquet y padron_particionado. ¿Alguna conclusión?**

particionado: 40.15

parquet: 38.16

No aparenta haber una diferencia real de rendimiento.

**4.6 Llevar a cabo la consulta en Impala en las tablas padron_parquet y padron_particionado. ¿Alguna conclusión?**

Particionado: 0.78s

parquet: 0.82s

Parece no haber una diferencia de rendimiento.

**Hacer consultas de agregación (Max,Min,Avg,Count) tal cual el ejemplo anterior con las 3 tablas (padron_txt_2, padron_parquet_2 y padron_particionado) y comparar rendimientos tanto en Hive como en Impala y sacar conclusiones**

Edito un poco el anterior query para que tenga mas consultas de agregación

    select desc_distrito,desc_barrio,avg(espanoleshombres),
    sum(espanolesmujeres),min(extranjeroshombres),max(extranjerosmujeres) 
    from padron_particionado
    where desc_distrito like "CENTRO" || desc_distrito like "LATINA" || desc_distrito like "CHAMARTIN"
    || desc_distrito LIKE "TETUAN" || desc_distrito LIKE "VICALVARO" || desc_distrito LIKE "BARAJAS"
    group by desc_distrito,desc_barrio;

Impala:
    Particionado: 1.81s
    Parquet: 1.78
    txt: 1.91s

Hive:
    Particionado: 39.21s
    Parquet: 39.29s
    txt:39.52s

No aparenta haber ninguna diferencia de rendimiento entre los distintos tipos de tabla, pero si hay una gran diferencia de rendimiento entre Hive y Impala.


## 5. Trabajando con tablas en HDFS

**5.1 Crear un documento de texto en el almacenamiento local que contenga una secuencia de números distribuidos en filas y separados en columnas, llámalo datos1 y que sea por ejemplo: 1,2,3 \ 4,5,6 \ 3,8,9**

Voy a hacerlo con el terminal pero se puede hacer gráficamente con Hue.

    echo "1,2,3" >> datos1 
    echo "4,5,6" >> datos1 
    echo "7,8,9" >> datos1

    hadoop fs -put datos1 /user/cloudera/datos1

    // chequear que todo está bien
    hadoop fs -cat /user/cloudera/datos1

**5.2 Crear un segundo documento (datos2) con otros números pero la misma estructura**

    echo "11,12,13" >> datos2 
    echo "14,15,16" >> datos2
    echo "17,18,19" >> datos2

    hadoop fs -put datos2 /user/cloudera/datos2

    // chequear que todo está bien
    hadoop fs -cat /user/cloudera/datos2

**5.3 Crear un directorio en HDFS con un nombre a placer, por ejemplo, /test. Si estás en una máquina de cloudera tienes que asegurarte de que el servicio HDFS está activo ya que no puede iniciarse al encender la máquina (puedes hacerlo desde el Cloudera Manager). A su vez, en las máquinas Cloudera es posible (dependiendo si usamos Hive desde la consola o desde Hue) que no tengamos permisos para crear directorios HDFS salvo en el directorio /user/cloudera**

(No ha hecho falta activar el servicio HDFS en está VM. Además me ha dejado crear un directorio en / pero voy a crear esta carpeta en la ruta correcta.)

    hadoop fs -mkdir /user/cloudera/test1

**5.4 Mueve tu fichero datos1 al directorio que has creado en HDFS con un comando desde consola**

    hadoop fs -mv /user/cloudera/datos1 /user/cloudera/test1/


**5.5 Desde Hive, crea una nueva database por ejemplo con el nombre numeros. Crea una tabla que no sea externa y sin argumento location con tres columnas numéricas, campos separados por coma y delimitada por filas. La llamaremos por ejemplo numeros_tbl.**

    CREATE TABLE numeros_tbl (
        campo1 int,
        campo2 int,
        campo3 int
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES("separatorChar" = ",");

**5.6 Carga los datos de nuestro fichero de texto datos1 almacenado en HDFS en la tabla de Hive. Consulta la localización donde estaban anteriormente los datos almacenados. ¿Siguen estando ahí? ¿Dónde están? Borra la tabla, ¿qué ocurre con los datos almacenados en HDFS?**

    LOAD DATA INPATH '/user/cloudera/test1/datos1' into table numeros_tbl;

El archivo datsos1 ha desaparecido de /user/cloudera/test1. Ahora está en /user/hive/warehouse/numeros.db/numeros_tbl/datos1.

    drop table numeros_tbl;

El archivo datos1 ha desaparecido de la warehouse de hive. Tampoco vuelve a estar en /user/cloudera/test1/datos1. Si nuestro Hive tiene una papelera de reciclaje se envía ahí pero la carpeta .trash no está en nuestro HDFS así que debe haber sido borrado.

**5.7 Vuelve a mover el fichero de texto datos1 desde el almacenamiento local al directorio anterior en HDFS.**

hadoop fs -put datos1 /user/cloudera/test1/datos1

**5.8 Desde Hive, crea una tabla externa sin el argumento location. Y carga datos1 (desde HDFS) en ella. ¿A dónde han ido los datos en HDFS? Borra la tabla ¿Qué ocurre con los datos en hdfs?**

    create external table numeros_tbl(
        campo1 int,
        campo2 int,
        campo3 int
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES("separatorChar" = ",");

    LOAD DATA INPATH '/user/cloudera/test1/datos1' into table numeros_tbl;

Nuevamente el archivo ha desaparecido de test1 y ha aparecido en la hive warehouse en la misma ruta.

    drop table numeros_tbl;

Sin embargo al borrar la tabla eh archivo datos1 no se borra del hive warehouse.


**5.9 Borra el fichero datos1 del directorio en el que estén. Vuelve a insertarlos en el directorio que creamos inicialmente (/test). Vuelve a crear la tabla numeros desde hive pero ahora de manera externa y con un argumento location que haga referencia al directorio donde los hayas situado en HDFS (/test). No cargues los datos de ninguna manera explícita. Haz una consulta sobre la tabla que acabamos de crear que muestre todos los registros. ¿Tiene algún contenido?**

borro a mano desde hue y vuelvo a usar el mismo comando put para cargarlo en el hdfs. 

    create external table numeros_tbl(
        campo1 int,
        campo2 int,
        campo3 int
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES("separatorChar" = ",")
    LOCATION '/user/cloudera/test1';

La salida me muestra los datos en dato1 sin necesidad de cargarlo explicitamente.

**5.10 Inserta el fichero de datos creado al principio, "datos2" en el mismo directorio de HDFS que "datos1". Vuelve a hacer la consulta anterior sobre la misma tabla. ¿Qué salida muestra?**

    hadoop fs -put datos2 /user/cloudera/test1/datos2

Las filas en datos2 se han añadido a la salida que devuelve select * from numeros_tbl;

**5.11 Extrae conclusiones de todos estos anteriores apartados**

Trabajando con datos en el HDFS, si la tabla es interna los datos se desplazarán al warehouse de hive. Cuando la tabla se elimina los datos se pierden o van a la papelera.
Si la tabla es externa carga los ficheros que hay en la location especificada sin necesidad de especificarlo. Además los datos no se pierden al borrar la tabla. Pero si no tiene location crea un directorio en el warehouse de hive para utilizarla como location. Los datos siguen sin perderse en cualquier caso.