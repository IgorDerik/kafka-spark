import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Main {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Spark-Kafka-Integration")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "172.18.0.2:6667")
//                .option("kafka.bootstrap.servers", "local:0000")
                .option("subscribe", "test")
                .load();

        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        /*
        StructType mySchema = new StructType()
                .add("", DataTypes.IntegerType)
                .add("", DataTypes.StringType)
                .add("", DataTypes.IntegerType)
                .add("", DataTypes.DoubleType)
                .add("", DataTypes.IntegerType);

        spark
                .readStream()
                .schema(mySchema)
                .csv("src/main/resources/moviedata.csv")
                .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .option("topic", "test")
                .option("kafka.bootstrap.servers", "172.18.0.2:6667")
                .option("checkpointLocation", "src/main/resources")
                .start();
        */
    }

}
