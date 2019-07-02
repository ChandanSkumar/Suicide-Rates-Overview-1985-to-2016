import org.apache.spark.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.types.DataTypes.*;

public class SuicideAnalysis{

    public static void main(String args[]) throws AnalysisException, IOException {

        /*String inputFile = args[0]; // Should be some file on your system*/



        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        ConfigFile c = new ConfigFile(args[0]);

        String InputDataFile = c.getFileName("Input");

        SQLContext sqlContext = new SQLContext(sc);
        if (InputDataFile.equals("Error")) {
            System.out.println("Input file is not mentioned in Config file");
        }

        JavaRDD<String> RDD1 = sc.textFile(InputDataFile);
        JavaRDD<Row> rowRDD = RDD1.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                ArrayList<String> arr = new ArrayList<>();
                Pattern p = Pattern.compile("(\\\".*?\\\"|.*?),");
                Matcher m = p.matcher(s);
                m.reset();
                while (m.find()) {
                    arr.add(m.group(1));
                }
                Pattern q = Pattern.compile(".*,(.*)");
                m = q.matcher(s);
                m.reset();
                while (m.find()) {
                    arr.add(m.group(1));
                }
                return RowFactory.create(arr.get(0), arr.get(1), arr.get(2), arr.get(3), arr.get(4), arr.get(5), arr.get(6), arr.get(7), arr.get(8), arr.get(9), arr.get(10), arr.get(11));


            }
        });
        StructType schema = createStructType(new StructField[]{
                createStructField("Country", StringType, true),
                createStructField("Year", StringType, true),
                createStructField("sex", StringType, true),
                createStructField("Age", StringType, true),
                createStructField("Sucide_no", StringType, true),


                createStructField("Population", StringType, true),
                createStructField("Suciders", StringType, true),
                createStructField("Country_Year", StringType, true),
                createStructField("HDI_Year", StringType, true),
                createStructField("GDP_Year", StringType, true),


                createStructField("GDP_Per_Cap", StringType, true),
                createStructField("Generation", StringType, true),
        });
        Dataset<Row> df = sqlContext.createDataFrame(rowRDD, schema);

        df.createOrReplaceTempView("suicide");
        Dataset<Row> result = sqlContext.sql("select * from suicide");
        result.show();
        //result.coalesce(1).write().csv("E:\\output3");


        Dataset<Row> result1 = sqlContext.sql("select sex, SUM(Sucide_no) as total_suicide_count from suicide group By sex order by sex ");
        result1.coalesce(1).show();
        String output1 = c.getFileName("Output1");
        if (output1 != "Error")
            result1.coalesce(1).write().csv(output1);


        Dataset<Row> result2 = sqlContext.sql("select age, SUM(Sucide_no)as total_suicide_count from suicide group By age order by age ");
        result2.coalesce(1).show();
        output1 = c.getFileName("Output2");
        if (output1 != "Error")
            result2.coalesce(1).write().csv(output1);


        Dataset<Row> result3 = sqlContext.sql("select  country,(SUM(Sucide_no)/Population*100000) as rate_per_100000 from suicide  group by country,population order by rate_per_100000 desc limit 1 ");
        result3.coalesce(1).show();
        output1= c.getFileName("Output3");
        if (output1 != "Error")
            result3.coalesce(1).write().csv(output1);


        Dataset<Row> result4 = sqlContext.sql("select  country,SUM(Sucide_no)as total_suicide_count ,SUM(Population)as total_population_count  from suicide  group by country order by total_suicide_count desc limit 10 ");
        result4.coalesce(1).show();
        output1 = c.getFileName("Output4");
        if (output1 != "Error")
            result4.coalesce(1).write().csv(output1);


        Dataset<Row> result5 = sqlContext.sql("select  (SUM(Sucide_no)/SUM(Population)*100000) as suicides_per_100000 from suicide   ");
        result5.coalesce(1).show();

        output1 = c.getFileName("Output5");
        if (output1 != "Error")
            result5.coalesce(1).write().csv(output1);

        Dataset<Row> result6 = sqlContext.sql("select  country,SUM(Sucide_no) as total_suicide_count ,SUM(GDP_Per_Cap) as total_gdp from suicide  group by country order by total_suicide_count ");
        result6.coalesce(1).show();
        output1 = c.getFileName("Output6");
        if (output1 != "Error")
            result6.coalesce(1).write().csv(output1);
    }

}