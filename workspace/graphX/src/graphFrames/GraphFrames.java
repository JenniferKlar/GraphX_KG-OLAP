package graphFrames;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import scala.reflect.ClassTag;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;

import java.util.List;


import org.apache.jena.sparql.core.Quad;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.*;

	public class GraphFrames {
		//Turn nodes into String depending on their type
		public static String nodeToString(Node node) {
			String nodeString = "";
			if (node.isLiteral()) { nodeString = node.getLiteralValue().toString();}
	    	if (node.isBlank()) { nodeString = node.getBlankNodeId().toString();}
	    	if (node.isURI()) { nodeString = node.getURI().toString();}
	    	return nodeString;
		}
		
		public static void main(String[] args) {
			SparkConf sparkConf = new SparkConf().setAppName("NQuadReader")
		            .setMaster("local[*]").set("spark.executor.memory","2g").set("spark.executor.cores", "1").set("spark.dynamicAllocation.enabled", "true");
		    SparkContext jsc = new SparkContext(sparkConf);
		    jsc.setLogLevel("ERROR");
			ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		    String path = "C:\\Users\\jenniffer\\Desktop\\Masterarbeit\\output_short.nq";
		    SparkSession spark = new SparkSession(jsc);
		    
		    
		    Dataset<String> dataframe = spark.read().text(path).as(Encoders.STRING());
		    
		    List<StructField> listOfStructField = new ArrayList<StructField>();
		    listOfStructField.add(DataTypes.createStructField("line", DataTypes.StringType, true));
		    
		    StructType structType = DataTypes.createStructType(listOfStructField);
		   
		    Dataset<Row> dataframeRowEncoder = dataframe.map(new MapFunction<String, Row>() {
		    		private static final long serialVersionUID = 445454;
		    		public Row call(String arg0) throws Exception {
		    		// TODO Auto-generated method stub
		    		return RowFactory.create(arg0.split(",")[0], "1,2,3,4");
		    		}
		    		}, RowEncoder.apply(structType));
		    NodeFactory nf = new NodeFactory();
		    Dataset<Row> splitDataSet=dataframeRowEncoder
		    	.withColumn("src",split(col("line"), " ").getItem(0))
		    	.withColumn("predicate", split(col("line"), " ").getItem(1))
		    	.withColumn("dst", split(col("line"), " ").getItem(2))
		    	.withColumn("graph", split(col("line"), " ").getItem(3))
		    	.withColumn("point", split(col("line"), " ").getItem(4));
		    
		    
		    	splitDataSet = splitDataSet.drop(col("line")).drop(col("point"));
		    	//splitDataSet.show();
		
		   new GraphFrame();
		GraphFrame gf = GraphFrame.fromEdges(splitDataSet);
		   spark.stop();
		}

		public static String nodeString(Node node) {
			String nodeString = "";
			if (node.isLiteral()) { nodeString = node.getLiteralValue().toString();}
	    	if (node.isBlank()) { nodeString = node.getBlankNodeId().toString();}
	    	if (node.isURI()) { nodeString = node.getURI().toString();}
	    	return nodeString;
		}
}
