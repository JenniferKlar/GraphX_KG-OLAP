package graphX;

import java.io.ByteArrayInputStream;
import java.util.UUID;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class GraphGenerator {
	
	public static void main(String[] args) {
		ClassTag<Object> objectTag = scala.reflect.ClassTag$.MODULE$.apply(Object.class);
		ClassTag<Relation> relationTag = scala.reflect.ClassTag$.MODULE$.apply(Relation.class);
		//String path = "C:\\Users\\jenniffer\\Dropbox\\Masterarbeit\\";
		String path = "";
		SparkConf sparkConf = new SparkConf().setAppName("NQuadReader").setMaster("local[*]")
				.set("spark.executor.memory", "2g").set("spark.executor.cores", "1")
				.set("spark.dynamicAllocation.enabled", "true")
				.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
				//.set("spark.eventLog.enabled", "true")
				;
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		jsc.setLogLevel("ERROR");
		
		String fileName = "bd108186-5adb-4f00-b5fe-b24a8993560b.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);

		graph.edges().saveAsObjectFile(path + "edges_noType" + fileName);
		graph.vertices().saveAsObjectFile(path + "vertices_noType" + fileName);
		
		jsc.close();
		
	}
	
	public GraphGenerator() {
	}

	public static Graph<Object, Relation> generateGraph(JavaSparkContext jsc, ClassTag<Object> vertexTag,
			ClassTag<Relation> edgeTag, String path, String fileName) {

		JavaRDD<Quad> javaRDD = getJavaRDD(path, fileName, jsc);

		JavaRDD<Tuple2<Object, Vertex>> verticesWithoutType = 
				javaRDD.filter(x -> !x.getPredicate().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
				.map(x -> (Object) new Resource(x.getSubject().toString()))
				.union(
						javaRDD.filter(x -> !x.getPredicate().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
						.map(x -> 
						{if(x.getObject().isLiteral()){return new Literal(x.getObject().getLiteralValue().toString());}
						else {return new Resource(x.getObject().toString());}
						}))
				.map(x -> new Tuple2<Object, Vertex>(UUID.nameUUIDFromBytes(x.toString().getBytes()).getMostSignificantBits(), (Vertex) x));
		
		//non-type relations --> create edges
		JavaRDD<Edge<Relation>> edges = 
				javaRDD.filter(x -> !x.getPredicate().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
				.map(x -> {
					if(x.getObject().isLiteral()) {
						return new Edge<Relation>(
								UUID.nameUUIDFromBytes(x.getSubject().toString().getBytes()).getMostSignificantBits(),
								UUID.nameUUIDFromBytes(x.getObject().getLiteralValue().toString().getBytes())
										.getMostSignificantBits(),
								new Relation(new Resource(x.getPredicate().toString()), new Resource(x.getGraph().toString()),
										x.getObject().getLiteralDatatype().getJavaClass().getSimpleName().toString())); }
					else {
						return new Edge<Relation>(
										UUID.nameUUIDFromBytes(x.getSubject().toString().getBytes()).getMostSignificantBits(),
										UUID.nameUUIDFromBytes(new Resource(x.getObject().toString()).toString().getBytes())
												.getMostSignificantBits(),
										new Relation(new Resource(x.getPredicate().toString()), new Resource(x.getGraph().toString()),
												"Resource")); }		
					});

	//type relations --> change vertices
	JavaRDD<Tuple2<Object, Vertex>> verticesWithType = 
			javaRDD.filter(x -> x.getPredicate().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
			.map(x -> (Object) new Resource(x.getSubject().toString(),x.getObject().toString()))
			.map(x -> new Tuple2<Object, Vertex>(UUID.nameUUIDFromBytes(x.toString().getBytes()).getMostSignificantBits(), (Resource) x));
	
	JavaPairRDD<Object, Tuple2<Object, Vertex>> subtracted = 
			verticesWithoutType.keyBy(x -> x._1)
			.subtractByKey(verticesWithType.keyBy(x -> x._1).distinct());
	JavaPairRDD<Object, Tuple2<Object, Vertex>> vertices = subtracted.union(verticesWithType.keyBy(x -> x._1));
	
	JavaRDD<Tuple2<Object, Object>> res = vertices.map(x -> new Tuple2<Object,Object>(x._1(), (Vertex) x._2()._2));

	Graph<Object, Relation> quadGraph = Graph.apply(res.rdd(), edges.rdd(), "",
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), vertexTag, edgeTag);
		return quadGraph;
		
	}

	// get JavaRDD from n-quad file
	private static JavaRDD<Quad> getJavaRDD(String path, String fileName, JavaSparkContext jsc) {

		JavaRDD<Quad> javaRDD = jsc.textFile(path + fileName).filter(line -> !line.startsWith("#"))
				.filter(line -> !line.isEmpty() || line.length() != 0).map(line -> RDFDataMgr
						.createIteratorQuads(new ByteArrayInputStream(line.getBytes()), Lang.NQUADS, null).next());
		
		javaRDD.persist(StorageLevel.MEMORY_ONLY());
		return javaRDD;
	}

}