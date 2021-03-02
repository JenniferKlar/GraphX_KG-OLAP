package graphX;

import org.apache.spark.SparkConf;

import scala.reflect.ClassTag;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
	
	public class NQuadReader {
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
	            .setMaster("local[*]").set("spark.executor.memory","2g").set("spark.executor.cores", "1").set("spark.dynamicAllocation.enabled", "true");;
	    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
	    String path = "C:\\Users\\jenniffer\\Desktop\\Masterarbeit\\output.nq";
	    
	    JavaRDD<Quad> x = jsc.textFile(path)
	    	.filter(line -> !line.startsWith("#"))
	    	.filter(line -> !line.isEmpty() || line.length()==0)
	    	.map(line -> RDFDataMgr
	    			.createIteratorQuads(new ByteArrayInputStream(line.getBytes()), Lang.NQUADS, null)
	    			.next());
	    x.persist(StorageLevel.MEMORY_AND_DISK());
	    
		List<Quad> quads = x.collect();
	    List<Edge<String>> edges = new ArrayList<>();
	    List<String> verticesList = new ArrayList<>();

	    long count = x.count();
	    String subject = null;
	    String object = null;
	    String predicate = null;
	    Edge<String> edge = null;
	    
	    for(int i = 0; i<count; i++) {
	    	subject = nodeToString(quads.get(i).getSubject());
	    	object = nodeToString(quads.get(i).getObject());
	    	predicate = nodeToString(quads.get(i).getPredicate());
	    	
	    	//so they are not duplicated
	    	if(!verticesList.contains(subject))
	    		verticesList.add(subject);
	    	if(!verticesList.contains(object))
	    		verticesList.add(object);
	    	
	    	edge = new Edge<String>(verticesList.indexOf(subject),verticesList.indexOf(object),predicate);
	    	edges.add(edge);
	    	//if(!verticesMap.containsKey(subject)) {
	    		//verticesMap.put(subject, id);
	    		//id++;
	    	//if(!verticesMap.containsKey(object)) {
	    		//verticesMap.put(object, id);
	    		//id++;
	    	}
	    
	    //adding ID to vertices
	    List<Tuple2<Object, String>> vertices = new ArrayList<>();
	    long l = 0;
	    for(int i = 0; i < verticesList.size(); i++) {
	    	l = i;
	    	vertices.add(new Tuple2<Object, String>(l, verticesList.get(i)));
	    }
	    
	    
	    JavaRDD<Tuple2<Object, String>> verticeRDD = jsc.parallelize(vertices);
	    JavaRDD<Edge<String>> edgeRDD = jsc.parallelize(edges);
    	
	    //Graph graph = Graph.fromEdges(edgeRDD.rdd(), "",StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);
        
        Graph<String, String> graph2 = Graph.apply(
        		verticeRDD.rdd(),edgeRDD.rdd(), "",StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),stringTag,stringTag);   
        
        GraphOps ops = new GraphOps(graph2, stringTag, stringTag);
        //System.out.print(ops.numEdges());
        //System.out.print(ops.numVertices());
        
        //Graph n = StronglyConnectedComponents.run(graph, 10, stringTag, stringTag);
        
        //graph.vertices().toJavaRDD().collect().forEach(System.out::println);
        
       //System.out.print(graph2.edges().count());
       //System.out.print(graph2.vertices().count());
       
       //EdgeDirection ed = new EdgeDirection("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
       //VertexRDD edgesCollect = ops.collectEdges(ed);
       //((Iterable<Quad>) edgesCollect).forEach(e -> System.out.println(e.toString()));
       //Object li = graph2.edges().collect();
       
       jsc.close();
		}
}
