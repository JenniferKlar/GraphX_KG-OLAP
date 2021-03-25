package graphX;

import org.apache.spark.SparkConf;

import scala.reflect.ClassTag;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;

import java.util.List;

import org.apache.jena.sparql.core.Quad;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
	
	public class NQuadReader {
		
		public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("NQuadReader")
		            .setMaster("local[*]").set("spark.executor.memory","2g").set("spark.executor.cores", "1").set("spark.dynamicAllocation.enabled", "true").set("spark.serializer","org.apache.spark.serializer.JavaSerializer");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		//ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		ClassTag<Object> objectTag = scala.reflect.ClassTag$.MODULE$.apply(Object.class);
		//ClassTag<Tuple2<Object, Object>> tupleTag = scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class);
		ClassTag<Relation> relationTag = scala.reflect.ClassTag$.MODULE$.apply(Relation.class);
			
		String path = "C:\\Users\\jenniffer\\Desktop\\Masterarbeit";
		
		JavaRDD<Quad> javaRDD = getJavaRDD(path, jsc);
		List<Quad> quads = javaRDD.collect();
		List<Edge<Relation>> quadEdges = new ArrayList<>();
	    List<Object> verticesList = new ArrayList<>();

	    long count = javaRDD.count();
	
	    for(int i = 0; i<count; i++) {
	    	String targetDataType = "Resource";
	    	//Subject
	    	//quads.get(i).getSubject().toString();
	    	Node subject = quads.get(i).getSubject();
		    Resource subjectRes =  new Resource();
	    	subjectRes.setValue(subject.toString());
	    	//Predicate & Context
	    	Resource predicate = new Resource();
	 	    Resource context =  new Resource();
	    	predicate.setValue(quads.get(i).getPredicate().toString());
	    	context.setValue(quads.get(i).getGraph().toString());
	    	
	    	//Object
	    	Node object = quads.get(i).getObject();
    		Resource objectRes = new Resource();
    		String objectLit = "";
    		Object objectToAdd = null;
	    	if(object.isLiteral()) {
	    		targetDataType = object.getLiteralDatatype().getJavaClass().getSimpleName().toString();
	    		objectLit = object.getLiteralValue().toString();
	    		objectToAdd = objectLit;
	    	} else {
	    		objectRes.setValue(object.toString());
	    		objectToAdd = objectRes;
	    	}
	    	
	    	if(!verticesList.contains(subjectRes) && subjectRes.getValue() != "")
	    		verticesList.add(subjectRes);
	    	if(!verticesList.contains(objectRes) && objectRes.getValue() != "")
	    		verticesList.add(objectRes);

	    	if(!verticesList.contains(objectLit) && objectLit != "")
	    		verticesList.add(objectLit);

	    	Edge<Relation> quadEdge = null;
	    	quadEdge = new Edge<Relation>(
	    			verticesList.indexOf(subjectRes),
	    			verticesList.indexOf(objectToAdd), 
	    			new Relation(predicate ,context, targetDataType));
	    	quadEdges.add(quadEdge);
	    	}

	    //adding ID to vertices
	    List<Tuple2<Object, Object>> vertices = new ArrayList<>();
	    verticesList.stream().forEach(x -> vertices.add(new Tuple2<Object, Object>((long) verticesList.indexOf(x), x)));
	    
	    
	    //create RDDs from lists
	    JavaRDD<Tuple2<Object, Object>> verticeRDD = jsc.parallelize(vertices);
	    JavaRDD<Edge<Relation>> quadEdgeRDD = jsc.parallelize(quadEdges);
	    
  
        Graph<Object, Relation> quadGraph = Graph.apply(
        		verticeRDD.rdd(),quadEdgeRDD.rdd(), "",StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),objectTag,relationTag);       


       
        //get all data of specific modules
        List<Edge<Relation>> modules = selectMods(quadGraph, "", "", "", "", jsc);
        //modules.forEach(x -> System.out.println(x.attr().getRelationship()));
        Graph<Object, Relation> graph = getDimensionGraph(quadGraph, jsc, objectTag, relationTag);
        graph.vertices().toJavaRDD().foreach(x -> System.out.println(x._2));
        jsc.close();	
		
		}

		
		private static JavaRDD<Quad> getJavaRDD(String path, JavaSparkContext jsc) {
		    
			JavaRDD<Quad> javaRDD = jsc.textFile(path+"\\output_short.nq")
		    	.filter(line -> !line.startsWith("#"))
		    	.filter(line -> !line.isEmpty() || line.length()!=0)
		    	.map(line -> RDFDataMgr
		    			.createIteratorQuads(new ByteArrayInputStream(line.getBytes()), Lang.NQUADS, null)
		    			.next());
		    javaRDD.persist(StorageLevel.MEMORY_AND_DISK());
			return javaRDD;
		}

		
		//returns modules and their data
		private static List<Edge<Relation>> selectMods(Graph<Object, Relation> graph, String levelAircraft, String levelLocation, String levelImportance, String levelDate, JavaSparkContext jsc){
			List<Tuple2<Object, Object>> allmodules = new ArrayList<>();
			List<Edge<Relation>> rels = 
					graph
					.edges()
			        .toJavaRDD()
			        .filter(e -> e.attr().getRelationship().getValue().contains("hasAssertedModule")).collect();			
			
			List<Object> sources = new ArrayList<Object>();
			rels.forEach(x -> sources.add(x.dstId()));
			graph.vertices().toJavaRDD().collect().forEach(v -> {
				if(sources.contains(v._1)) {
					allmodules.add(v);
				}
			});

	        List<Tuple2<Object, Object>> list = jsc.parallelize(allmodules).collect();
			List<Edge<Relation>> filteredEdges = new ArrayList<Edge<Relation>>();
	        for (int i = 0; i < list.size(); i++) {
	        	int j = i;
	        	filteredEdges.addAll(graph.edges().toJavaRDD().filter(x -> x.attr().getContext().getValue().equals(list.get(j)._2().toString())).collect());
	        }
			return filteredEdges;
		}
		
		private static Graph<Object, Relation> getRollUpGraph(Graph<Object, Relation> graph, JavaSparkContext jsc, ClassTag<Object> objectTag, ClassTag<Relation> relationTag){
			List<Edge<Relation>> rels = 
					graph
					.edges()
			        .toJavaRDD()
			        .filter(e -> e.attr().getRelationship().getValue().contains("directlyRollsUpTo")).collect();			
			
			List<Tuple2<Object, Object>> rollupVertices = new ArrayList<>();
			List<Object> vertices = new ArrayList<Object>();
			//all sources and destinations of all rollup relationships
			rels.forEach(x -> vertices.add(x.dstId()));
			rels.forEach(x -> vertices.add(x.srcId()));
			//filter vertices
			graph.vertices().toJavaRDD().collect().forEach(v -> {
				if(vertices.contains(v._1)) {
					rollupVertices.add(v);
				}
			});
			jsc.parallelize(rollupVertices);
			jsc.parallelize(rels);
	        Graph<Object, Relation> rollUpGraph = Graph.apply(
	        		jsc.parallelize(rollupVertices).rdd(),jsc.parallelize(rels).rdd(), "",StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),objectTag,relationTag);       

			return rollUpGraph;
		}
		
		private static Graph<Object, Relation> getDimensionGraph(Graph<Object, Relation> graph, JavaSparkContext jsc, ClassTag<Object> objectTag, ClassTag<Relation> relationTag){
			List<Edge<Relation>> dims = 
					graph
					.edges()
			        .toJavaRDD()
			        .filter(e -> e.attr().getRelationship().getValue().contains("hasImportance")
			        		|| e.attr().getRelationship().getValue().contains("hasAircraft")
			        		|| e.attr().getRelationship().getValue().contains("hasLocation")
			        		|| e.attr().getRelationship().getValue().contains("hasDate")).collect();			
			
			List<Tuple2<Object, Object>> dimVertices = new ArrayList<>();
			List<Object> vertices = new ArrayList<Object>();
			//all sources and destinations of all rollup relationships
			dims.forEach(x -> vertices.add(x.dstId()));
			dims.forEach(x -> vertices.add(x.srcId()));
			//filter vertices
			graph.vertices().toJavaRDD().collect().forEach(v -> {
				if(vertices.contains(v._1)) {
					dimVertices.add(v);
				}
			});
			jsc.parallelize(dimVertices);
			jsc.parallelize(dims);
	        Graph<Object, Relation> rollUpGraph = Graph.apply(
	        		jsc.parallelize(dimVertices).rdd(),jsc.parallelize(dims).rdd(), "",StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),objectTag,relationTag);       

			return rollUpGraph;
		}
}
