package graphX;

import org.apache.spark.SparkConf;

import scala.reflect.ClassTag;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.sparql.core.Quad;
import org.apache.jena.graph.Node;
import org.apache.spark.storage.StorageLevel;
	
	public class NQuadReader {
		
		public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("NQuadReader")
		            .setMaster("local[*]").set("spark.executor.memory","2g").set("spark.executor.cores", "1").set("spark.dynamicAllocation.enabled", "true").set("spark.serializer","org.apache.spark.serializer.JavaSerializer");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		ClassTag<Object> objectTag = scala.reflect.ClassTag$.MODULE$.apply(Object.class);
		ClassTag<Relation> relationTag = scala.reflect.ClassTag$.MODULE$.apply(Relation.class);

		String path = "C:\\Users\\jenniffer\\Dropbox\\Masterarbeit";
		
		JavaRDD<Quad> javaRDD = getJavaRDD(path, jsc);
		
		List<Quad> quads = javaRDD.collect();
		List<Edge<Relation>> quadEdges = new ArrayList<>();
	    List<Object> verticesList = new ArrayList<>();
	    long count = javaRDD.count();
	
	    for(int i = 0; i<count; i++) {
	    	String targetDataType = "Resource";
	    	//Subject
	    	Node subject = quads.get(i).getSubject();
		    Resource subjectRes =  new Resource();
		    Resource predicate = new Resource();
	 	    Resource context =  new Resource();
		    
		    subjectRes.setValue(subject.toString());
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
  
	    //add IDs to Vertices from their Index
	    JavaRDD<Tuple2<Object, Object>> test2 = jsc.parallelize(verticesList).zipWithIndex().map(x -> new Tuple2<Object, Object>(x._2, x._1));
	    
	    //create RDDs from lists
	    JavaRDD<Edge<Relation>> quadEdgeRDD = jsc.parallelize(quadEdges);
	   
        Graph<Object, Relation> quadGraph = Graph.apply(
        		test2.rdd(),quadEdgeRDD.rdd(), "",StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),objectTag,relationTag);       

        sliceDice(quadGraph, jsc, objectTag, relationTag, "Level_Importance_All", "Level_Aircraft_All", "Level_Location_All", "Level_Date_All").triplets().toJavaRDD().foreach(x -> System.out.println(x.dstAttr()));;
        jsc.close();	
		
		}
		
		
		//get all Data as a Graph that are associated with modules that correspond to cells that have required dimension levels
		private static Graph<Object, Relation> sliceDice(Graph<Object, Relation> graph, JavaSparkContext jsc,
				ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String importanceLevel, String aircraftLevel, String locationLevel , String dateLevel) {						
	        List<Object> cellIds = getCells(graph, jsc, objectTag, relationTag, importanceLevel, aircraftLevel, locationLevel, dateLevel);
	        List<String> mods = getMods(graph, cellIds);
			List<Edge<Relation>> rels = new ArrayList<>();
			for(int i = 0; i < mods.size(); i++) {
				int j = i;
				rels.addAll(graph
				.edges()
				.toJavaRDD()
				.filter(e -> ((Resource) e.attr().getContext()).getValue().contains(mods.get(j))).collect());	
			}
			
			List<Tuple2<Object, Object>> moduleVertices = new ArrayList<>();
			List<Object> vertices = new ArrayList<Object>();
			//all sources and destinations of all mods
			rels.forEach(x -> vertices.add(x.dstId()));
			rels.forEach(x -> vertices.add(x.srcId()));
			//filter vertices
			graph.vertices().toJavaRDD().collect().forEach(v -> {
				if(vertices.contains(v._1)) {
					moduleVertices.add(v);
				}
			});
			jsc.parallelize(moduleVertices);
			jsc.parallelize(rels);
	        //create graph obejct from filtered edges and vertices
			Graph<Object, Relation> resultGraph = Graph.apply(
	        		jsc.parallelize(moduleVertices).rdd(),jsc.parallelize(rels).rdd(), "",StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),objectTag,relationTag);       

			return resultGraph;
		}
		
		//get all Mods that belong to the cells that have correct dimensions
		private static List<String> getMods(Graph<Object, Relation> quadGraph, List<Object> cellIds) {
			List<String> result = new ArrayList<>();
			//get mods
			List<EdgeTriplet<Object, Relation>> modules = quadGraph.triplets().toJavaRDD()
			.filter(x -> ((Resource) x.attr().getRelationship()).getValue().contains("hasAssertedModule"))
			.collect();
			for(int i = 0; i < modules.size(); i++) {
				for(int j = 0; j < cellIds.size(); j++) {
					//id cellId contains mod --> add it to result
					if(modules.get(i).srcId() == (long) cellIds.get(j)) {
						result.add(modules.get(i).dstAttr().toString());
					}
				}
			}
			return result;
		}

        //get all cells (their ID) that satisfy dimensionlevels
		private static List<Object> getCells(Graph<Object, Relation> quadGraph, JavaSparkContext jsc, ClassTag<Object> objectTag,
				ClassTag<Relation> relationTag, String importanceLevel, String aircraftLevel, String locationLevel , String dateLevel) {
			//get cells for each dimension individually
			List<Object> importance = getCellsWithCertainLevel(quadGraph, objectTag, relationTag, "hasImportance", importanceLevel);
	        List<Object> aircraft = getCellsWithCertainLevel(quadGraph, objectTag, relationTag, "hasAircraft", aircraftLevel);
	        List<Object> location = getCellsWithCertainLevel(quadGraph, objectTag, relationTag, "hasLocation", locationLevel);
	        List<Object> date = getCellsWithCertainLevel(quadGraph, objectTag, relationTag, "hasDate", dateLevel);
			List<Object> result = new ArrayList<>();
			//compare lists --> if cell is in each --> return it (satisfies all four dimensions)
	        for(int i=0; i < importance.size(); i++) {
	        	Object o = importance.get(i);
	        	if(aircraft.contains(o)
	        			&& location.contains(o)
	        			&& date.contains(o)){
							result.add(o);
	        			}
	        }
	        return result;  
		}

		
		//gets Cells that have a certain level of a specific dimension
		private static List<Object> getCellsWithCertainLevel(Graph<Object, Relation> graph, ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String dimension, String level){
	        List<Long> correctLevelVertices = graph.triplets().toJavaRDD()
	        .filter(triplet -> ((Resource) triplet.attr().getRelationship()).getValue().contains("atLevel") 
	        		&& triplet.dstAttr().toString().contains(level)).map(triplet -> triplet.srcId()).collect();
	        
	        List<EdgeTriplet<Object, Relation>> g = graph.triplets().toJavaRDD().filter(triplet -> ((Resource) triplet.attr().getRelationship()).getValue().contains(dimension)).collect();
	        
    		List<Object> result = new ArrayList<>();
	        for(int i = 0; i < g.size(); i++) {
	        	if(correctLevelVertices.contains(g.get(i).dstId())) {
					result.add(g.get(i).srcId());
	        	}
	        }
	        return result;
		}
		
		//get JavaRDD from n-quad file
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
		
		
		
		private static Graph<Object, Relation> getRollUpGraph(Graph<Object, Relation> graph, JavaSparkContext jsc, ClassTag<Object> objectTag, ClassTag<Relation> relationTag){
			List<Edge<Relation>> rels = 
					graph
					.edges()
			        .toJavaRDD()
			        .filter(e -> ((Resource) e.attr().getRelationship()).getValue().contains("directlyRollsUpTo")).collect();			
			
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
			        .filter(e -> ((Resource) e.attr().getRelationship()).getValue().contains("hasImportance")
			        		|| ((Resource) e.attr().getRelationship()).getValue().contains("hasAircraft")
			        		|| ((Resource) e.attr().getRelationship()).getValue().contains("hasLocation")
			        		|| ((Resource) e.attr().getRelationship()).getValue().contains("hasDate")).collect();			
			
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
	        Graph<Object, Relation> dimGraph = Graph.apply(
	        		jsc.parallelize(dimVertices).rdd(),jsc.parallelize(dims).rdd(), "",StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),objectTag,relationTag);       

			return dimGraph;
		}
		
		private static Graph<Object, Relation> getGlobalGraph(Graph<Object, Relation> graph, JavaSparkContext jsc, ClassTag<Object> objectTag, ClassTag<Relation> relationTag){
			List<Edge<Relation>> rels = 
					graph
					.edges()
			        .toJavaRDD()
			        .filter(e -> ((Resource) e.attr().getContext()).getValue().contains("global")).collect();			
			
			List<Tuple2<Object, Object>> globalVertices = new ArrayList<>();
			List<Object> vertices = new ArrayList<Object>();
			//all sources and destinations of all global relationships
			rels.forEach(x -> vertices.add(x.dstId()));
			rels.forEach(x -> vertices.add(x.srcId()));
			//filter vertices
			graph.vertices().toJavaRDD().collect().forEach(v -> {
				if(vertices.contains(v._1)) {
					globalVertices.add(v);
				}
			});
			jsc.parallelize(globalVertices);
			jsc.parallelize(rels);
	        Graph<Object, Relation> globalGraph = Graph.apply(
	        		jsc.parallelize(globalVertices).rdd(),jsc.parallelize(rels).rdd(), "",StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),objectTag,relationTag);       

			return globalGraph;
		}
		
		
}
