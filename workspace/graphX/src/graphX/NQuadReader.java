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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.sparql.core.Quad;
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
		
	    Set<Object> set = new LinkedHashSet<>();
	    set.addAll(javaRDD.map(x -> new Resource(x.getSubject().toString())).collect());
	    set.addAll(javaRDD.filter(x -> x.getObject().isLiteral()).map(x -> x.getObject().toString()).collect());
	    set.addAll(javaRDD.filter(x -> !x.getObject().isLiteral()).map(x -> new Resource(x.getObject().toString())).collect());

	    List<Object> verticesList = new ArrayList<>(set);
	    
	    //all Objects that are Literals
	    JavaRDD<Edge<Relation>> literalEdges = javaRDD.filter(x -> x.getObject().isLiteral())
	    .map(x -> new Edge<Relation>(
	    		verticesList.indexOf(new Resource(x.getSubject().toString())),
	    		verticesList.indexOf(x.getObject().toString()), 
    			new Relation(new Resource(x.getPredicate().toString()) ,new Resource(x.getGraph().toString()), x.getObject().getLiteralDatatype().getJavaClass().getSimpleName().toString())));
	    
	    //all Objects that are Resources
	    JavaRDD<Edge<Relation>> resourceEdges = javaRDD.filter(x -> !x.getObject().isLiteral())
	    .map(x -> new Edge<Relation>(
	    		verticesList.indexOf(new Resource(x.getSubject().toString())),
	    		verticesList.indexOf(new Resource(x.getObject().toString())), 
    			new Relation(new Resource(x.getPredicate().toString()), new Resource(x.getGraph().toString()), "Resource")));
	    
	    JavaRDD<Edge<Relation>> quadEdgeRDD = literalEdges.union(resourceEdges);
	    
	    
	    //add IDs to Vertices from their Index
	    JavaRDD<Tuple2<Object, Object>> vertices = jsc.parallelize(verticesList).zipWithIndex().map(x -> new Tuple2<Object, Object>(x._2, x._1));
	    
	    //create RDDs from lists
	    //JavaRDD<Edge<Relation>> quadEdgeRDD = jsc.parallelize(quadEdges);
	   
        Graph<Object, Relation> quadGraph = Graph.apply(
        		vertices.rdd(),quadEdgeRDD.rdd(), "",StorageLevel.MEMORY_ONLY(),StorageLevel.MEMORY_ONLY(),objectTag,relationTag);       

        sliceDice(quadGraph, jsc, objectTag, relationTag, "Level_Importance_Package", "Level_Aircraft_All", "Level_Location_All", "Level_Date_Year").triplets().toJavaRDD().foreach(x -> System.out.println(x.dstAttr()));;

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
	        ArrayList<String> lowerLevels = new ArrayList<>();
	        lowerLevels.add(level);
	        if(getLowerlevels(level) != null) {
	        	lowerLevels.addAll(getLowerlevels(level));
	        }
	        
			ArrayList<Long> aList = new ArrayList<>();
			for (int i = 0; i < lowerLevels.size(); i++) {
				int j = i;
				aList.addAll(graph.triplets().toJavaRDD()
				        .filter(triplet -> ((Resource) triplet.attr().getRelationship()).getValue().contains("atLevel")
				        		&& triplet.dstAttr().toString().contains(lowerLevels.get(j)))
		        		.map(triplet -> triplet.srcId()).collect());
			}
			System.out.println(aList.size());
			
	        List<EdgeTriplet<Object, Relation>> g = graph.triplets().toJavaRDD().filter(triplet -> ((Resource) triplet.attr().getRelationship()).getValue().contains(dimension)).collect();
	        
    		List<Object> result = new ArrayList<>();
	        for(int i = 0; i < g.size(); i++) {
	        	if(aList.contains(g.get(i).dstId())) {
					result.add(g.get(i).srcId());
	        	}
	        }
	        return result;
		}
		
		//returns a list of all lower level of certain given level
		private static ArrayList<String> getLowerlevels(String level) {
			HashMap<String, List<String>> lowerLevelMap = new HashMap();
			ArrayList<String> aircraftAll = new ArrayList<>();
			 aircraftAll.add("Level_Aircraft_Model");
			 aircraftAll.add("Level_Aircraft_Type");
			lowerLevelMap.put("Level_Aircraft_All", aircraftAll);
			ArrayList<String> aircraftModel = new ArrayList<>();
			aircraftModel.add("Level_Aircraft_Type");
			lowerLevelMap.put("Level_Aircraft_Model", aircraftModel);
			
			ArrayList<String> locationAll = new ArrayList<>();
			locationAll.add("Level_Location_Region");
			locationAll.add("Level_Location_Segment");
			lowerLevelMap.put("Level_Location_All", locationAll);
			ArrayList<String> locationRegion = new ArrayList<>();
			locationRegion.add("Level_Location_Segment");
			lowerLevelMap.put("Level_Location_Region", locationRegion);
			
			ArrayList<String> importanceAll = new ArrayList<>();
			importanceAll.add("Level_Importance_Package");
			importanceAll.add("Level_Importance_Importance");
			lowerLevelMap.put("Level_Importance_All", importanceAll);
			ArrayList<String> importancePackage = new ArrayList<>();
			importancePackage.add("Level_Importance_Importance");
			lowerLevelMap.put("Level_Importance_Package", importancePackage);

			ArrayList<String> dateAll = new ArrayList<>();
			dateAll.add("Level_Date_Year");
			dateAll.add("Level_Date_Month");
			dateAll.add("Level_Date_Day");
			lowerLevelMap.put("Level_Date_All", dateAll);
			ArrayList<String> dateYear = new ArrayList<>();
			dateYear.add("Level_Date_Month");
			dateYear.add("Level_Date_Day");
			lowerLevelMap.put("Level_Date_Year", dateYear);
			ArrayList<String> dateMonth = new ArrayList<>();
			dateMonth.add("Level_Date_Day");
			lowerLevelMap.put("Level_Date_Month", dateMonth);
			if (lowerLevelMap.get(level) != null){
				return (ArrayList<String>) lowerLevelMap.get(level);
			}
			return null;
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

}
