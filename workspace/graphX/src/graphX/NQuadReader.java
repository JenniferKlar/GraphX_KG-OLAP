package graphX;
import java.util.UUID;

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD;
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD.*;
import io.fabric8.kubernetes.api.model.APIGroupListFluentImpl.GroupsNestedImpl;
import io.fabric8.kubernetes.api.model.apps.ControllerRevisionFluent.GroupDataNested;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.groupingExpressionSingle_return;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.spark.SparkConf;
import scala.reflect.ClassTag;
import scala.tools.nsc.backend.jvm.BackendReporting.ResultingMethodTooLarge;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.graphx.impl.AggregatingEdgeContext;
import org.apache.spark.rdd.RDD;

import java.awt.datatransfer.SystemFlavorMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.storage.StorageLevel;
import org.apache.zookeeper.KeeperException.SystemErrorException;

import com.codahale.metrics.graphite.GraphiteRabbitMQ;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsEmptyListSerializer;
import com.fasterxml.jackson.dataformat.yaml.snakeyaml.error.YAMLException;
import com.sun.xml.bind.v2.runtime.property.AttributeProperty;

import breeze.optimize.FirstOrderMinimizer.ConvergenceCheck;

public class NQuadReader {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("NQuadReader").setMaster("local[*]")
				.set("spark.executor.memory", "2g").set("spark.executor.cores", "1")
				.set("spark.dynamicAllocation.enabled", "true")
				.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		jsc.setLogLevel("ERROR");
		ClassTag<Object> objectTag = scala.reflect.ClassTag$.MODULE$.apply(Object.class);
		ClassTag<Relation> relationTag = scala.reflect.ClassTag$.MODULE$.apply(Relation.class);
		
		String path = "C:\\Users\\jenniffer\\Dropbox\\Masterarbeit";
		String fileName = "reification.nq";
		Graph<Object, Relation> quadGraph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);

		Graph<Object, Relation> sliceDiceGraph = sliceDice(quadGraph, jsc, objectTag, relationTag,
				"Level_Importance_All-All",
				"Level_Aircraft_All-All",
				"Level_Location_All-All",
				"Level_Date_All-All");		 	
	}
		
	public static Graph<Object, Relation> reify(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String context, String reificationPredicate, String type, String object, String subject) {
		
		JavaRDD<EdgeTriplet<Object, Relation>> statements = 
				quadGraph.triplets().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains(reificationPredicate));	

		Long typeID = getIdOfObject(type);
		List<Tuple2<Object, Object>> typeV = new ArrayList<Tuple2<Object, Object>>();
		typeV.add(new Tuple2<Object, Object>(typeID, type));
		JavaRDD<Tuple2<Object, Object>> typeVertice= jsc.parallelize(typeV);
		
		Relation objectRelation = new Relation(object, context, "Resource");
		Relation subjectRelation = new Relation(subject, context, "Resource");
		Relation typeRelation = new Relation(type, context, "Resource");
		
		//new Vertices
		JavaRDD<Tuple3<Object, Object, Object>> statementTuple3 = statements
				.map(x -> new Tuple3<Object, Object, Object>(x.srcId(), x.dstId(), new Resource("urn:uuid:"+UUID.randomUUID()))).persist(StorageLevel.MEMORY_ONLY());
		//1 = subject, 2 = object, 3 ID of new one, 3 = new Resource
		JavaRDD<Tuple4<Object, Object, Object, Object>> statementTuple4 = statementTuple3.map(x -> new Tuple4<Object, Object, Object, Object>(x._1(), x._2(), getIdOfObject(((Resource) x._3()).getValue()), (Resource) x._3()));		
		
		JavaRDD<Tuple2<Object, Object>> newVertices = statementTuple4.map(x -> new Tuple2<Object, Object>(x._3(), x._4()));
		//subject Edges
		JavaRDD<Edge<Relation>> subjectEdges = statementTuple4
		.map(x -> new Edge<Relation>((long) x._3(), (long) x._1(), subjectRelation));	

		JavaRDD<Tuple4<Object, Object, Object, Object>> statementVertices2 = statementTuple4;
		//object Edges
		JavaRDD<Edge<Relation>> objectEdges = statementVertices2
				.map(x -> new Edge<Relation>((long) x._3(), (long) x._2(), objectRelation));	
		
		//type Edges
		JavaRDD<Edge<Relation>> typeEdges = statementTuple4
		.map(x -> 
		new Edge<Relation>((long) x._3(),  typeID, typeRelation));
		
		//combining old edges and vertices with new ones
		JavaRDD<Edge<Relation>> allEdges = quadGraph.edges().toJavaRDD().union(subjectEdges).union(objectEdges).union(typeEdges).distinct();
		JavaRDD<Tuple2<Object, Object>> allVertices = quadGraph.vertices().toJavaRDD().union(typeVertice).union(newVertices).distinct();
				
		//create new graph with added vertices and edges
		Graph<Object, Relation> graph = Graph.apply(allVertices.rdd(),
				allEdges.rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag,
				relationTag);
		
		return graph;
	}
	
	public static Graph<Object, Relation> pivot(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String dimensionProperty, String pivotProperty, String type,
			String selectionCondition, String context) {
		Long dimPropertyVertice = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains(dimensionProperty)).map(x -> x.dstId())
				.first();

		JavaRDD<Edge<Relation>> newEdges = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getContext().toString().contains(context)
						&& x.attr().getRelationship().toString().contains(type)
						&& x.dstAttr().toString()
								.contains(selectionCondition))
				.map(x -> new Edge<Relation>(x.srcId(), dimPropertyVertice,
						new Relation(pivotProperty, context, "Resource")));
		
		RDD<Edge<Relation>> allEdges = quadGraph.edges().union(newEdges.rdd());

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices().toJavaRDD().rdd(),
				allEdges, "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag,
				relationTag);
		return graph;
	}
	
	public static Graph<Object, Relation> aggregatePropertyValues(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String aggregateProperty,
			String mod, String aggregateType) {
		JavaRDD<Tuple2<Long, Long>> verticesRDD = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().equals(aggregateProperty)).map(x
						-> new Tuple2<Long, Long>((Long) x.srcId(), Long.parseLong(x.dstAttr().toString())));
		JavaPairRDD<Long, Long> verticesPair = JavaPairRDD.fromJavaRDD(verticesRDD);
						
		JavaRDD<Tuple2<Object, Object>> newVertice = null;
		JavaRDD<Edge<Relation>> newEdges = null;

		if (aggregateType.contains("AVG")) { //done
			newEdges = verticesPair.mapValues(x -> new Tuple2<Long, Long>(x, 1L))
					.reduceByKey((x,y) -> new Tuple2<Long, Long>(x._1 + y._1, x._2 + y._2))
					.mapValues(x -> x._1 /x._2)
					.map(x -> new Edge<Relation>(x._1, getIdOfObject(x._2), new Relation(aggregateProperty, mod, Double.class.getSimpleName().toString())));
					
			newVertice = verticesPair.mapValues(x -> new Tuple2<Long, Long>(x, 1L))
					.reduceByKey((x,y) -> new Tuple2<Long, Long>(x._1 + y._1, x._2 + y._2))
					.mapValues(x -> x._1 /x._2)
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}
		
		if (aggregateType.contains("COUNT")) { 
			newEdges = verticesPair.mapValues(x -> 1L)
					.reduceByKey((x,y) -> x + y)
					.map(x -> new Edge<Relation>(x._1, getIdOfObject(x._2), new Relation(aggregateProperty, mod, Double.class.getSimpleName().toString())));
			
			newVertice = verticesPair.mapValues(x -> 1L)
					.reduceByKey((x,y) -> x + y)
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));	
		}
		if (aggregateType.contains("SUM")) { 
			newEdges = verticesPair.reduceByKey((a,b) -> a+b)
					.map(x -> new Edge<Relation>(x._1, getIdOfObject(x._2), new Relation(aggregateProperty, mod, Double.class.getSimpleName().toString())));
			
			newVertice = verticesPair.reduceByKey((a,b) -> a+b)
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
			
		}
		if (aggregateType.contains("MAX")) {
			newEdges = verticesPair.reduceByKey((a,b) -> Math.max(a, b))
					.map(x -> new Edge<Relation>(x._1, getIdOfObject(x._2), new Relation(aggregateProperty, mod, Double.class.getSimpleName().toString())));
			
			newVertice = verticesPair.reduceByKey((a,b) -> Math.max(a, b))
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}
		if (aggregateType.contains("MIN")) {
			newEdges = verticesPair.reduceByKey((a,b) -> Math.min(a, b))
					.map(x -> new Edge<Relation>(x._1, getIdOfObject(x._2), new Relation(aggregateProperty, mod, Double.class.getSimpleName().toString())));
			
			newVertice = verticesPair.reduceByKey((a,b) -> Math.min(a, b))
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}
		
		JavaRDD<Edge<Relation>> keepTriplets = quadGraph.triplets().toJavaRDD()
				.filter(x -> !x.attr().getRelationship().toString().equals(aggregateProperty))
				.map(x -> new Edge<Relation>(x.srcId(), x.dstId(), x.attr()));
		
		
		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices().toJavaRDD().union(newVertice).rdd(),
				keepTriplets.union(newEdges).rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag,
				relationTag);
		return graph;
	}	
	
	public static Graph<Object, Relation> groupByProperty(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String groupingProperty,
			String groupingPredicate, String mod) {

		JavaRDD<Tuple2<Object, Object>> newVertices = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains(groupingProperty))
				.map(x -> new Tuple2<Object, Object>(getIdOfObject(x.dstAttr()+"-Group"), x.dstAttr()+"-Group"));
	
		Relation newRelation = new Relation(groupingPredicate, mod, "Resource");
		JavaRDD<Edge<Relation>> groupingEdges = 
				quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains(groupingProperty))
				.map(x -> new Tuple2<Object, Object>(x.srcAttr(), x.dstAttr()+"-Group"))
				.distinct()
				.map(x -> new Edge<Relation>(getIdOfObject(x._1), getIdOfObject(x._2), newRelation));
						
		JavaRDD<Tuple4<Object, Object, Object, Object>> groups = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains(groupingProperty)).distinct()
				.map(x -> 
					new Tuple4<Object, Object, Object, Object>(x.srcId(), x.srcAttr(), getIdOfObject(x.dstAttr()+"-Group"), x.dstAttr()+"-Group"));
		
		List<Edge<Relation>> edges = new ArrayList<Edge<Relation>>();
				
		//subjects
		groups.collect().forEach(x ->{
			JavaRDD<Edge<Relation>> subjectsReplaced = quadGraph.triplets().toJavaRDD()
			.filter(y -> y.srcId() == (long)x._1()
			&& !y.attr().getRelationship().toString().contains(groupingProperty)
			&& !y.attr().getRelationship().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
			.map(y -> new Edge<Relation>((long) x._3(), y.dstId(), y.attr()));

			//objects
			JavaRDD<Edge<Relation>> objectsReplaced= quadGraph.triplets().toJavaRDD()
					.filter(y -> y.dstId() == (long)x._1()
					&& !y.attr().getRelationship().toString().contains(groupingProperty)
					&& !y.attr().getRelationship().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
					.map(y -> new Edge<Relation>(y.srcId(), (long) x._3(), y.attr()));			
			
			//Others - tokeep
			JavaRDD<Edge<Relation>> keepEdges = quadGraph.triplets().toJavaRDD()
				.filter(y -> y.srcId() != (long)x._1() && y.dstId() != (long) x._1()
				&& y.attr().getRelationship().toString().contains(groupingProperty)
				|| y.attr().getRelationship().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
				.map(y -> new Edge<Relation>(y.srcId(), y.dstId(), y.attr()));
			
			edges.addAll(subjectsReplaced.union(objectsReplaced).union(keepEdges).collect());
			});
			
		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices().union(newVertices.rdd()).distinct(),
				jsc.parallelize(edges).rdd().union(groupingEdges.rdd()).distinct(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag,
				relationTag);
		return graph;

	}

	public static Graph<Object, Relation> replaceByGrouping(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,

			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String toBeReplaced, String groupingValue) {
		// get instances of the type (subjects) that has to be replaced (that hace "toBeReplaced" as object)
		List<String> subjectsToBeReplaced = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.dstAttr().toString().contains(toBeReplaced)).map(x -> x.srcAttr().toString()).collect();
		
		Set<Edge<Relation>> allEdges = new LinkedHashSet<Edge<Relation>>();
		Set<Edge<Relation>> toBeRemovedEdges = new LinkedHashSet<Edge<Relation>>();

		subjectsToBeReplaced.forEach(x -> {
			List<Edge<Relation>> newTriplets = new ArrayList<>();
			List<Edge<Relation>> removeTriplets = new ArrayList<>();

			//get the grouping object that should replace the current subject
			List<String> grouping = quadGraph.triplets().toJavaRDD()
					.filter(y -> y.srcAttr().toString().contains(x)
							&& y.attr().getRelationship().toString().contains(groupingValue))
					.map(y -> y.dstAttr().toString()).collect();
			
			//get the ID of this object that should replace the subject
			Long groupingId = getIdOfObject(grouping.get(0));

			//new triples to add to the graph (with object that replaces the current subject, not type, not groupingValue
			newTriplets.addAll(quadGraph.triplets().toJavaRDD()
					.filter(y -> y.srcAttr().toString().contains(x)
							&& !y.attr().getRelationship().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
							&& !y.attr().getRelationship().toString().contains(groupingValue))
					.map(y -> new Edge<Relation>((long) groupingId, y.dstId(), y.attr())).collect());
			
			//add new triplets where object is replaced
			newTriplets.addAll(quadGraph.triplets().toJavaRDD()
					.filter(y -> y.dstAttr().toString().contains(x)
							&& !y.attr().getRelationship().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
							&& !y.attr().getRelationship().toString().contains(groupingValue))
					.map(y -> new Edge<Relation>(y.srcId(), (long) groupingId, y.attr())).collect());
			allEdges.addAll(newTriplets);
			
			// triples to be removed - subject + object
			removeTriplets = quadGraph.triplets().toJavaRDD()
					.filter(y -> (y.srcAttr().toString().contains(x) || y.dstAttr().toString().contains(x))
							&& !y.attr().getRelationship().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
							&& !y.attr().getRelationship().toString().contains(groupingValue))
					.map(y -> new Edge<Relation>(y.srcId(), y.dstId(), y.attr())).collect();
			toBeRemovedEdges.addAll(removeTriplets);
		});
		
		List<Edge<Relation>> edgeList = new ArrayList<Edge<Relation>>();

		edgeList.addAll(quadGraph.edges().toJavaRDD().collect());
		edgeList.addAll(allEdges);
		edgeList.removeAll(toBeRemovedEdges);
		JavaRDD<Edge<Relation>> edges = jsc.parallelize(edgeList);

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices().toJavaRDD().rdd(), edges.rdd(), "",
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag, relationTag);
		
		return graph;
	}

	public static Graph<Object, Relation> merge(Graph<Object, Relation> graph, JavaSparkContext jsc,
			String levelImportance, String levelAircraft, String levelLocation, String levelDate, ClassTag<Object> objectTag, ClassTag<Relation> relationTag) {
		JavaRDD<Tuple2<Object, Object>> importance = getCellsAtDimensionLevel(graph, "hasImportance", levelImportance, jsc);
		JavaRDD<Tuple2<Object, Object>> aircraft = getCellsAtDimensionLevel(graph, "hasAircraft", levelAircraft, jsc);
		JavaRDD<Tuple2<Object, Object>> location = getCellsAtDimensionLevel(graph, "hasLocation", levelLocation, jsc);
		JavaRDD<Tuple2<Object, Object>> date = getCellsAtDimensionLevel(graph, "hasDate", levelDate, jsc);
//		// check which cells satisfy all four dimensions
		JavaRDD<Tuple2<Object, Object>> result = importance.intersection(aircraft).intersection(location).intersection(date);
		Broadcast<List<Tuple2<Object, Object>>> resultBroadcast = jsc.broadcast(result.collect());
		List<Edge<Relation>> allEdges = new ArrayList<Edge<Relation>>();
				allEdges.addAll(graph.edges().toJavaRDD().collect());
		resultBroadcast.value().forEach(x -> {
			JavaRDD<String> covered = GraphGenerator.getCoverage(x._2.toString(), jsc).map(z -> z + "-mod");
			Broadcast<List<String>> coveredBroadcast = jsc.broadcast(covered.collect());
			
			JavaRDD<Edge<Relation>> newEdges = graph.edges().toJavaRDD()
				.filter(y -> coveredBroadcast.value().contains(y.attr().getContext().toString()))
				.map(y -> new Edge<Relation>(y.srcId(), y.dstId(), y.attr().updateContext(x._2.toString() + "-mod")));
			JavaRDD<Edge<Relation>> removeEdges = graph.edges().toJavaRDD()
					.filter(y -> coveredBroadcast.value().contains(y.attr().getContext().toString()));
			allEdges.addAll(newEdges.collect());
			allEdges.removeAll(removeEdges.collect());
		});
		
		Graph<Object, Relation> resultGraph  = Graph.apply(graph.vertices().toJavaRDD().rdd(), jsc.parallelize(allEdges).rdd(), "",
						StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag, relationTag);		
		return resultGraph;
	}

	public static JavaRDD<Tuple2<Object, Object>> getVerticesAttributes(Graph<Object, Relation> graph, JavaRDD<Long> ids, JavaSparkContext jsc) {
		Broadcast<List<Long>> broadcastList = jsc.broadcast(ids.collect());

		JavaRDD<Tuple2<Object, Object>> result = graph.vertices().toJavaRDD().filter(x -> broadcastList.value().contains(x._1)).rdd().toJavaRDD();
		
		return result;
	}


	public static Graph<Object, Relation> sliceDice(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String importanceValue, String aircraftValue,
			String locationValue, String dateValue) {
		
		if(importanceValue=="Level_Importance_All-All" 
				&& aircraftValue == "Level_Aircraft_All-All"
				&& locationValue == "Level_Location_All-All"
				&& dateValue == "Level_Date_All-All") {return quadGraph;}

		JavaRDD<Tuple2<Object, Object>> cellIds = getCellsWithDimValues(quadGraph, jsc, importanceValue, aircraftValue, locationValue,
				dateValue);

		JavaRDD<String> mods = getMods(quadGraph, cellIds, jsc);
		
		Broadcast<List<String>> broadcastMods = jsc.broadcast(mods.collect());
		JavaRDD<Edge<Relation>> result = 
				quadGraph.triplets().toJavaRDD()
				.filter(x -> broadcastMods.value().contains(x.attr().getContext().toString()) || 
						x.attr().getContext().toString().contains("global")).map(x -> new Edge<Relation>(x.srcId(), x.dstId(), x.attr()));
		
		Graph<Object, Relation> resultGraph = Graph.apply(quadGraph.vertices(),
				result.rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag,
				relationTag);

		return resultGraph;
	}

	private static JavaRDD<Tuple2<Object, Object>> getCellsWithDimValues(Graph<Object, Relation> graph, JavaSparkContext jsc,
			String importanceValue, String aircraftValue, String locationValue, String dateValue) {
		// get cells for each dimension individually
		JavaRDD<Tuple2<Object, Object>> importance = getCellsWithDimValueAndCovered(graph, "hasImportance", importanceValue, jsc);
		JavaRDD<Tuple2<Object, Object>> aircraft = getCellsWithDimValueAndCovered(graph, "hasAircraft", aircraftValue, jsc);
		JavaRDD<Tuple2<Object, Object>> location = getCellsWithDimValueAndCovered(graph, "hasLocation", locationValue, jsc);
		JavaRDD<Tuple2<Object, Object>> date = getCellsWithDimValueAndCovered(graph, "hasDate", dateValue, jsc);
		
		JavaRDD<Tuple2<Object, Object>> result = importance.intersection(aircraft).intersection(location).intersection(date);
		
		return result;
	}

	private static JavaRDD<Tuple2<Object, Object>> getCellsWithDimValueAndCovered(Graph<Object, Relation> graph, String dimension,
			String value, JavaSparkContext jsc) {

		// array for all current Instances and their covered ones
		ArrayList<String> coveredInstances = new ArrayList<>();
		coveredInstances.add(value);
		JavaRDD<String> instances = jsc.parallelize(coveredInstances);
				
		if(GraphGenerator.getCoveredInstances(value, jsc) != null) {
			instances = instances.union(GraphGenerator.getCoveredInstances(value, jsc));
		}
		
		List<String> instanceList =  instances.collect();
		
		JavaRDD<Tuple2<Object, Object>> result = graph.triplets().toJavaRDD()
				.filter(triplet -> 
					triplet.attr().getRelationship().toString().contains(dimension)
						&& triplet.dstAttr().toString().length() >= 45
						 && instanceList.contains(triplet.dstAttr().toString().subSequence(33, 45)))
				.map(triplet -> new Tuple2<Object, Object>(triplet.srcId(), triplet.srcAttr()));
		return result;
	}

	// get all Mods (as String) that belong to the Cells that have the correct
	// dimensions
	private static JavaRDD<String> getMods(Graph<Object, Relation> quadGraph, JavaRDD<Tuple2<Object, Object>> cellIds, JavaSparkContext jsc) {
		// get mods
		Broadcast<List<Long>> broadcastCellIds = jsc.broadcast(cellIds.map(x -> (long) x._1).collect());
		JavaRDD<String> modules = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains("hasAssertedModule")
						&& broadcastCellIds.value().contains(x.srcId())).map(x -> x.dstAttr().toString());
		return modules;
	}

	// get Cells With A Certain Level
	private static JavaRDD<Tuple2<Object, Object>> getCellsAtDimensionLevel(Graph<Object, Relation> graph, String dimension, String level, JavaSparkContext jsc) {
				
		//https://stackoverflow.com/questions/26214112/filter-based-on-another-rdd-in-spark		
		JavaRDD<Long> aList = graph.triplets().toJavaRDD()
						.filter(triplet -> triplet.attr().getRelationship().toString().contains("atLevel")
								&& triplet.dstAttr().toString().contains(level))
						.map(triplet -> triplet.srcId());
		Broadcast<List<Long>> broadcastList = jsc.broadcast(aList.collect());		
		
		JavaRDD<EdgeTriplet<Object, Relation>> g = graph.triplets().toJavaRDD()
				.filter(triplet -> triplet.attr().getRelationship().toString().contains(dimension));
		
		JavaRDD<Tuple2<Object, Object>> result = g.filter(x -> broadcastList.getValue().contains(x.dstId())).map(x -> new Tuple2<Object, Object>(x.srcId(), x.srcAttr()));
		
		return result;
	}
	
	public static long getIdOfObject(Object s) {
		return UUID.nameUUIDFromBytes(s.toString().getBytes()).getMostSignificantBits();
	}
}
