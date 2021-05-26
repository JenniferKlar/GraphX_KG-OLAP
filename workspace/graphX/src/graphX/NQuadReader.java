package graphX;

import java.util.UUID;

import org.apache.spark.SparkConf;

import scala.reflect.ClassTag;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.storage.StorageLevel;

public class NQuadReader {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("NQuadReader").setMaster("local[*]")
				.set("spark.executor.memory", "2g").set("spark.executor.cores", "1")
				.set("spark.dynamicAllocation.enabled", "true")
				.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		ClassTag<Object> objectTag = scala.reflect.ClassTag$.MODULE$.apply(Object.class);
		ClassTag<Relation> relationTag = scala.reflect.ClassTag$.MODULE$.apply(Relation.class);

		Graph<Object, Relation> quadGraph = GraphGenerator.generateGraph(jsc, objectTag, relationTag);

//		Graph<Object, Relation> sliceDiceGraph = sliceDice(quadGraph, jsc,
//		objectTag,relationTag,"Level_Importance_All-All","Level_Aircraft_All-All","Level_Location_All-All",
//		"Level_Date_All-All");

//		Graph<Object, Relation> mergedGraph =
//		merge(quadGraph,jsc,"Level_Importance_Package",
//		"Level_Aircraft_All","Level_Location_Region","Level_Date_Year");
//

//		// tripleGeneratingAbstraction - replaceByGrouping
//		
//		 Graph<Object, Relation> replaceAreaUsageGraph 
//		 = replaceByGrouping(quadGraph, jsc, objectTag, relationTag,"ManoeuvringAreaUsage", "usageType");
//		 
//
//		// individualGeneratingAbstraction
//		Graph<Object, Relation> groupOperationalStatus = groupByProperty(quadGraph, jsc, objectTag, relationTag,
//				"operationalStatus", "http://example.org/kgolap/object-model#grouping",
//				"urn:uuid:8378d3c2-575d-4cb8-874b-eb4ae286d61b-mod");


		// Pivoting
		// for specific context!
//		Graph<Object, Relation> pivotLocationGraph = pivotGraph(quadGraph, jsc, objectTag, relationTag, "hasLocation",
//				"http://example.org/kgolap/object-model#hasLocation", "type",
//				"http://example.org/kgolap/object-model#ManoeuvringAreaAvailability",
//				"urn:uuid:2c95e204-26ea-43ec-a997-774b5dc41c6d-mod");
		

		//Reification
//		Graph<Object, Relation> reificationUsage = 
//		reificationGraph(quadGraph, jsc, objectTag, relationTag,"urn:uuid:0acc4b38-168d-4a33-898c-258b89881556-mod", "http://example.org/kgolap/object-model#usage",
//				"http://example.org/kgolap/object-model#usage-type", "http://www.w3.org/1999/02/22-rdf-syntax-ns#object", "http://www.w3.org/1999/02/22-rdf-syntax-ns#subject");
//		

			//aggregatePropertyValues + aggregateProperty - needs some grouping (probably most of the time)
//		 Graph<Object, Relation> replaceAircraftCharacteristics 
//		 = replaceByGrouping(quadGraph, jsc, objectTag, relationTag,"AircraftCharacteristic", "wingspanInterpretation");
//		 Graph<Object, Relation> wingspanSum = aggregatePropertyValues(replaceAircraftCharacteristics, jsc, objectTag, relationTag, 
//				 "http://example.org/kgolap/object-model#wingspan", "<urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod>", "SUM"); 
//		 wingspanAverage.triplets().toJavaRDD().foreach(x -> System.out.println(x.srcAttr() + " " + x.attr().getRelationship().toString() + " " + x.dstAttr() + " " + x.attr().getContext().toString()));

		 Graph<Object, Relation> replaceAircraftCharacteristics 
		 = groupByProperty(quadGraph, jsc, objectTag, relationTag,"http://example.org/kgolap/object-model#wingspanInterpretation", "http://example.org/kgolap/object-model#grouping", "<urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod>");
		 //replaceAircraftCharacteristics.triplets().toJavaRDD().foreach(x -> System.out.println(x.srcAttr() + " " + x.attr().getRelationship().toString() + " " + x.dstAttr() + " " + x.attr().getContext().toString()));

		 Graph<Object, Relation> wingspanSum = aggregatePropertyValues(replaceAircraftCharacteristics, jsc, objectTag, relationTag, 
				 "http://example.org/kgolap/object-model#wingspan", "<urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod>", "SUM"); 
		 wingspanSum.triplets().toJavaRDD().foreach(x -> System.out.println(x.srcAttr() + " " + x.attr().getRelationship().toString() + " " + x.dstAttr() + " " + x.attr().getContext().toString()));
		 	
	}
	private static Graph<Object, Relation> reificationGraph(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String context, String reifictaionPredicate, String type, String object, String subject) {
		
		List<EdgeTriplet<Object, Relation>> statements = 
				quadGraph.triplets().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains(reifictaionPredicate)).collect();
		List<Tuple2<Object, Object>> newVertices = new ArrayList<Tuple2<Object, Object>>();
		List<Edge<Relation>> newEdges = new ArrayList<Edge<Relation>>();
		
		List<Tuple2<Object, Object>> allVertices = new ArrayList<Tuple2<Object, Object>>();
		allVertices.addAll(quadGraph.vertices().toJavaRDD().collect());
		List<Edge<Relation>> allEdges = new ArrayList<Edge<Relation>>();
		allEdges.addAll(quadGraph.edges().toJavaRDD().collect());

		Long typeID = quadGraph.vertices().count() + 1;
		Tuple2<Object, Object> typeVertice= new Tuple2<Object, Object>(typeID, type);
		newVertices.add(typeVertice);
		
		Relation objectRelation = new Relation(object, context, "Resource");
		Relation subjectRelation = new Relation(subject, context, "Resource");
		Relation typeRelation = new Relation(type, context, "Resource");
		statements.forEach(x -> {
					String uuid = "urn:uuid:"+UUID.randomUUID().toString();
					newVertices.add(new Tuple2<Object, Object>((long) (quadGraph.vertices().count() + 2L + statements.indexOf(x)), uuid));
					newEdges.add(new Edge<Relation>((long) (quadGraph.vertices().count() + 2L + statements.indexOf(x)), x.srcId(), subjectRelation)); //subject
					newEdges.add(new Edge<Relation>((long) (quadGraph.vertices().count() + 2L + statements.indexOf(x)), x.dstId(), objectRelation)); //object
					newEdges.add(new Edge<Relation>((long) (quadGraph.vertices().count() + 2L + statements.indexOf(x)), typeID, typeRelation)); //type
		});
		allVertices.addAll(newVertices);
		allEdges.addAll(newEdges);
		
		Graph<Object, Relation> graph = Graph.apply(jsc.parallelize(allVertices).rdd(),
				jsc.parallelize(allEdges).rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag,
				relationTag);
		return graph;
	}

	
	private static Graph<Object, Relation> pivotGraph(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String dimensionProperty, String pivotProperty, String type,
			String selectionCondition, String context) {
		List<Long> dimPropertyVertice = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains(dimensionProperty)).map(x -> x.dstId())
				.collect();
		List<Edge<Relation>> newEdges = new ArrayList<Edge<Relation>>();

		newEdges = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getContext().toString().contains(context)
						&& x.attr().getRelationship().toString().contains(type)
						&& x.dstAttr().toString()
								.contains(selectionCondition))
				.map(x -> new Edge<Relation>(x.srcId(), dimPropertyVertice.get(0),
						new Relation(pivotProperty, context, "Resource")))
				.collect();

		List<Edge<Relation>> oldEdges = new ArrayList<Edge<Relation>>();
		oldEdges.addAll(quadGraph.edges().toJavaRDD().collect());
		oldEdges.addAll(newEdges);

		Graph<Object, Relation> graph = Graph.apply(jsc.parallelize(quadGraph.vertices().toJavaRDD().collect()).rdd(),
				jsc.parallelize(oldEdges).rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag,
				relationTag);
		return graph;
	}

	
	private static Graph<Object, Relation> aggregatePropertyValues(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String wingspan,
			String mod, String aggregateType) {
		JavaRDD<String> wingspanVerticesRDD = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().equals(wingspan))
				.map(x -> x.srcAttr().toString());
		List<String> wingspanVertices = wingspanVerticesRDD.distinct().collect();

		List<Tuple2<Object, Object>> currentVertices = new ArrayList<>(quadGraph.vertices().toJavaRDD().collect());
		Set<Edge<Relation>> newEdges = new LinkedHashSet<>();

		wingspanVertices.forEach(x -> {
				// only size 1
			Long sum = quadGraph.triplets().toJavaRDD()
						.filter(z -> z.srcAttr().toString().contains(x)
								&& z.attr().getRelationship().toString().equals(wingspan)
								&& z.attr().getTargetDataType() != "Resource")
						.map(z -> Long.parseLong(z.dstAttr().toString())).reduce((a,b) -> a+b);
			Long count = quadGraph.triplets().toJavaRDD()
					.filter(z -> z.srcAttr().toString().contains(x)
							&& z.attr().getRelationship().toString().equals(wingspan)).count();

			Long value = null;
			if (aggregateType.contains("AVG")) {
				value = sum / count;
			}
			if (aggregateType.contains("COUNT")) {
				value = count;
			}
			if (aggregateType.contains("SUM")) {
				value = sum;
			}
			if (aggregateType.contains("MIN")) {
				value = quadGraph.triplets().toJavaRDD()
						.filter(z -> z.srcAttr().toString().contains(x)
								&& z.attr().getRelationship().toString().contains(wingspan))
						.map(z -> Long.parseLong(z.dstAttr().toString())).min(Comparator.naturalOrder());
			}
			if (aggregateType.contains("MAX")) {
				value = quadGraph.triplets().toJavaRDD()
						.filter(z -> z.srcAttr().toString().contains(x)
								&& z.attr().getRelationship().toString().contains(wingspan))
						.map(z -> Long.parseLong(z.dstAttr().toString())).max(Comparator.naturalOrder());
			}
			
			String literal = Long.toString(value);
			Tuple2<Object, Object> newVertice = new Tuple2<Object, Object>(
					quadGraph.vertices().count() + wingspanVertices.indexOf(x), literal);
			currentVertices.add(newVertice);
			
			// new edge with old vertice and new value..
			Long verticeID = getVerticeId(quadGraph, x);
			Edge<Relation> newEdge = new Edge<Relation>(verticeID,
					quadGraph.vertices().count() + wingspanVertices.indexOf(x),
					new Relation(wingspan, mod, Double.class.getSimpleName().toString()));
			newEdges.add(newEdge);
		});
		
		List<Edge<Relation>> removeTriplets = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().equals(wingspan))
				.map(x -> new Edge<Relation>(x.srcId(), x.dstId(), x.attr())).collect();

		List<Edge<Relation>> edgeList = new ArrayList<Edge<Relation>>();
		edgeList.addAll(quadGraph.edges().toJavaRDD().collect());
		edgeList.removeAll(removeTriplets);
		edgeList.addAll(newEdges);

		Graph<Object, Relation> graph = Graph.apply(jsc.parallelize(currentVertices).rdd(),
				jsc.parallelize(edgeList).rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag,
				relationTag);
		return graph;
	}
	

	private static Graph<Object, Relation> groupByProperty(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String groupingProperty,
			String groupingPredicate, String mod) {

		JavaRDD<String> groupsRDD = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains(groupingProperty))
				.map(x -> x.dstAttr().toString());

		List<String> groups = groupsRDD.distinct().collect();
		List<Tuple2<Object, Object>> currentVertices = new ArrayList<>(quadGraph.vertices().toJavaRDD().collect());
		Set<Edge<Relation>> newEdges = new LinkedHashSet<>();
		Set<Edge<Relation>> edgesToBeRemoved = new LinkedHashSet<>();
		groups.forEach(x -> {
			// create and add new Vertex
			Resource newResource = new Resource(x + "-Group");
			Tuple2<Object, Object> newVertice = new Tuple2<Object, Object>(
					quadGraph.vertices().count() + groups.indexOf(x), newResource);
			currentVertices.add(newVertice);

			// reference to grouping
			Relation newRelation = new Relation(groupingPredicate, mod, "Resource");
			// all individuals with the same property e.g. operationalStatus
			List<Long> verticesToBeReplaced = quadGraph.triplets().toJavaRDD()
					.filter(y -> y.dstAttr().toString().contains(x)).map(y -> y.srcId()).collect();
			// for each individual a reference to the new grouping
			verticesToBeReplaced.forEach(y -> {
				Edge<Relation> newEdge = new Edge<Relation>(y, quadGraph.vertices().count() + groups.indexOf(x),
						newRelation);
				newEdges.add(newEdge);
				// edges to be removed
				List<Edge<Relation>> removeEdges = quadGraph.triplets().toJavaRDD().filter(z -> z.srcId() == y)
						.map(z -> new Edge<Relation>(z.srcId(), z.dstId(), z.attr())).collect();
				edgesToBeRemoved.addAll(removeEdges);
				// add new Triples where individuals are replaced by grouping
				List<Edge<Relation>> edgesToAdd = new ArrayList<>();
				edgesToAdd = quadGraph.triplets().toJavaRDD().filter(z -> z.srcId() == (y))
						.map(z -> new Edge<Relation>((long) newVertice._1, z.dstId(), z.attr())).collect();
				newEdges.addAll(edgesToAdd);
			});
		});

		List<Edge<Relation>> edgeList = new ArrayList<Edge<Relation>>();
		edgeList.addAll(quadGraph.edges().toJavaRDD().collect());
		edgeList.addAll(newEdges);
		edgeList.removeAll(edgesToBeRemoved);

		Graph<Object, Relation> graph = Graph.apply(jsc.parallelize(currentVertices).rdd(),
				jsc.parallelize(edgeList).rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag,
				relationTag);

		return graph;
	}

	private static Graph<Object, Relation> replaceByGrouping(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String toBeReplaced, String groupingValue) {
		// get instances of the type that has to be replaced
		List<String> verticesToBeReplaced = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.dstAttr().toString().contains(toBeReplaced)).map(x -> x.srcAttr().toString()).collect();

		Set<Edge<Relation>> allEdges = new LinkedHashSet<Edge<Relation>>();
		Set<Edge<Relation>> toBeRemovedEdges = new LinkedHashSet<Edge<Relation>>();

		verticesToBeReplaced.forEach(x -> {
			List<Edge<Relation>> newTriplets = new ArrayList<>();
			List<Edge<Relation>> removeTriplets = new ArrayList<>();

			List<String> grouping = quadGraph.triplets().toJavaRDD()
					.filter(y -> y.srcAttr().toString().contains(x)
							&& y.attr().getRelationship().toString().contains(groupingValue))
					.map(y -> y.dstAttr().toString()).collect();

			List<Object> groupingId = quadGraph.vertices().toJavaRDD()
					.filter(y -> y._2.toString().contains(grouping.get(0))).map(y -> y._1).collect();

			// triples to add to the graph
			newTriplets = quadGraph.triplets().toJavaRDD().filter(y -> y.srcAttr().toString().contains(x))
					.map(y -> new Edge<Relation>((long) groupingId.get(0), y.dstId(), y.attr())).collect();

			// triples to be removed
			removeTriplets = quadGraph.triplets().toJavaRDD().filter(y -> y.srcAttr().toString().contains(x))
					.map(y -> new Edge<Relation>(y.srcId(), y.dstId(), y.attr())).collect();
			allEdges.addAll(newTriplets);
			toBeRemovedEdges.addAll(removeTriplets);
		});

		List<Edge<Relation>> edgeList = new ArrayList<Edge<Relation>>();

		edgeList.addAll(quadGraph.edges().toJavaRDD().collect());
		edgeList.addAll(allEdges);
		edgeList.removeAll(toBeRemovedEdges);
		JavaRDD<Edge<Relation>> edges = jsc.parallelize(edgeList);
		edges = edges.filter(x -> !x.attr().getRelationship().toString().contains(groupingValue));

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices().toJavaRDD().rdd(), edges.rdd(), "",
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag, relationTag);
		return graph;
	}

	private static Graph<Object, Relation> merge(Graph<Object, Relation> graph, JavaSparkContext jsc,
			String levelImportance, String levelAircraft, String levelLocation, String levelDate) {
		List<Long> importance = getCellsAtDimenionLevel(graph, "hasImportance", levelImportance);
		List<Long> aircraft = getCellsAtDimenionLevel(graph, "hasAircraft", levelAircraft);
		List<Long> location = getCellsAtDimenionLevel(graph, "hasLocation", levelLocation);
		List<Long> date = getCellsAtDimenionLevel(graph, "hasDate", levelDate);

		List<Long> result = new ArrayList<>();

		// check which cells satisfy all four dimensions
		importance.forEach(x -> {
			if (aircraft.contains(x) && location.contains(x) && date.contains(x)) {
				result.add(x);
			}
		});

		getVerticesAttributes(graph, result).forEach(x -> {
			List<String> covered = new ArrayList<>();
			covered.addAll(GraphGenerator.getCoverage(x._2.toString(), jsc).map(z -> z + "-mod").collect());
			graph.triplets().toJavaRDD().foreach(y -> {
				if (covered.contains(y.attr().getContext().toString())) {
					y.attr().setContext(x._2.toString() + "-mod");
				}
			});
		});
		return graph;
	}

	private static List<Tuple2<Object, Object>> getVerticesAttributes(Graph<Object, Relation> graph, List<Long> ids) {
		List<Tuple2<Object, Object>> result = new ArrayList<>();
		ids.forEach(x -> {
			result.addAll(graph.vertices().toJavaRDD().filter(y -> y._1.equals(x)).collect());
		});
		return result;
	}

	private static Long getVerticeId(Graph<Object, Relation> graph, String attribute) {
		List<Long> result = graph.vertices().toJavaRDD().filter(x -> x._2.toString().contains(attribute))
				.map(x -> (Long) x._1).collect();
		Long res = result.get(0);
		return res;
	}

	private static Graph<Object, Relation> sliceDice(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String importanceValue, String aircraftValue,
			String locationValue, String dateValue) {
		List<Long> cellIds = getCellsWithDimValues(quadGraph, jsc, importanceValue, aircraftValue, locationValue,
				dateValue);
		List<String> mods = getMods(quadGraph, cellIds);

		// get Data of all Mods - that have mod as context
		List<EdgeTriplet<Object, Relation>> triplets = new ArrayList<>();
		mods.forEach(x -> {
			triplets.addAll(quadGraph.triplets().toJavaRDD().filter(e -> e.attr().getContext().toString().contains(x))
					.collect());
		});

		List<Tuple2<Object, Object>> verticeTuples = new ArrayList<Tuple2<Object, Object>>();
		List<Edge<Relation>> edges = new ArrayList<Edge<Relation>>();
		// all sources and destinations (their ids) of all triplets in mods
		triplets.forEach(x -> {
			verticeTuples.add(new Tuple2<>(x.srcId(), x.srcAttr()));
			verticeTuples.add(new Tuple2<>(x.dstId(), x.dstAttr()));
			edges.add(new Edge<Relation>(x.srcId(), x.dstId(), x.attr()));
		});

		// create graph from filtered edges and vertices
		Graph<Object, Relation> resultGraph = Graph.apply(jsc.parallelize(verticeTuples).rdd(),
				jsc.parallelize(edges).rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag,
				relationTag);

		return resultGraph;
	}

	private static List<Long> getCellsWithDimValues(Graph<Object, Relation> graph, JavaSparkContext jsc,
			String importanceValue, String aircraftValue, String locationValue, String dateValue) {
		// get cells for each dimension individually
		List<Long> importance = getCellsWithDimValueAndCovered(graph, "hasImportance", importanceValue, jsc);
		List<Long> aircraft = getCellsWithDimValueAndCovered(graph, "hasAircraft", aircraftValue, jsc);
		List<Long> location = getCellsWithDimValueAndCovered(graph, "hasLocation", locationValue, jsc);
		List<Long> date = getCellsWithDimValueAndCovered(graph, "hasDate", dateValue, jsc);

		List<Long> result = new ArrayList<>();
		// compare lists --> if cell is in each --> return it (satisfies all four
		// dimensions)
		importance.forEach(x -> {
			if (aircraft.contains(x) && location.contains(x) && date.contains(x)) {
				result.add(x);
			}
		});
		return result;
	}

	private static List<Long> getCellsWithDimValueAndCovered(Graph<Object, Relation> graph, String dimension,
			String value, JavaSparkContext jsc) {

		// array for all current Instances and their covered ones
		ArrayList<String> coveredInstances = new ArrayList<>();
		coveredInstances.add(value);

		if (GraphGenerator.getCoveredInstances(value, jsc) != null) {
			coveredInstances.addAll(GraphGenerator.getCoveredInstances(value, jsc).collect());
		}

		ArrayList<Long> result = new ArrayList<>();
		coveredInstances.forEach(x -> {
			result.addAll(
					graph.triplets().toJavaRDD()
							.filter(triplet -> ((Resource) triplet.attr().getRelationship()).getValue()
									.contains(dimension) && triplet.dstAttr().toString().contains(x))
							.map(triplet -> triplet.srcId()).collect());
		});
		return result;
	}

	// get all Mods (as String) that belong to the Cells that have the correct
	// dimensions
	private static List<String> getMods(Graph<Object, Relation> quadGraph, List<Long> cellIds) {
		// get mods
		List<EdgeTriplet<Object, Relation>> modules = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains("hasAssertedModule")).collect();

		List<String> result = new ArrayList<>();
		modules.forEach(x -> {
			cellIds.forEach(y -> {
				if (x.srcId() == y)
					result.add(x.dstAttr().toString());
			});
		});
		return result;
	}

	// get Cells With A Certain Level
	private static List<Long> getCellsAtDimenionLevel(Graph<Object, Relation> graph, String dimension, String level) {
		ArrayList<Long> aList = new ArrayList<>();
		aList.addAll(
				graph.triplets().toJavaRDD()
						.filter(triplet -> triplet.attr().getRelationship().toString().contains("atLevel")
								&& triplet.dstAttr().toString().contains(level))
						.map(triplet -> triplet.srcId()).collect());

		List<EdgeTriplet<Object, Relation>> g = graph.triplets().toJavaRDD()
				.filter(triplet -> triplet.attr().getRelationship().toString().contains(dimension)).collect();

		List<Long> result = new ArrayList<>();
		g.forEach(x -> {
			if (aList.contains(x.dstId())) {
				result.add(x.srcId());
			}
		});
		return result;
	}

}
