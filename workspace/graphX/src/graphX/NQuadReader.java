package graphX;

import java.util.UUID;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.spark.SparkConf;
import scala.reflect.ClassTag;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.rdd.RDD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.storage.StorageLevel;

public class NQuadReader {

	@SuppressWarnings("unused")
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("NQuadReader").setMaster("local[*]")
				.set("spark.driver.cores", args[2])
				.set("spark.driver.memory", args[3])				
				.set("spark.executor.memory", args[4])
				.set("spark.executor.cores", args[5])
				.set("spark.task.cpus", args[6])
				//.set("spark.default.parallelism", args[7])
				.set("spark.memory.fraction", args[8])
				.set("spark.reducer.maxSizeInFlight", args[9])
				.set("spark.shuffle.compress", args[10])
				.set("spark.shuffle.spill.compress", args[11])
				.set("spark.speculation", args[12])
				.set("spark.dynamicAllocation.enabled", "true")
				.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
				.set("spark.eventLog.enabled", "true")
				.set("spark.eventLog.dir", args[0]+"logs")
				.set("org.apache.spark.sql.SaveMode.Overwrite", "true")
				;
		
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		org.apache.hadoop.conf.Configuration res = jsc.hadoopConfiguration();
		res.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		res.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
				
		jsc.setLogLevel("ERROR");
		ClassTag<Object> objectTag = scala.reflect.ClassTag$.MODULE$.apply(Object.class);
		ClassTag<Relation> relationTag = scala.reflect.ClassTag$.MODULE$.apply(Relation.class);

		String path = args[0];
		String fileName = args[1];

		//Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		//graph.edges().saveAsObjectFile(path + "edges" + fileName);
		//graph.vertices().saveAsObjectFile(path + "vertices" + fileName);
		
		JavaRDD<Tuple2<Object, Object>> vertices = jsc.objectFile(path + "vertices" + fileName);
		JavaRDD<Edge<Relation>> edges = jsc.objectFile(path + "edges" + fileName);
		Graph<Object, Relation> quadGraph = Graph.apply(vertices.rdd(), edges.rdd(), "",
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag, relationTag);
				
		ArrayList<String> contexts = new ArrayList<String>();
		contexts.add("urn:uuid:1d3c08d3-4c35-4610-8331-418485344f51-mod");
		contexts.add("urn:uuid:1b1f75f4-8e41-4aeb-a5dd-3b1281d43922-mod");
		contexts.add("urn:uuid:026de398-4e83-4b7b-9045-e79c010f6868-mod");
		contexts.add("urn:uuid:9c0d2075-4e2b-495f-bca6-37c0936c678e-mod");
		contexts.add("urn:uuid:81cd562d-dbf7-4ba0-bbe9-5eb102f6b1ec-mod");
		contexts.add("urn:uuid:b205a9db-52f3-473c-9a67-960366b42952-mod");
		contexts.add("urn:uuid:3545201c-bbff-4f73-8cfa-3dd0be12f02e-mod");
		contexts.add("urn:uuid:0aad99d0-bd18-4ca9-a7f0-c1473bea1446-mod");
		contexts.add("urn:uuid:1ad34d07-be5d-462f-bc36-295d44781b5b-mod");
		contexts.add("urn:uuid:c17bab3b-8377-4023-bf9d-4bb0a6662fdf-mod");
		contexts.add("urn:uuid:b6a6101a-b16d-40dd-983c-ef64219d4a4c-mod");
		contexts.add("urn:uuid:43f26998-913d-4b53-8526-7afa829c41c3-mod");
		contexts.add("urn:uuid:59b45f65-7d87-47ce-ba87-191293b40f39-mod");
		contexts.add("urn:uuid:1cc9f029-c2aa-4780-91f8-9822128fa2b1-mod");
		contexts.add("urn:uuid:90602e81-63ed-4410-ad0d-597704c05b5e-mod");
		contexts.add("urn:uuid:e81071f6-d03b-48a2-bffb-734e88cb1608-mod");
		contexts.add("urn:uuid:6de3cdf8-8f03-40a8-a106-706a365f31d5-mod");
		contexts.add("urn:uuid:f900e036-a13e-4826-8417-7eef8f1e20ee-mod");
		contexts.add("urn:uuid:5c09a880-072b-4a88-8bc0-e0cd13781a53-mod");
		contexts.add("urn:uuid:c0d7fd7b-8627-463e-8ad3-ec67faf07582-mod");
		contexts.add("urn:uuid:25970c7e-4e5a-4da6-a729-b9f57d976657-mod");
		contexts.add("urn:uuid:d108fe4b-099b-476b-8541-f79b2b46f618-mod");
		contexts.add("urn:uuid:288c411c-53d3-4b2e-a6c7-0d6cd26a1c6c-mod");
		contexts.add("urn:uuid:9bf9024e-ba0a-4934-a6e7-1717d7b3b152-mod");
		contexts.add("urn:uuid:7563a2c2-f889-4a44-84e1-faa4ca5c22de-mod");
		contexts.add("urn:uuid:a1085ae5-885b-4718-a9db-798bb904e09f-mod");
		contexts.add("urn:uuid:404600bb-01ea-4a85-8077-3072ea8ca535-mod");
		contexts.add("urn:uuid:3d832680-45d1-44eb-95b4-c5e3f4199127-mod");
		contexts.add("urn:uuid:675441fc-3ca0-47b2-a3b8-0cebc6211184-mod");
		contexts.add("urn:uuid:7abf881b-85cc-49a6-8ea9-ef4e85d9dae7-mod");

//		Graph<Object, Relation> groupByPropertyGraph = NQuadReader.groupByProperty(quadGraph, jsc, objectTag, relationTag,
//				"operationalStatus", "http://example.org/kgolap/object-model#grouping",
//				contexts);
//		groupByPropertyGraph.edges().saveAsObjectFile(path + "edges" + fileName + "groupedByProperty_"+Time.now());
//		groupByPropertyGraph.vertices().saveAsObjectFile(path + "vertices" + fileName + "groupedByProperty_"+Time.now());
		
		Graph<Object, Relation> replaceByGroupingGraph 
		= NQuadReader.replaceByGrouping(quadGraph, jsc, objectTag, relationTag,"ManoeuvringAreaUsage", "usageType", contexts);
		replaceByGroupingGraph.edges().saveAsObjectFile(path + "edges" + fileName + "replacedByGrouping_"+Time.now());
		replaceByGroupingGraph.vertices().saveAsObjectFile(path + "vertices" + fileName + "replacedByGrouping_"+Time.now());
		
//		Graph<Object, Relation> reifiedGraph = reify(quadGraph, jsc, objectTag, relationTag,
//				contexts, "http://example.org/kgolap/object-model#usage",
//		"http://example.org/kgolap/object-model#usage-type", "http://www.w3.org/1999/02/22-rdf-syntax-ns#object", 
//		"http://www.w3.org/1999/02/22-rdf-syntax-ns#subject");
//		reifiedGraph.edges().saveAsObjectFile(path + "edges" + fileName + "reified_"+Time.now());
//		reifiedGraph.vertices().saveAsObjectFile(path + "vertices" + fileName + "reified_"+Time.now());
		
//		Graph<Object, Relation> pivotGraph = NQuadReader.pivot(quadGraph, jsc, objectTag, relationTag, "hasLocation",
//				"http://example.org/kgolap/object-model#hasLocation", "type",
//				"http://example.org/kgolap/object-model#ManoeuvringAreaAvailability",
//				contexts);
//		pivotGraph.edges().saveAsObjectFile(path + "edges" + fileName + "pivt_"+Time.now());
//		pivotGraph.vertices().saveAsObjectFile(path + "vertices" + fileName + "pivt_"+Time.now());


//		Graph<Object, Relation> aggregatedGraph = NQuadReader.aggregatePropertyValues(quadGraph, jsc, objectTag, relationTag, 
//				 "http://example.org/kgolap/object-model#wingspan", contexts, "SUM");
//		aggregatedGraph.edges().saveAsObjectFile(path + "edges" + fileName + "aggregated_"+Time.now());
//		aggregatedGraph.vertices().saveAsObjectFile(path + "vertices" + fileName + "aggregated_"+Time.now());
//
		jsc.stop();	
		
	}
			

	public static Graph<Object, Relation> reify(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, ArrayList<String> contexts, String reificationPredicate,
			String type, String object, String subject) {
		Broadcast<ArrayList<String>> broadcastContexts = jsc.broadcast(contexts);
		
		JavaRDD<EdgeTriplet<Object, Relation>> statements = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains(reificationPredicate)
						&&  broadcastContexts.value().contains(x.attr().getContext().toString())
						);

		Long typeID = getIdOfObject(type);
		List<Tuple2<Object, Object>> typeV = new ArrayList<Tuple2<Object, Object>>();
		typeV.add(new Tuple2<Object, Object>(typeID, type));
		JavaRDD<Tuple2<Object, Object>> typeVertice = jsc.parallelize(typeV);
		
		
		//Relation objectRelation = new Relation(object, context, "Resource");
		//Relation subjectRelation = new Relation(subject, context, "Resource");
		//Relation typeRelation = new Relation(type, context, "Resource");

		// new Vertices
		JavaRDD<Tuple3<Object, Object, Object>> statementTuple3 = statements
				.map(x -> new Tuple3<Object, Object, Object>(x.srcId(), x.dstId(),
						new Resource("urn:uuid:" + UUID.randomUUID())))
				.persist(StorageLevel.MEMORY_ONLY());
		// new Vertices + mod
		JavaRDD<Tuple4<Object, Object, Object, Object>> statementTuple4 = statements
				.map(x -> new Tuple4<Object, Object, Object, Object>(x.srcId(), x.dstId(),
						new Resource("urn:uuid:" + UUID.randomUUID()), x.attr().getContext().toString()))
				.persist(StorageLevel.MEMORY_ONLY());
		//statementTuple41.foreach(x -> System.out.println(x._1() + " " + x._2() + " " + x._3() + " " + x._4()));
		
		// 1 = subject, 2 = object, 3 ID of new one, 4 = new Resource, 5 = context/mod
		JavaRDD<Tuple5<Object, Object, Object, Object, Object>> statementTuple5 = statementTuple4
				.map(x -> new Tuple5<Object, Object, Object, Object, Object>(x._1(), x._2(),
						getIdOfObject(((Resource) x._3()).getValue()), (Resource) x._3(), x._4()));
		
		// 1 = subject, 2 = object, 3 ID of new one, 4 = new Resource
		//JavaRDD<Tuple4<Object, Object, Object, Object>> statementTuple4 = statementTuple3
			//	.map(x -> new Tuple4<Object, Object, Object, Object>(x._1(), x._2(),
				//		getIdOfObject(((Resource) x._3()).getValue()), (Resource) x._3()));

		JavaRDD<Tuple2<Object, Object>> newVertices = statementTuple4
				.map(x -> new Tuple2<Object, Object>(getIdOfObject(((Resource) x._3()).getValue()), (Resource) x._3()));
		
		//JavaRDD<Tuple2<Object, Object>> newVertices = statementTuple3
			//	.map(x -> new Tuple2<Object, Object>(getIdOfObject(((Resource) x._3()).getValue()), (Resource) x._3()));

		// subject Edges
		JavaRDD<Edge<Relation>> subjectEdges = statementTuple5
				.map(x -> new Edge<Relation>((long) x._3(), (long) x._1(), new Relation(subject, x._5().toString(), "Resource")));
		// object Edges
		JavaRDD<Edge<Relation>> objectEdges = statementTuple5
				.map(x -> new Edge<Relation>((long) x._3(), (long) x._2(), new Relation(object, x._5().toString(), "Resource")));
		// type Edges
		JavaRDD<Edge<Relation>> typeEdges = statementTuple5
				.map(x -> new Edge<Relation>((long) x._3(), typeID, new Relation(type, x._5().toString(), "Resource")));

		// combining old edges and vertices with new ones
		JavaRDD<Edge<Relation>> allEdges = quadGraph.edges().toJavaRDD().union(subjectEdges).union(objectEdges)
				.union(typeEdges);
		JavaRDD<Tuple2<Object, Object>> allVertices = quadGraph.vertices().toJavaRDD().union(typeVertice)
				.union(newVertices);

		// create new graph with added vertices and edges
		Graph<Object, Relation> graph = Graph.apply(allVertices.rdd(), allEdges.rdd(), "", StorageLevel.MEMORY_ONLY(),
				StorageLevel.MEMORY_ONLY(), objectTag, relationTag);
		
		return graph;
	}


	public static Graph<Object, Relation> pivot(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String dimensionProperty, String pivotProperty,
			String type, String selectionCondition, ArrayList<String> context) {
		Broadcast<ArrayList<String>> broadcastedContexts = jsc.broadcast(context);
		
		Long dimPropertyVertice = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString()
						.contains(dimensionProperty)).map(x -> x.dstId()).first();

		JavaRDD<Edge<Relation>> newEdges = quadGraph.triplets().toJavaRDD()
				.filter(x -> broadcastedContexts.value().contains(x.attr().getContext().toString())
						&& x.attr().getRelationship().toString().contains(type)
						&& x.dstAttr().toString().contains(selectionCondition))
				.map(x -> new Edge<Relation>(x.srcId(), dimPropertyVertice,
						new Relation(pivotProperty, context, "Resource")));

		RDD<Edge<Relation>> allEdges = quadGraph.edges().union(newEdges.rdd());

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices(), allEdges, "", StorageLevel.MEMORY_ONLY(),
				StorageLevel.MEMORY_ONLY(), objectTag, relationTag);
		return graph;
	}

	//value generating
	public static Graph<Object, Relation> aggregatePropertyValues(Graph<Object, Relation> quadGraph,
			JavaSparkContext jsc, ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String aggregateProperty,
			ArrayList<String> contexts, String aggregateType) {
		Broadcast<ArrayList<String>> broadcastContexts = jsc.broadcast(contexts);
		
		JavaRDD<Tuple2<Tuple2<Long, Object>, Long>> verticesRDD = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().equals(aggregateProperty)
						&& broadcastContexts.value().contains(x.attr().getContext().toString()))
				.map(x -> new Tuple2<Tuple2<Long, Object>, Long>(new Tuple2<Long, Object>((Long) x.srcId(), x.attr().getContext().toString()), Long.parseLong(x.dstAttr().toString())));
		System.out.println(verticesRDD.count());
		
		JavaPairRDD<Tuple2<Long, Object>, Long> verticesPair = JavaPairRDD.fromJavaRDD(verticesRDD);	
		
		JavaRDD<Tuple2<Object, Object>> newVertice = null;
		JavaRDD<Edge<Relation>> newEdges = null;

		if (aggregateType.contains("AVG")) { // done
			newEdges = verticesPair.mapValues(x -> new Tuple2<Long, Long>(x, 1L))
					.reduceByKey((x, y) -> new Tuple2<Long, Long>(x._1 + y._1, x._2 + y._2)).mapValues(x -> x._1 / x._2)
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
							new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));

			newVertice = verticesPair.mapValues(x -> new Tuple2<Long, Long>(x, 1L))
					.reduceByKey((x, y) -> new Tuple2<Long, Long>(x._1 + y._1, x._2 + y._2)).mapValues(x -> x._1 / x._2)
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}

		if (aggregateType.contains("COUNT")) {
			newEdges = verticesPair.mapValues(x -> 1L).reduceByKey((x, y) -> x + y)
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
							new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));

			newVertice = verticesPair.mapValues(x -> 1L).reduceByKey((x, y) -> x + y)
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}
		if (aggregateType.contains("SUM")) {
			newEdges = verticesPair.reduceByKey((a, b) -> a + b)
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
					new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));
			//newEdges.foreach(x -> System.out.println(x.attr().getRelationship()));
			newVertice = verticesPair.reduceByKey((a, b) -> a + b)
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));

		}
		if (aggregateType.contains("MAX")) {
			newEdges = verticesPair.reduceByKey((a, b) -> Math.max(a, b))
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
							new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));

			newVertice = verticesPair.reduceByKey((a, b) -> Math.max(a, b))
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}
		if (aggregateType.contains("MIN")) {
			newEdges = verticesPair.reduceByKey((a, b) -> Math.min(a, b))
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
							new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));

			newVertice = verticesPair.reduceByKey((a, b) -> Math.min(a, b))
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}

		JavaRDD<Edge<Relation>> keepTriplets = quadGraph.triplets().toJavaRDD()
				.filter(x -> !x.attr().getRelationship().toString().equals(aggregateProperty)
						|| !broadcastContexts.value().contains(x.attr().getContext().toString())
						)
				.map(x -> new Edge<Relation>(x.srcId(), x.dstId(), x.attr()));

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices().toJavaRDD().union(newVertice).rdd(),
				keepTriplets.union(newEdges).rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
				objectTag, relationTag);
		return graph;
	}

	//individual generating
	public static Graph<Object, Relation> groupByProperty(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String groupingProperty,
			String groupingPredicate, ArrayList<String> contexts) {

		Broadcast<ArrayList<String>> broadcastContexts = jsc.broadcast(contexts);
		
		JavaRDD<EdgeTriplet<Object, Relation>> filteredEdges = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains(groupingProperty)
						&& broadcastContexts.value().contains(x.attr().getContext().toString()));
		
		JavaRDD<Tuple2<Object, Object>> newVertices = filteredEdges
				.map(x -> new Tuple2<Object, Object>(getIdOfObject(x.dstAttr() + "-Group"), x.dstAttr() + "-Group"));

		//Relation newRelation = new Relation(groupingPredicate, mod, "Resource");

		// new additional grouping edges
		JavaRDD<Edge<Relation>> groupingEdges = filteredEdges
				.map(x -> new Tuple3<Object, Object, Object>(x.srcId(), x.dstAttr() + "-Group", x.attr().getContext().toString()))
				.map(x -> new Edge<Relation>((long) x._1(), getIdOfObject(x._2()), new Relation(groupingPredicate, x._3().toString(), "Resource")));
		
		HashMap<Object, Object> hashmap = new HashMap<Object, Object>();

		// add mappings from objects to be replaced and group that should replace it to
		// hashmap
		filteredEdges.map(x -> new Tuple2<Object, Object>(x.srcId(), x.dstAttr() + "-Group")).collect()
				.forEach(x -> hashmap.put(x._1, getIdOfObject(x._2)));

		Broadcast<HashMap<Object, Object>> groupMapping = jsc.broadcast(hashmap);
		// replace all relevant objects with group
		JavaRDD<Edge<Relation>> replacedEdges = quadGraph.triplets().toJavaRDD()
			.filter(x -> broadcastContexts.value().contains(x.attr().getContext().toString()))
			.map(x -> {
			if (groupMapping.value().containsKey(x.srcId())
					&& !x.attr().getRelationship().toString().contains(groupingProperty) && !x.attr().getRelationship()
							.toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
				return new Edge<Relation>((long) groupMapping.value().get(x.srcId()), x.dstId(), x.attr());
			}
			if (groupMapping.value().containsKey(x.dstId())
					&& !x.attr().getRelationship().toString().contains(groupingProperty) && !x.attr().getRelationship()
							.toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
				return new Edge<Relation>(x.srcId(), (long) groupMapping.value().get(x.dstId()), x.attr());
			} else {
				return new Edge<Relation>(x.srcId(), x.dstId(), x.attr());
			}
		});

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices().union(newVertices.rdd()),
				replacedEdges.union(groupingEdges).rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
				objectTag, relationTag);
		
		return graph;

	}

	public static Graph<Object, Relation> replaceByGrouping(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String replacementObject,
			String groupingValue, ArrayList<String> contexts) {
		Broadcast<ArrayList<String>> broadcastContexts = jsc.broadcast(contexts);
		// get instances of the type (subjects) that has to be replaced (that hace
		// "toBeReplaced" as object)

		//ManoueveringAreaUsage
		List<Long> subjectsToBeReplaced = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.dstAttr().toString().contains(replacementObject)
						&& broadcastContexts.value().contains(x.attr().getContext().toString()))
				.map(x -> x.srcId())
				.collect();
		
		Broadcast<List<Long>> broadcastSubjectsToBeReplaced = jsc.broadcast(subjectsToBeReplaced);
		HashMap<Object, Object> hashmap = new HashMap<Object, Object>();

		//"usageType"
		JavaRDD<Edge<Relation>> groups = quadGraph.edges().toJavaRDD()
				.filter(x -> broadcastSubjectsToBeReplaced.value().contains(x.srcId())
						&& x.attr().getRelationship().toString().contains(groupingValue)
						);

		// put mapping into hashmap from tobeplaced and replacement
		groups.collect().forEach(x -> hashmap.put(x.srcId(), x.dstId()));
		Broadcast<HashMap<Object, Object>> groupMapping = jsc.broadcast(hashmap);

		// replace subjets/objects with their grouping
		JavaRDD<Edge<Relation>> replacedEdges = quadGraph.triplets().toJavaRDD()
				.filter(x -> broadcastContexts.value().contains(x.attr().getContext().toString()))
				.map(x -> {
			if (groupMapping.value().containsKey(x.srcId())
					&& !x.attr().getRelationship().toString()
							.contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
					&& !x.attr().getRelationship().toString().contains(groupingValue)) {
				return new Edge<Relation>((long) groupMapping.value().get(x.srcId()), x.dstId(), x.attr());
			}
			if (groupMapping.value().containsKey(x.dstId())
					&& !x.attr().getRelationship().toString()
							.contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
					&& !x.attr().getRelationship().toString().contains(groupingValue)) {
				return new Edge<Relation>(x.srcId(), (long) groupMapping.value().get(x.dstId()), x.attr());
			} else {
				return new Edge<Relation>(x.srcId(), x.dstId(), x.attr());
			}
		});
		
		JavaRDD<Edge<Relation>> keepEdges = quadGraph.triplets().toJavaRDD()
				.filter(x -> !broadcastContexts.value().contains(x.attr().getContext().toString()))
				.map(x -> new Edge<Relation>(x.srcId(), x.dstId(), x.attr()));

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices(), replacedEdges.union(keepEdges).rdd(), "",
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag, relationTag);

		return graph;
	}

	public static Graph<Object, Relation> merge(Graph<Object, Relation> graph, JavaSparkContext jsc,
			String levelImportance, String levelAircraft, String levelLocation, String levelDate,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag) {
		JavaRDD<Tuple2<Object, Object>> importance = getCellsAtDimensionLevel(graph, "hasImportance", levelImportance,
				jsc);
		JavaRDD<Tuple2<Object, Object>> aircraft = getCellsAtDimensionLevel(graph, "hasAircraft", levelAircraft, jsc);
		JavaRDD<Tuple2<Object, Object>> location = getCellsAtDimensionLevel(graph, "hasLocation", levelLocation, jsc);
		JavaRDD<Tuple2<Object, Object>> date = getCellsAtDimensionLevel(graph, "hasDate", levelDate, jsc);
		// check which cells satisfy all four dimensions
		JavaRDD<Tuple2<Object, Object>> result = importance.intersection(aircraft).intersection(location)
				.intersection(date);

		HashMap<String, String> resultHashMap = GraphGenerator
				.getCoverageHashMap(result.map(x -> x._2.toString()).collect());
		Broadcast<HashMap<String, String>> broadCastResult = jsc.broadcast(resultHashMap);

		JavaRDD<Edge<Relation>> allEdges = graph.edges().toJavaRDD()
				.map(x -> {
					if(broadCastResult.value().containsKey(x.attr().getContext().toString())){
						return new Edge<Relation>(x.srcId(), x.dstId(),
								x.attr().updateContext(broadCastResult.value().get(x.attr().getContext().toString())));
					} else {
						return new Edge<Relation>(x.srcId(), x.dstId(), x.attr());
					}
					});

		Graph<Object, Relation> resultGraph = Graph.apply(graph.vertices(), allEdges.rdd(), "",
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag, relationTag);

		return resultGraph;
	}

	public static Graph<Object, Relation> sliceDice(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String importanceValue, String aircraftValue,
			String locationValue, String dateValue) {

		if (importanceValue == "Level_Importance_All-All" && aircraftValue == "Level_Aircraft_All-All"
				&& locationValue == "Level_Location_All-All" && dateValue == "Level_Date_All-All") {
			return quadGraph;
		}

		JavaRDD<Tuple2<Object, Object>> cells = getCellsWithDimValues(quadGraph, jsc, importanceValue, aircraftValue,
				locationValue, dateValue);

		JavaRDD<String> mods = cells.map(x -> x._2 + "-mod");

		Broadcast<List<String>> broadcastMods = jsc.broadcast(mods.collect());
		JavaRDD<Edge<Relation>> result = quadGraph.triplets().toJavaRDD()
				.filter(x -> broadcastMods.value().contains(x.attr().getContext().toString())
						|| x.attr().getContext().toString().contains("global"))
				.map(x -> new Edge<Relation>(x.srcId(), x.dstId(), x.attr()));

		Graph<Object, Relation> resultGraph = Graph.apply(quadGraph.vertices(), result.rdd(), "",
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag, relationTag);

		return resultGraph;
	}

	private static JavaRDD<Tuple2<Object, Object>> getCellsWithDimValues(Graph<Object, Relation> graph,
			JavaSparkContext jsc, String importanceValue, String aircraftValue, String locationValue,
			String dateValue) {
		// get cells for each dimension individually
		JavaRDD<Tuple2<Object, Object>> importance = getCellsWithDimValueAndCovered(graph, "hasImportance",
				importanceValue, jsc);
		JavaRDD<Tuple2<Object, Object>> aircraft = getCellsWithDimValueAndCovered(graph, "hasAircraft", aircraftValue,
				jsc);
		JavaRDD<Tuple2<Object, Object>> location = getCellsWithDimValueAndCovered(graph, "hasLocation", locationValue,
				jsc);
		JavaRDD<Tuple2<Object, Object>> date = getCellsWithDimValueAndCovered(graph, "hasDate", dateValue, jsc);

		JavaRDD<Tuple2<Object, Object>> result = importance.intersection(aircraft).intersection(location)
				.intersection(date);

		return result;
	}

	private static JavaRDD<Tuple2<Object, Object>> getCellsWithDimValueAndCovered(Graph<Object, Relation> graph,
			String dimension, String value, JavaSparkContext jsc) {

		// array for all current Instances and their covered ones
		ArrayList<String> instances = new ArrayList<>();
		instances.add(value);
		JavaRDD<String> allInstances = jsc.parallelize(instances);

		JavaRDD<String> covered = GraphGenerator.getCoveredInstances(value, jsc);
		if (covered != null) {
			allInstances = allInstances.union(covered);
		}

		Broadcast<List<String>> broadcastInstances = jsc.broadcast(allInstances.collect());

		JavaRDD<Tuple2<Object, Object>> result = graph.triplets().toJavaRDD()
				.filter(triplet -> triplet.attr().getRelationship().toString().contains(dimension)
						&& triplet.dstAttr().toString().length() >= 45
						&& broadcastInstances.getValue().contains(triplet.dstAttr().toString().subSequence(33, 45)))
				.map(triplet -> new Tuple2<Object, Object>(triplet.srcId(), triplet.srcAttr()));
		return result;
	}

	// get Cells With A Certain Level
	private static JavaRDD<Tuple2<Object, Object>> getCellsAtDimensionLevel(Graph<Object, Relation> graph,
			String dimension, String level, JavaSparkContext jsc) {

		// https://stackoverflow.com/questions/26214112/filter-based-on-another-rdd-in-spark
		// get the objects that are at the correct level
		JavaRDD<Long> atLevelSubjects = graph.triplets().toJavaRDD()
				.filter(triplet -> triplet.attr().getRelationship().toString().contains("atLevel")
						&& triplet.dstAttr().toString().contains(level))
				.map(triplet -> triplet.srcId());

		Broadcast<List<Long>> broadcastSubjects = jsc.broadcast(atLevelSubjects.collect());

		// get the cells that have those objects as dimension values that are at the
		// correct level
		JavaRDD<Tuple2<Object, Object>> result = graph.triplets().toJavaRDD()
				.filter(x -> broadcastSubjects.getValue().contains(x.dstId())
						&& x.attr().getRelationship().toString().contains(dimension))
				.map(x -> new Tuple2<Object, Object>(x.srcId(), x.srcAttr()));

		return result;
	}

	public static long getIdOfObject(Object s) {
		return UUID.nameUUIDFromBytes(s.toString().getBytes()).getMostSignificantBits();
	}
}