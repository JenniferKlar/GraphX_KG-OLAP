package graphX;

import java.util.UUID;

import org.apache.hadoop.util.Time;
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

import org.apache.spark.storage.StorageLevel;

public class NQuadReader2 {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws InterruptedException {
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
		
		JavaRDD<Tuple2<Object, Object>> vertices = jsc.objectFile(path + "vertices_noType" + fileName);
		JavaRDD<Edge<Relation>> edges = jsc.objectFile(path + "edges_noType" + fileName);
		Graph<Object, Relation> quadGraph = Graph.apply(vertices.rdd(), edges.rdd(), "",
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag, relationTag);
//		ArrayList<String> contexts = new ArrayList<String>();
//		contexts.add("urn:uuid:1d3c08d3-4c35-4610-8331-418485344f51-mod");
//		contexts.add("urn:uuid:1b1f75f4-8e41-4aeb-a5dd-3b1281d43922-mod");
//		contexts.add("urn:uuid:026de398-4e83-4b7b-9045-e79c010f6868-mod");
//		contexts.add("urn:uuid:9c0d2075-4e2b-495f-bca6-37c0936c678e-mod");
//		contexts.add("urn:uuid:81cd562d-dbf7-4ba0-bbe9-5eb102f6b1ec-mod");
//		contexts.add("urn:uuid:b205a9db-52f3-473c-9a67-960366b42952-mod");
//		contexts.add("urn:uuid:3545201c-bbff-4f73-8cfa-3dd0be12f02e-mod");
//		contexts.add("urn:uuid:0aad99d0-bd18-4ca9-a7f0-c1473bea1446-mod");
//		contexts.add("urn:uuid:1ad34d07-be5d-462f-bc36-295d44781b5b-mod");
//		contexts.add("urn:uuid:c17bab3b-8377-4023-bf9d-4bb0a6662fdf-mod");
//		contexts.add("urn:uuid:b6a6101a-b16d-40dd-983c-ef64219d4a4c-mod");
//		contexts.add("urn:uuid:43f26998-913d-4b53-8526-7afa829c41c3-mod");
//		contexts.add("urn:uuid:59b45f65-7d87-47ce-ba87-191293b40f39-mod");
//		contexts.add("urn:uuid:1cc9f029-c2aa-4780-91f8-9822128fa2b1-mod");
//		contexts.add("urn:uuid:90602e81-63ed-4410-ad0d-597704c05b5e-mod");
//		contexts.add("urn:uuid:e81071f6-d03b-48a2-bffb-734e88cb1608-mod");
//		contexts.add("urn:uuid:6de3cdf8-8f03-40a8-a106-706a365f31d5-mod");
//		contexts.add("urn:uuid:f900e036-a13e-4826-8417-7eef8f1e20ee-mod");
//		contexts.add("urn:uuid:5c09a880-072b-4a88-8bc0-e0cd13781a53-mod");
//		contexts.add("urn:uuid:c0d7fd7b-8627-463e-8ad3-ec67faf07582-mod");
//		contexts.add("urn:uuid:25970c7e-4e5a-4da6-a729-b9f57d976657-mod");
//		contexts.add("urn:uuid:d108fe4b-099b-476b-8541-f79b2b46f618-mod");
//		contexts.add("urn:uuid:288c411c-53d3-4b2e-a6c7-0d6cd26a1c6c-mod");
//		contexts.add("urn:uuid:9bf9024e-ba0a-4934-a6e7-1717d7b3b152-mod");
//		contexts.add("urn:uuid:7563a2c2-f889-4a44-84e1-faa4ca5c22de-mod");
//		contexts.add("urn:uuid:a1085ae5-885b-4718-a9db-798bb904e09f-mod");
//		contexts.add("urn:uuid:404600bb-01ea-4a85-8077-3072ea8ca535-mod");
//		contexts.add("urn:uuid:3d832680-45d1-44eb-95b4-c5e3f4199127-mod");
//		contexts.add("urn:uuid:675441fc-3ca0-47b2-a3b8-0cebc6211184-mod");
//		contexts.add("urn:uuid:7abf881b-85cc-49a6-8ea9-ef4e85d9dae7-mod");
//		contexts.add("urn:uuid:a359cd57-b520-414c-9547-1830abb2aa7c-mod");
//		contexts.add("urn:uuid:0aad99d0-bd18-4ca9-a7f0-c1473bea1446-mod");
//		contexts.add("urn:uuid:1ad34d07-be5d-462f-bc36-295d44781b5b-mod");
//		contexts.add("urn:uuid:c17bab3b-8377-4023-bf9d-4bb0a6662fdf-mod");
//		contexts.add("urn:uuid:b6a6101a-b16d-40dd-983c-ef64219d4a4c-mod");
//		contexts.add("urn:uuid:43f26998-913d-4b53-8526-7afa829c41c3-mod");
//		contexts.add("urn:uuid:1cc9f029-c2aa-4780-91f8-9822128fa2b1-mod");
//		contexts.add("urn:uuid:90602e81-63ed-4410-ad0d-597704c05b5e-mod");
//		contexts.add("urn:uuid:e81071f6-d03b-48a2-bffb-734e88cb1608-mod");
//		contexts.add("urn:uuid:77873260-9ec9-4ba3-bc79-569f81e2d3e4-mod");
//		contexts.add("urn:uuid:7b3a6909-0a17-47a6-ad2e-b56e13b0d8d9-mod");
//		contexts.add("urn:uuid:5c99a755-288b-41bf-b621-5fa616d6e03f-mod");
//		contexts.add("urn:uuid:7ab0e272-da5b-408d-a632-fb5d3132ebfb-mod");
//		contexts.add("urn:uuid:c60e474a-0c16-46ec-b0dc-9cdab36d4b11-mod");
//		contexts.add("urn:uuid:dce1e8c0-af6d-42ba-9b07-6769016ad07c-mod");
//		contexts.add("urn:uuid:886c7e93-b6de-47d4-a422-be15c0444492-mod");
//		contexts.add("urn:uuid:5f0d76f0-0f7e-4c3a-b44b-8fe097b3c274-mod");
//		contexts.add("urn:uuid:d49d99e6-303d-4793-8eb3-a245302cd60b-mod");
//		contexts.add("urn:uuid:035d7d3f-2cfa-46ab-aff8-b2af649b6a55-mod");
//		contexts.add("urn:uuid:61ffe436-8a3a-4c79-a185-765433790d8a-mod");
//		contexts.add("urn:uuid:9bead253-e2c8-4f09-bcb8-4834620beedc-mod");
//		contexts.add("urn:uuid:add2b1b9-d822-4ad0-b9c7-7a1ed2870988-mod");
//		contexts.add("urn:uuid:9a8ab33f-262a-469e-a3c7-cb0817bb8a55-mod");
//		contexts.add("urn:uuid:4ca2eec2-dfb6-4b80-b691-bead3786b0af-mod");
//		contexts.add("urn:uuid:122953af-8aec-4d00-b47d-d5c63442f6f6-mod");
//		contexts.add("urn:uuid:f96f7375-3c7d-4b77-8ef2-6ad4668550ae-mod");
//		contexts.add("urn:uuid:1ba33f65-266a-4a41-a6d0-ba400c4b35b7-mod");
//		contexts.add("urn:uuid:481f05b5-de8d-47da-83e3-5821014a4a3c-mod");
//		contexts.add("urn:uuid:4cf620d9-fa38-48ae-9955-49de7d75ac32-mod");
//		contexts.add("urn:uuid:fb0ecb81-3285-4f79-9b63-04b9a0d82ff3-mod");

//		Graph<Object, Relation> groupByPropertyGraph = NQuadReader2.groupByProperty(quadGraph, jsc, objectTag, relationTag,
//				"operationalStatus", "http://example.org/kgolap/object-model#grouping", contexts);
//		groupByPropertyGraph.edges().saveAsObjectFile(path + "edges" + fileName + "groupedByProperty_"+Time.now());
//		groupByPropertyGraph.vertices().saveAsObjectFile(path + "vertices" + fileName + "groupedByProperty_"+Time.now());
		
//		Graph<Object, Relation> replaceByGroupingGraph 
//		= NQuadReader2.replaceByGrouping(quadGraph, jsc, objectTag, relationTag,"ManoeuvringAreaUsage", "usageType", contexts);
//		replaceByGroupingGraph.edges().saveAsObjectFile(path + "edges" + fileName + "replacedByGrouping_"+Time.now());
//		replaceByGroupingGraph.vertices().saveAsObjectFile(path + "vertices" + fileName + "replacedByGrouping_"+Time.now());
//		
//		Graph<Object, Relation> reifiedGraph = NQuadReader2.reify(quadGraph, jsc, objectTag, relationTag,
//				"http://example.org/kgolap/object-model#usage",
//		"http://example.org/kgolap/object-model#usage-type", "http://www.w3.org/1999/02/22-rdf-syntax-ns#object", 
//		"http://www.w3.org/1999/02/22-rdf-syntax-ns#subject", contexts);
//		reifiedGraph.edges().saveAsObjectFile(path + "edges" + fileName + "reified_"+Time.now());
//		reifiedGraph.vertices().saveAsObjectFile(path + "vertices" + fileName + "reified_"+Time.now());
//		
//		Graph<Object, Relation> pivotGraph = NQuadReader2.pivot(quadGraph, jsc, objectTag, relationTag, "hasLocation",
//				"http://example.org/kgolap/object-model#hasLocation",
//				"http://example.org/kgolap/object-model#ManoeuvringAreaAvailability", contexts);
//		pivotGraph.edges().saveAsObjectFile(path + "edges" + fileName + "pivt_"+Time.now());
//		pivotGraph.vertices().saveAsObjectFile(path + "vertices" + fileName + "pivt_"+Time.now());
//
//
//		Graph<Object, Relation> aggregatedGraph = NQuadReader2.aggregatePropertyValues(quadGraph, jsc, objectTag, relationTag, 
//				 "http://example.org/kgolap/object-model#wingspan", "SUM", contexts);
		
		
		quadGraph.edges().saveAsObjectFile(path + "edges" + fileName + "original_"+Time.now());
		quadGraph.vertices().saveAsObjectFile(path + "vertices" + fileName + "orginal_"+Time.now());
//
		jsc.stop();	
		
	}
			

	public static Graph<Object, Relation> reify(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String reificationPredicate,
			String type, String object, String subject) {
		
		JavaRDD<EdgeTriplet<Object, Relation>> statements = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains(reificationPredicate)
						);

		Long typeID = getIdOfObject(type);
		List<Tuple2<Object, Object>> typeV = new ArrayList<Tuple2<Object, Object>>();
		typeV.add(new Tuple2<Object, Object>(typeID, type));
		JavaRDD<Tuple2<Object, Object>> typeVertice = jsc.parallelize(typeV);
		
		// new Vertices + mod
		JavaRDD<Tuple4<Object, Object, Object, Object>> statementTuple4 = statements
				.map(x -> new Tuple4<Object, Object, Object, Object>(x.srcId(), x.dstId(),
						new Resource("urn:uuid:" + UUID.randomUUID()), x.attr().getContext().toString()))
				.persist(StorageLevel.MEMORY_ONLY());
		
		// 1 = subject, 2 = object, 3 ID of new one, 4 = new Resource, 5 = context/mod
		JavaRDD<Tuple5<Object, Object, Object, Object, Object>> statementTuple5 = statementTuple4
				.map(x -> new Tuple5<Object, Object, Object, Object, Object>(x._1(), x._2(),
						getIdOfObject(((Resource) x._3()).getValue()), (Resource) x._3(), x._4()));
		
		JavaRDD<Tuple2<Object, Object>> newVertices = statementTuple4
				.map(x -> new Tuple2<Object, Object>(getIdOfObject(((Resource) x._3()).getValue()), (Resource) x._3()));

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
			String selectionCondition, ArrayList<String> context) {
		
		Broadcast<ArrayList<String>> broadcastedContexts = jsc.broadcast(context);
		HashMap<Object, Object> hashmap = new HashMap<Object, Object>();
		
		JavaRDD<Tuple2<Object, Object>> modObjectMapping = quadGraph.triplets().toJavaRDD()
		.filter(x -> x.attr().getRelationship().toString().contains(dimensionProperty)
				&& 
				broadcastedContexts.value().contains(x.srcAttr()+"-mod".toString())
				)
		.map(x -> new Tuple2<Object, Object>(x.srcAttr()+"-mod", x.dstId()));
		modObjectMapping.collect().forEach(x -> hashmap.put(x._1(), x._2()));	
		Broadcast<HashMap<Object, Object>> mapping = jsc.broadcast(hashmap);

		JavaRDD<Edge<Relation>> newEdges = quadGraph.triplets().toJavaRDD()
				.filter(x -> 
				broadcastedContexts.value().contains(x.attr().getContext().toString())
						&& ((Resource) x.srcAttr()).getType().contains(selectionCondition))
				.map(x -> new Tuple2<Object, Object> (x.srcId(), x.attr().getContext())).distinct()
				.map(x -> new Edge<Relation>((long) x._1(), (long) mapping.value().get(x._2()),
						new Relation(pivotProperty, x._2.toString(), "Resource")));
		
		broadcastedContexts.value().forEach(x -> System.out.println(x));						
		RDD<Edge<Relation>> allEdges = quadGraph.edges().union(newEdges.rdd());

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices(), allEdges, "", StorageLevel.MEMORY_ONLY(),
				StorageLevel.MEMORY_ONLY(), objectTag, relationTag);
		return graph;
	}

	//value generating
	public static Graph<Object, Relation> aggregatePropertyValues(Graph<Object, Relation> quadGraph,
			JavaSparkContext jsc, ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String aggregateProperty,
			String aggregateType) {
		
		JavaPairRDD<Tuple2<Long, Object>, Long> verticesPair = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().equals(aggregateProperty))
				.mapToPair(x -> new Tuple2<Tuple2<Long, Object>, Long>(new Tuple2<Long, Object>((Long) x.srcId(), x.attr().getContext().toString()), Long.parseLong(x.dstAttr().toString())));
		verticesPair.foreach(x -> System.out.println(x));
		
		JavaRDD<Tuple2<Object, Object>> newVertices = null;
		JavaRDD<Edge<Relation>> newEdges = null;

		if (aggregateType.contains("AVG")) { // done
			newEdges = verticesPair.mapValues(x -> new Tuple2<Long, Long>(x, 1L))
					.reduceByKey((x, y) -> new Tuple2<Long, Long>(x._1 + y._1, x._2 + y._2)).mapValues(x -> x._1 / x._2)
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
							new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));

			newVertices = verticesPair.mapValues(x -> new Tuple2<Long, Long>(x, 1L))
					.reduceByKey((x, y) -> new Tuple2<Long, Long>(x._1 + y._1, x._2 + y._2)).mapValues(x -> x._1 / x._2)
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}

		if (aggregateType.contains("COUNT")) {
			newEdges = verticesPair.mapValues(x -> 1L).reduceByKey((x, y) -> x + y)
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
							new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));

			newVertices = verticesPair.mapValues(x -> 1L).reduceByKey((x, y) -> x + y)
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}
		if (aggregateType.contains("SUM")) {
			newEdges = verticesPair.reduceByKey((a, b) -> a + b)
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
					new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));
			newVertices = verticesPair.reduceByKey((a, b) -> a + b)
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));

		}
		if (aggregateType.contains("MAX")) {
			newEdges = verticesPair.reduceByKey((a, b) -> Math.max(a, b))
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
							new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));

			newVertices = verticesPair.reduceByKey((a, b) -> Math.max(a, b))
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}
		if (aggregateType.contains("MIN")) {
			newEdges = verticesPair.reduceByKey((a, b) -> Math.min(a, b))
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
							new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));

			newVertices = verticesPair.reduceByKey((a, b) -> Math.min(a, b))
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}

		JavaRDD<Edge<Relation>> keepTriplets = quadGraph.triplets().toJavaRDD()
				.filter(x -> !x.attr().getRelationship().toString().equals(aggregateProperty))
				.map(x -> new Edge<Relation>(x.srcId(), x.dstId(), x.attr()));

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices().toJavaRDD().union(newVertices).rdd(),
				keepTriplets.union(newEdges).rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
				objectTag, relationTag);
		return graph;
	}

	//individual generating
	public static Graph<Object, Relation> groupByProperty(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String groupingProperty,
			String groupingPredicate) {
		
		JavaRDD<EdgeTriplet<Object, Relation>> filteredEdges = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains(groupingProperty));
		
		JavaRDD<Tuple2<Object, Object>> newVertices = filteredEdges
				.map(x -> new Tuple2<Object, Object>(getIdOfObject(x.dstAttr() + "-Group"), x.dstAttr() + "-Group"));

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
			.map(x -> {
			if (groupMapping.value().containsKey(x.srcId())
					&& !x.attr().getRelationship().toString().contains(groupingProperty)) {
				return new Edge<Relation>((long) groupMapping.value().get(x.srcId()), x.dstId(), x.attr());
			}
			if (groupMapping.value().containsKey(x.dstId())
					&& !x.attr().getRelationship().toString().contains(groupingProperty)) {
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

	//triple generating
	public static Graph<Object, Relation> replaceByGrouping(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String replacementObject,
			String groupingValue) {
		HashMap<Object, Object> hashmap = new HashMap<Object, Object>();

		//ManoueveringAreaUsage & "usageType"
		JavaRDD<Edge<Relation>> groups = quadGraph.triplets().toJavaRDD()
				.filter(x -> ((Resource) x.srcAttr()).getType().contains(replacementObject)
						&& x.attr().getRelationship().toString().contains(groupingValue))
					.map(x -> new Edge<Relation>(x.srcId(), x.dstId(), x.attr()));

		// put mapping into hashmap from tobeplaced and replacement
		groups.collect().forEach(x -> hashmap.put(x.srcId(), x.dstId()));
		Broadcast<HashMap<Object, Object>> groupMapping = jsc.broadcast(hashmap);

		// replace subjets/objects with their grouping
		JavaRDD<Edge<Relation>> replacedEdges = quadGraph.triplets().toJavaRDD()
				.map(x -> {
			if (groupMapping.value().containsKey(x.srcId())
					&& !x.attr().getRelationship().toString().contains(groupingValue))
			{
				return new Edge<Relation>((long) groupMapping.value().get(x.srcId()), x.dstId(), x.attr());
			}
			if (groupMapping.value().containsKey(x.dstId())
					&& !!x.attr().getRelationship().toString().contains(groupingValue)) {
				return new Edge<Relation>(x.srcId(), (long) groupMapping.value().get(x.dstId()), x.attr());
			} else {
				return new Edge<Relation>(x.srcId(), x.dstId(), x.attr());
			}
		});

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices(), replacedEdges.rdd(), "",
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag, relationTag);

		return graph;
	}

	
	
	
	

	//without context information
	public static Graph<Object, Relation> reify(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String subject, String reificationPredicate,
			String type, String object, ArrayList<String> contexts) {
		Broadcast<ArrayList<String>> broadcastContexts = jsc.broadcast(contexts);
		
		JavaRDD<EdgeTriplet<Object, Relation>> statements = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains(reificationPredicate)
						&&  broadcastContexts.value().contains(x.attr().getContext().toString())
						);

		Long typeID = getIdOfObject(type);
		List<Tuple2<Object, Object>> typeV = new ArrayList<Tuple2<Object, Object>>();
		typeV.add(new Tuple2<Object, Object>(typeID, type));
		JavaRDD<Tuple2<Object, Object>> typeVertice = jsc.parallelize(typeV);
		
		// new Vertices + mod
		JavaRDD<Tuple4<Object, Object, Object, Object>> statementTuple4 = statements
				.map(x -> new Tuple4<Object, Object, Object, Object>(x.srcId(), x.dstId(),
						new Resource("urn:uuid:" + UUID.randomUUID()), x.attr().getContext().toString()))
				.persist(StorageLevel.MEMORY_ONLY());
		
		// 1 = subject, 2 = object, 3 ID of new one, 4 = new Resource, 5 = context/mod
		JavaRDD<Tuple5<Object, Object, Object, Object, Object>> statementTuple5 = statementTuple4
				.map(x -> new Tuple5<Object, Object, Object, Object, Object>(x._1(), x._2(),
						getIdOfObject(((Resource) x._3()).getValue()), (Resource) x._3(), x._4()));
		
		JavaRDD<Tuple2<Object, Object>> newVertices = statementTuple4
				.map(x -> new Tuple2<Object, Object>(getIdOfObject(((Resource) x._3()).getValue()), (Resource) x._3()));

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
			String selectionCondition) {

		HashMap<Object, Object> hashmap = new HashMap<Object, Object>();
	
		JavaRDD<Tuple2<Object, Object>> modObjectMapping = quadGraph.triplets().toJavaRDD()
		.filter(x -> x.attr().getRelationship().toString().contains(dimensionProperty))
		.map(x -> new Tuple2<Object, Object>(x.srcAttr()+"-mod", x.dstId()));
		modObjectMapping.collect().forEach(x -> hashmap.put(x._1(), x._2()));	
		Broadcast<HashMap<Object, Object>> mapping = jsc.broadcast(hashmap);

		JavaRDD<Edge<Relation>> newEdges = quadGraph.triplets().toJavaRDD()
				.filter(x -> ((Resource) x.srcAttr()).getType().contains(selectionCondition))
				.map(x -> new Tuple2<Object, Object> (x.srcId(), x.attr().getContext())).distinct()
				.map(x -> new Edge<Relation>((long) x._1(), (long) mapping.value().get(x._2()),
						new Relation(pivotProperty, x._2.toString(), "Resource")));
		
		RDD<Edge<Relation>> allEdges = quadGraph.edges().union(newEdges.rdd());

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices(), allEdges, "", StorageLevel.MEMORY_ONLY(),
				StorageLevel.MEMORY_ONLY(), objectTag, relationTag);
		return graph;
	}

	//value generating
	public static Graph<Object, Relation> aggregatePropertyValues(Graph<Object, Relation> quadGraph,
			JavaSparkContext jsc, ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String aggregateProperty,
			String aggregateType, ArrayList<String> contexts) {
		Broadcast<ArrayList<String>> broadcastContexts = jsc.broadcast(contexts);
		
		JavaPairRDD<Tuple2<Long, Object>, Long> verticesPair = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().equals(aggregateProperty)
						&& broadcastContexts.value().contains(x.attr().getContext().toString()))
				.mapToPair(x -> new Tuple2<Tuple2<Long, Object>, Long>(new Tuple2<Long, Object>((Long) x.srcId(), x.attr().getContext().toString()), Long.parseLong(x.dstAttr().toString())));
		verticesPair.foreach(x -> System.out.println(x));
		
		JavaRDD<Tuple2<Object, Object>> newVertices = null;
		JavaRDD<Edge<Relation>> newEdges = null;

		if (aggregateType.contains("AVG")) { // done
			newEdges = verticesPair.mapValues(x -> new Tuple2<Long, Long>(x, 1L))
					.reduceByKey((x, y) -> new Tuple2<Long, Long>(x._1 + y._1, x._2 + y._2)).mapValues(x -> x._1 / x._2)
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
							new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));

			newVertices = verticesPair.mapValues(x -> new Tuple2<Long, Long>(x, 1L))
					.reduceByKey((x, y) -> new Tuple2<Long, Long>(x._1 + y._1, x._2 + y._2)).mapValues(x -> x._1 / x._2)
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}

		if (aggregateType.contains("COUNT")) {
			newEdges = verticesPair.mapValues(x -> 1L).reduceByKey((x, y) -> x + y)
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
							new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));

			newVertices = verticesPair.mapValues(x -> 1L).reduceByKey((x, y) -> x + y)
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}
		if (aggregateType.contains("SUM")) {
			newEdges = verticesPair.reduceByKey((a, b) -> a + b)
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
					new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));
			newVertices = verticesPair.reduceByKey((a, b) -> a + b)
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));

		}
		if (aggregateType.contains("MAX")) {
			newEdges = verticesPair.reduceByKey((a, b) -> Math.max(a, b))
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
							new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));

			newVertices = verticesPair.reduceByKey((a, b) -> Math.max(a, b))
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}
		if (aggregateType.contains("MIN")) {
			newEdges = verticesPair.reduceByKey((a, b) -> Math.min(a, b))
					.map(x -> new Edge<Relation>(x._1()._1, getIdOfObject(x._2),
							new Relation(aggregateProperty, x._1()._2.toString(), Double.class.getSimpleName().toString())));

			newVertices = verticesPair.reduceByKey((a, b) -> Math.min(a, b))
					.map(x -> new Tuple2<Object, Object>(getIdOfObject(x._2), x._2));
		}

		JavaRDD<Edge<Relation>> keepTriplets = quadGraph.triplets().toJavaRDD()
				.filter(x -> !x.attr().getRelationship().toString().equals(aggregateProperty)
						|| !broadcastContexts.value().contains(x.attr().getContext().toString())
						)
				.map(x -> new Edge<Relation>(x.srcId(), x.dstId(), x.attr()));

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices().toJavaRDD().union(newVertices).rdd(),
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
					&& !x.attr().getRelationship().toString().contains(groupingProperty)) {
				return new Edge<Relation>((long) groupMapping.value().get(x.srcId()), x.dstId(), x.attr());
			}
			if (groupMapping.value().containsKey(x.dstId())
					&& !x.attr().getRelationship().toString().contains(groupingProperty)) {
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

	//triple generating
	public static Graph<Object, Relation> replaceByGrouping(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String replacementObject,
			String groupingValue, ArrayList<String> contexts) {
		Broadcast<ArrayList<String>> broadcastContexts = jsc.broadcast(contexts);
		HashMap<Object, Object> hashmap = new HashMap<Object, Object>();

		//ManoueveringAreaUsage & "usageType"
		JavaRDD<Edge<Relation>> groups = quadGraph.triplets().toJavaRDD()
				.filter(x -> ((Resource) x.srcAttr()).getType().contains(replacementObject)
						&& x.attr().getRelationship().toString().contains(groupingValue)
						&& broadcastContexts.value().contains(x.attr().getContext().toString()))
					.map(x -> new Edge<Relation>(x.srcId(), x.dstId(), x.attr()));

		// put mapping into hashmap from tobeplaced and replacement
		groups.collect().forEach(x -> hashmap.put(x.srcId(), x.dstId()));
		Broadcast<HashMap<Object, Object>> groupMapping = jsc.broadcast(hashmap);

		// replace subjets/objects with their grouping
		JavaRDD<Edge<Relation>> replacedEdges = quadGraph.triplets().toJavaRDD()
				.map(x -> {
			if (groupMapping.value().containsKey(x.srcId())
					&& broadcastContexts.value().contains(x.attr().getContext().toString())
					&& !x.attr().getRelationship().toString().contains(groupingValue))
			{
				return new Edge<Relation>((long) groupMapping.value().get(x.srcId()), x.dstId(), x.attr());
			}
			if (groupMapping.value().containsKey(x.dstId())
					&& broadcastContexts.value().contains(x.attr().getContext().toString())
					&& !!x.attr().getRelationship().toString().contains(groupingValue)) {
				return new Edge<Relation>(x.srcId(), (long) groupMapping.value().get(x.dstId()), x.attr());
			} else {
				return new Edge<Relation>(x.srcId(), x.dstId(), x.attr());
			}
		});

		Graph<Object, Relation> graph = Graph.apply(quadGraph.vertices(), replacedEdges.rdd(), "",
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag, relationTag);

		return graph;
	}
	

	
	
	//help
	public static long getIdOfObject(Object s) {
		return UUID.nameUUIDFromBytes(s.toString().getBytes()).getMostSignificantBits();
	}
}