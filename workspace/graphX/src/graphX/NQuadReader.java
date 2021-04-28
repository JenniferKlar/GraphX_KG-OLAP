package graphX;

import org.apache.spark.SparkConf;

import scala.reflect.ClassTag;
import scala.Tuple2;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.storage.StorageLevel;

public class NQuadReader {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("NQuadReader").setMaster("local[*]")
				.set("spark.executor.memory", "2g").set("spark.executor.cores", "1")
				.set("spark.dynamicAllocation.enabled", "true")
				.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		ClassTag<Object> objectTag = scala.reflect.ClassTag$.MODULE$.apply(Object.class);
		ClassTag<Relation> relationTag = scala.reflect.ClassTag$.MODULE$.apply(Relation.class);

		Graph<Object, Relation> quadGraph = GraphGenerator.generateGraph(jsc, objectTag, relationTag);

		// sliceDice(quadGraph, jsc, objectTag, relationTag, "Level_Importance_Package",
		// "Level_Aircraft_All", "Level_Location_All",
		// "Level_Date_Year").triplets().toJavaRDD().foreach(x ->
		// System.out.println(x.dstAttr()));
		// sliceDiceCorrect(quadGraph, jsc, objectTag, relationTag,
		// "Level_Importance_All-All","Level_Aircraft_All-All",
		// "Level_Location_All-All", "Level_Date_All-All");

		// merge(quadGraph, jsc, objectTag, relationTag, "Level_Importance_All",
		// "Level_Aircraft_All", "Level_Location_All", "Level_Date_Year");

		List<Tuple2<Object, Object>> res = getVerticesAttributes(quadGraph, getCellsAndBelow(quadGraph, jsc, objectTag,
				relationTag, "Level_Importance_All", "Level_Aircraft_All", "Level_Location_All", "Level_Date_Year"));
		for (int i = 0; i < res.size(); i++) {
			int j = i;
			quadGraph.triplets().toJavaRDD().filter(x -> x.srcAttr().equals(res.get(j)));
		}

		String cell = "urn:uuid:4c239bc4-cc63-46ec-91e2-31375dac798a";
		List<Object> importance = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains("hasImportance")
						&& x.srcAttr().toString().contains(cell))
				.map(x -> x.dstAttr()).collect();
		List<Object> aircraft = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains("hasAircraft")
						&& x.srcAttr().toString().contains(cell))
				.map(x -> x.dstAttr()).collect();
		List<Object> location = quadGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains("hasLocation")
						&& x.srcAttr().toString().contains(cell))
				.map(x -> x.dstAttr()).collect();
		List<Object> date = quadGraph.triplets().toJavaRDD().filter(
				x -> x.attr().getRelationship().toString().contains("hasDate") && x.srcAttr().toString().contains(cell))
				.map(x -> x.dstAttr()).collect();

		String imp = "";
		String air = "";
		String loc = "";
		String dat = "";

		if (importance.size() == 1 && aircraft.size() == 1 && location.size() == 1 && date.size() == 1) {
			imp = importance.get(0).toString();
			air = aircraft.get(0).toString();
			loc = location.get(0).toString();
			dat = date.get(0).toString();

		}
		System.out.println(imp);
		System.out.println(air);
		System.out.println(loc);
		System.out.println(dat);

		jsc.close();

	}

	private static List<Tuple2<Object, Object>> getVerticesAttributes(Graph<Object, Relation> graph, List<Object> ids) {
		List<Tuple2<Object, Object>> result = new ArrayList<>();
		for (int i = 0; i < ids.size(); i++) {
			int j = i;
			result.addAll(graph.vertices().toJavaRDD().filter(x -> x._1.equals(ids.get(j))).collect());
		}
		return result;
	}

	private static Graph<Object, Relation> sliceDiceCorrect(Graph<Object, Relation> graph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String importanceValue, String aircraftValue,
			String locationValue, String dateValue) {
		List<Object> cellIds = getCellsCorrect(graph, jsc, objectTag, relationTag, importanceValue, aircraftValue,
				locationValue, dateValue);
		List<String> mods = getMods(graph, cellIds);
		List<Edge<Relation>> rels = new ArrayList<>();
		for (int i = 0; i < mods.size(); i++) {
			int j = i;
			rels.addAll(graph.edges().toJavaRDD()
					.filter(e -> ((Resource) e.attr().getContext()).getValue().contains(mods.get(j))).collect());
		}

		List<Tuple2<Object, Object>> moduleVertices = new ArrayList<>();
		List<Object> vertices = new ArrayList<Object>();
		// all sources and destinations of all mods
		rels.forEach(x -> vertices.add(x.dstId()));
		rels.forEach(x -> vertices.add(x.srcId()));
		// filter vertices
		graph.vertices().toJavaRDD().collect().forEach(v -> {
			if (vertices.contains(v._1)) {
				moduleVertices.add(v);
			}
		});
		jsc.parallelize(moduleVertices);
		jsc.parallelize(rels);
		// create graph obejct from filtered edges and vertices
		Graph<Object, Relation> resultGraph = Graph.apply(jsc.parallelize(moduleVertices).rdd(),
				jsc.parallelize(rels).rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag,
				relationTag);

		return resultGraph;
	}

	private static List<Object> getCellsCorrect(Graph<Object, Relation> graph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String importanceValue, String aircraftValue,
			String locationValue, String dateValue) {
		// get cells for each dimension individually
		List<Object> importance = getCellsWithDimValueAndCovered(graph, objectTag, relationTag, "hasImportance",
				importanceValue, jsc);
		List<Object> aircraft = getCellsWithDimValueAndCovered(graph, objectTag, relationTag, "hasAircraft",
				aircraftValue, jsc);
		List<Object> location = getCellsWithDimValueAndCovered(graph, objectTag, relationTag, "hasLocation",
				locationValue, jsc);
		List<Object> date = getCellsWithDimValueAndCovered(graph, objectTag, relationTag, "hasDate", dateValue, jsc);
		List<Object> result = new ArrayList<>();

		// compare lists --> if cell is in each --> return it (satisfies all four
		// dimensions)
		for (int i = 0; i < importance.size(); i++) {
			Object o = importance.get(i);
			if (aircraft.contains(o) && location.contains(o) && date.contains(o)) {
				result.add(o);
			}
		}
		return result;
	}

	private static List<Object> getCellsWithDimValueAndCovered(Graph<Object, Relation> graph,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String dimension, String value,
			JavaSparkContext jsc) {

		ArrayList<String> lowerInstances = new ArrayList<>();
		lowerInstances.add(value);
		if (GraphGenerator.getLowerInstances(value, jsc) != null) {
			lowerInstances.addAll(GraphGenerator.getLowerInstances(value, jsc).collect());
		}

		ArrayList<Object> result = new ArrayList<>();
		for (int i = 0; i < lowerInstances.size(); i++) {
			int j = i;
			result.addAll(graph.triplets().toJavaRDD()
					.filter(triplet -> ((Resource) triplet.attr().getRelationship()).getValue().contains(dimension)
							&& triplet.dstAttr().toString().contains(lowerInstances.get(j)))
					.map(triplet -> triplet.srcId()).collect());
		}
		return result;
	}

	// get all Data as a Graph that are associated with modules that correspond to
	// cells that have required dimension levels
	private static Graph<Object, Relation> sliceDice(Graph<Object, Relation> graph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String importanceLevel, String aircraftLevel,
			String locationLevel, String dateLevel) {
		List<Object> cellIds = getCellsAndBelow(graph, jsc, objectTag, relationTag, importanceLevel, aircraftLevel,
				locationLevel, dateLevel);
		List<String> mods = getMods(graph, cellIds);
		List<Edge<Relation>> rels = new ArrayList<>();
		for (int i = 0; i < mods.size(); i++) {
			int j = i;
			rels.addAll(graph.edges().toJavaRDD()
					.filter(e -> ((Resource) e.attr().getContext()).getValue().contains(mods.get(j))).collect());
		}

		List<Tuple2<Object, Object>> moduleVertices = new ArrayList<>();
		List<Object> vertices = new ArrayList<Object>();
		// all sources and destinations of all mods
		rels.forEach(x -> vertices.add(x.dstId()));
		rels.forEach(x -> vertices.add(x.srcId()));
		// filter vertices
		graph.vertices().toJavaRDD().collect().forEach(v -> {
			if (vertices.contains(v._1)) {
				moduleVertices.add(v);
			}
		});
		jsc.parallelize(moduleVertices);
		jsc.parallelize(rels);
		// create graph obejct from filtered edges and vertices
		Graph<Object, Relation> resultGraph = Graph.apply(jsc.parallelize(moduleVertices).rdd(),
				jsc.parallelize(rels).rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag,
				relationTag);

		return resultGraph;
	}

	// get all Mods (string) that belong to the cells that have correct dimensions
	private static List<String> getMods(Graph<Object, Relation> quadGraph, List<Object> cellIds) {
		List<String> result = new ArrayList<>();
		// get mods
		List<EdgeTriplet<Object, Relation>> modules = quadGraph.triplets().toJavaRDD()
				.filter(x -> ((Resource) x.attr().getRelationship()).getValue().contains("hasAssertedModule"))
				.collect();
		for (int i = 0; i < modules.size(); i++) {
			for (int j = 0; j < cellIds.size(); j++) {
				// id cellId contains mod --> add it to result
				if (modules.get(i).srcId() == (long) cellIds.get(j)) {
					result.add(modules.get(i).dstAttr().toString());
				}
			}
		}
		return result;
	}

	// get all cells (their ID) that satisfy dimensionlevels and them below
	private static List<Object> getCellsAndBelow(Graph<Object, Relation> quadGraph, JavaSparkContext jsc,
			ClassTag<Object> objectTag, ClassTag<Relation> relationTag, String importanceLevel, String aircraftLevel,
			String locationLevel, String dateLevel) {
		// get cells for each dimension individually
		List<Object> importance = getCellsWithLevelAndLower(quadGraph, objectTag, relationTag, "hasImportance",
				importanceLevel);
		List<Object> aircraft = getCellsWithLevelAndLower(quadGraph, objectTag, relationTag, "hasAircraft",
				aircraftLevel);
		List<Object> location = getCellsWithLevelAndLower(quadGraph, objectTag, relationTag, "hasLocation",
				locationLevel);
		List<Object> date = getCellsWithLevelAndLower(quadGraph, objectTag, relationTag, "hasDate", dateLevel);
		List<Object> result = new ArrayList<>();

		// compare lists --> if cell is in each --> return it (satisfies all four
		// dimensions)
		for (int i = 0; i < importance.size(); i++) {
			Object o = importance.get(i);
			if (aircraft.contains(o) && location.contains(o) && date.contains(o)) {
				result.add(o);
			}
		}
		return result;
	}

	private static List<Object> getCellsWithLevel(Graph<Object, Relation> graph, ClassTag<Object> objectTag,
			ClassTag<Relation> relationTag, String dimension, String level) {
		ArrayList<String> levels = new ArrayList<>();
		levels.add(level);

		ArrayList<Long> aList = new ArrayList<>();
		for (int i = 0; i < levels.size(); i++) {
			int j = i;
			aList.addAll(graph.triplets().toJavaRDD()
					.filter(triplet -> ((Resource) triplet.attr().getRelationship()).getValue().contains("atLevel")
							&& triplet.dstAttr().toString().contains(levels.get(j)))
					.map(triplet -> triplet.srcId()).collect());
		}
		List<EdgeTriplet<Object, Relation>> g = graph.triplets().toJavaRDD()
				.filter(triplet -> ((Resource) triplet.attr().getRelationship()).getValue().contains(dimension))
				.collect();

		List<Object> result = new ArrayList<>();
		for (int i = 0; i < g.size(); i++) {
			if (aList.contains(g.get(i).dstId())) {
				result.add(g.get(i).srcId());
			}
		}
		return result;
	}

	// gets Cells that have a certain level of a specific dimension and below
	private static List<Object> getCellsWithLevelAndLower(Graph<Object, Relation> graph, ClassTag<Object> objectTag,
			ClassTag<Relation> relationTag, String dimension, String level) {
		ArrayList<String> lowerLevels = new ArrayList<>();
		lowerLevels.add(level);
		if (GraphGenerator.getLowerlevels(level) != null) {
			lowerLevels.addAll(GraphGenerator.getLowerlevels(level));
		}
		ArrayList<Long> aList = new ArrayList<>();
		for (int i = 0; i < lowerLevels.size(); i++) {
			int j = i;
			aList.addAll(graph.triplets().toJavaRDD()
					.filter(triplet -> ((Resource) triplet.attr().getRelationship()).getValue().contains("atLevel")
							&& triplet.dstAttr().toString().contains(lowerLevels.get(j)))
					.map(triplet -> triplet.srcId()).collect());
		}

		List<EdgeTriplet<Object, Relation>> g = graph.triplets().toJavaRDD()
				.filter(triplet -> ((Resource) triplet.attr().getRelationship()).getValue().contains(dimension))
				.collect();

		List<Object> result = new ArrayList<>();
		for (int i = 0; i < g.size(); i++) {
			if (aList.contains(g.get(i).dstId())) {
				result.add(g.get(i).srcId());
			}
		}
		return result;
	}
}
