package graphX;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.reflect.ClassTag;

public class GraphGenerator {
	public GraphGenerator() {
	}

	public static Graph<Object, Relation> generateGraph(JavaSparkContext jsc, ClassTag<Object> objectTag,
			ClassTag<Relation> relationTag) {
		String path = "C:\\Users\\jenniffer\\Dropbox\\Masterarbeit";

		JavaRDD<Quad> javaRDD = getJavaRDD(path, jsc);

		Set<Object> set = new LinkedHashSet<>();
		set.addAll(javaRDD.map(x -> new Resource(x.getSubject().toString())).collect());
		set.addAll(javaRDD.filter(x -> x.getObject().isLiteral()).map(x -> x.getObject().toString()).collect());
		set.addAll(javaRDD.filter(x -> !x.getObject().isLiteral()).map(x -> new Resource(x.getObject().toString()))
				.collect());

		List<Object> verticesList = new ArrayList<>(set);

		// all Objects that are Literals
		JavaRDD<Edge<Relation>> literalEdges = javaRDD.filter(x -> x.getObject().isLiteral())
				.map(x -> new Edge<Relation>(verticesList.indexOf(new Resource(x.getSubject().toString())),
						verticesList.indexOf(x.getObject().toString()),
						new Relation(new Resource(x.getPredicate().toString()), new Resource(x.getGraph().toString()),
								x.getObject().getLiteralDatatype().getJavaClass().getSimpleName().toString())));

		// all Objects that are Resources
		JavaRDD<Edge<Relation>> resourceEdges = javaRDD.filter(x -> !x.getObject().isLiteral())
				.map(x -> new Edge<Relation>(verticesList.indexOf(new Resource(x.getSubject().toString())),
						verticesList.indexOf(new Resource(x.getObject().toString())),
						new Relation(new Resource(x.getPredicate().toString()), new Resource(x.getGraph().toString()),
								"Resource")));

		JavaRDD<Edge<Relation>> quadEdgeRDD = literalEdges.union(resourceEdges);

		// add IDs to Vertices from their Index
		JavaRDD<Tuple2<Object, Object>> vertices = jsc.parallelize(verticesList).zipWithIndex()
				.map(x -> new Tuple2<Object, Object>(x._2, x._1));

		// create RDDs from lists
		// JavaRDD<Edge<Relation>> quadEdgeRDD = jsc.parallelize(quadEdges);

		Graph<Object, Relation> quadGraph = Graph.apply(vertices.rdd(), quadEdgeRDD.rdd(), "",
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), objectTag, relationTag);

		return quadGraph;
	}

	// get JavaRDD from n-quad file
	private static JavaRDD<Quad> getJavaRDD(String path, JavaSparkContext jsc) {

		JavaRDD<Quad> javaRDD = jsc.textFile(path + "\\output_short.nq").filter(line -> !line.startsWith("#"))
				.filter(line -> !line.isEmpty() || line.length() != 0).map(line -> RDFDataMgr
						.createIteratorQuads(new ByteArrayInputStream(line.getBytes()), Lang.NQUADS, null).next());
		javaRDD.persist(StorageLevel.MEMORY_AND_DISK());
		return javaRDD;
	}

	// returns a list of all rollup relationships of certain given instance
	static JavaRDD<String> getLowerInstances(String instance, JavaSparkContext jsc) {
		HashMap<String, JavaRDD<String>> instanceMap = new HashMap<String, JavaRDD<String>>();
		ArrayList<String> aircraftAll = new ArrayList<>();
		aircraftAll.add("c82c7d6ae614");
		aircraftAll.add("39349af73677");
		aircraftAll.add("84c2eb97b9f8");
		aircraftAll.add("23cdb2917571");
		aircraftAll.add("659830915fcc");
		aircraftAll.add("213069b205ef");
		aircraftAll.add("ec245d8f0b81");
		aircraftAll.add("74c81f5e6635");
		aircraftAll.add("5dc51fb5153e");
		aircraftAll.add("69f5146a61a3");
		aircraftAll.add("86967a783164");
		aircraftAll.add("7194cc6525fb");
		aircraftAll.add("906840b13b2d");
		aircraftAll.add("572e1cdc8c8f");
		aircraftAll.add("f3873a3931fe");
		aircraftAll.add("c785d658cbdd");
		aircraftAll.add("64901cda3e87");
		aircraftAll.add("eeb26e279ed1");
		aircraftAll.add("6e3580b5f2a2");
		aircraftAll.add("959e8839c384");
		aircraftAll.add("133410b5c1a3");
		aircraftAll.add("b8b4ba93e61d");
		aircraftAll.add("e5c70bb466a1");
		aircraftAll.add("52d6df6ee6d2");
		aircraftAll.add("ce9ebb5c05af");
		aircraftAll.add("fcc299815080");
		aircraftAll.add("ba7bdd3046b2");
		aircraftAll.add("6d1c3b4dbf29");
		aircraftAll.add("be3915d61e4d");
		aircraftAll.add("15578b93cf9d");
		aircraftAll.add("bea0448e9b7f");
		aircraftAll.add("c51f0d7697b4");
		aircraftAll.add("1eaefdf4cba5");
		aircraftAll.add("4d26b1462681");
		aircraftAll.add("2ff0743621c1");
		aircraftAll.add("74c4c5b18f20");
		aircraftAll.add("93f3e36159cc");
		aircraftAll.add("defa04867c51");
		aircraftAll.add("86e8a7e40b80");
		JavaRDD<String> rdd = jsc.parallelize(aircraftAll);
		instanceMap.put("Level_Aircraft_All-All", rdd);

		ArrayList<String> aircraftModel1 = new ArrayList<>();
		aircraftModel1.add("5dc51fb5153e");
		aircraftModel1.add("64901cda3e87");
		aircraftModel1.add("ce9ebb5c05af");
		aircraftModel1.add("1eaefdf4cba5");
		rdd = jsc.parallelize(aircraftModel1);
		instanceMap.put("c82c7d6ae614", rdd);
		ArrayList<String> aircraftModel2 = new ArrayList<>();
		aircraftModel2.add("69f5146a61a3");
		aircraftModel2.add("eeb26e279ed1");
		aircraftModel2.add("fcc299815080");
		rdd = jsc.parallelize(aircraftModel2);
		instanceMap.put("39349af73677", rdd);
		ArrayList<String> aircraftModel3 = new ArrayList<>();
		aircraftModel3.add("86967a783164");
		aircraftModel3.add("6e3580b5f2a2");
		aircraftModel3.add("ba7bdd3046b2");
		aircraftModel3.add("4d26b1462681");
		rdd = jsc.parallelize(aircraftModel3);
		instanceMap.put("84c2eb97b9f8", rdd);
		ArrayList<String> aircraftModel4 = new ArrayList<>();
		aircraftModel4.add("7194cc6525fb");
		aircraftModel4.add("959e8839c384");
		aircraftModel4.add("6d1c3b4dbf29");
		aircraftModel4.add("2ff0743621c1");
		rdd = jsc.parallelize(aircraftModel4);
		instanceMap.put("23cdb2917571", rdd);
		ArrayList<String> aircraftModel5 = new ArrayList<>();
		aircraftModel5.add("906840b13b2d");
		aircraftModel5.add("133410b5c1a3");
		aircraftModel5.add("be3915d61e4d");
		aircraftModel5.add("74c4c5b18f20");
		rdd = jsc.parallelize(aircraftModel5);
		instanceMap.put("659830915fcc", rdd);
		ArrayList<String> aircraftModel6 = new ArrayList<>();
		aircraftModel6.add("572e1cdc8c8f");
		aircraftModel6.add("b8b4ba93e61d");
		aircraftModel6.add("15578b93cf9d");
		aircraftModel6.add("93f3e36159cc");
		rdd = jsc.parallelize(aircraftModel6);
		instanceMap.put("213069b205ef", rdd);
		ArrayList<String> aircraftModel7 = new ArrayList<>();
		aircraftModel7.add("f3873a3931fe");
		aircraftModel7.add("e5c70bb466a1");
		aircraftModel7.add("bea0448e9b7f");
		aircraftModel7.add("defa04867c51");
		rdd = jsc.parallelize(aircraftModel7);
		instanceMap.put("ec245d8f0b81", rdd);
		ArrayList<String> aircraftModel8 = new ArrayList<>();
		aircraftModel8.add("c785d658cbdd");
		aircraftModel8.add("52d6df6ee6d2");
		aircraftModel8.add("c51f0d7697b4");
		aircraftModel8.add("86e8a7e40b80");
		rdd = jsc.parallelize(aircraftModel8);
		instanceMap.put("74c81f5e6635", rdd);
		ArrayList<String> locationAll = new ArrayList<>();
		locationAll.add("4f7ed092a702");
		locationAll.add("8ef0358fe826");
		locationAll.add("8663b59fe537");
		locationAll.add("121ccbb4d866");
		locationAll.add("8e0e7516d3c3");
		locationAll.add("076760540a38");
		locationAll.add("f35d65318cee");
		locationAll.add("d059ee103466");
		locationAll.add("dfb94de543e2");
		locationAll.add("bb2b6a6daa76");
		rdd = jsc.parallelize(locationAll);
		instanceMap.put("Level_Location_All-All", rdd);
		ArrayList<String> locationRegion1 = new ArrayList<>();
		locationRegion1.add("8663b59fe537");
		locationRegion1.add("121ccbb4d866");
		locationRegion1.add("8e0e7516d3c3");
		locationRegion1.add("076760540a38");
		rdd = jsc.parallelize(locationRegion1);
		instanceMap.put("4f7ed092a702", rdd);
		ArrayList<String> locationRegion2 = new ArrayList<>();
		locationRegion2.add("f35d65318cee");
		locationRegion2.add("d059ee103466");
		locationRegion2.add("dfb94de543e2");
		locationRegion2.add("bb2b6a6daa76");
		rdd = jsc.parallelize(locationRegion2);
		instanceMap.put("8ef0358fe826", rdd);

		ArrayList<String> importanceAll = new ArrayList<>();
		importanceAll.add("b343f3d3badd");
		importanceAll.add("d8853e3502d8");
		importanceAll.add("e1f071ccdf19");
		importanceAll.add("60c25a70c2fb");
		importanceAll.add("38f074dc2be8");
		importanceAll.add("e2e39d129a4f");
		importanceAll.add("6f4dc708f55b");
		importanceAll.add("1dce00303d38");
		importanceAll.add("3ebf1ba989c2");
		importanceAll.add("0b59728a55ae");
		importanceAll.add("1c22d84cd8c9");
		importanceAll.add("09a92dfcbec0");
		importanceAll.add("3ebf1ba989c2");
		importanceAll.add("0b59728a55ae");
		importanceAll.add("1c22d84cd8c9");
		importanceAll.add("09a92dfcbec0");
		importanceAll.add("38b6e26040ce");
		importanceAll.add("292e45f5f204");
		importanceAll.add("afdd1e7beda8");
		importanceAll.add("07eff604fe2d");
		rdd = jsc.parallelize(importanceAll);
		instanceMap.put("Level_Importance_All-All", rdd);
		ArrayList<String> importancePackage1 = new ArrayList<>();
		importancePackage1.add("38f074dc2be8");
		importancePackage1.add("e2e39d129a4f");
		importancePackage1.add("6f4dc708f55b");
		importancePackage1.add("1dce00303d38");
		rdd = jsc.parallelize(importancePackage1);
		instanceMap.put("b343f3d3badd", rdd);
		ArrayList<String> importancePackage2 = new ArrayList<>();
		importancePackage2.add("3ebf1ba989c2");
		importancePackage2.add("0b59728a55ae");
		importancePackage2.add("1c22d84cd8c9");
		importancePackage2.add("09a92dfcbec0");
		rdd = jsc.parallelize(importancePackage2);
		instanceMap.put("d8853e3502d8", rdd);
		ArrayList<String> importancePackage3 = new ArrayList<>();
		importancePackage3.add("38b6e26040ce");
		importancePackage3.add("292e45f5f204");
		importancePackage3.add("afdd1e7beda8");
		importancePackage3.add("07eff604fe2d");
		rdd = jsc.parallelize(importancePackage3);
		instanceMap.put("e1f071ccdf19", rdd);
		ArrayList<String> importancePackage4 = new ArrayList<>();
		importancePackage4.add("a40704879f25");
		importancePackage4.add("cc8cfc347d44");
		importancePackage4.add("c100172f7955");
		importancePackage4.add("cb5045301aa2");
		rdd = jsc.parallelize(importancePackage4);
		instanceMap.put("60c25a70c2fb", rdd);

		ArrayList<String> dateAll = new ArrayList<>();
		dateAll.add("8ba60c383168");
		dateAll.add("68311b884156");
		dateAll.add("124e0ea435f8");
		dateAll.add("4d6aa02fea95");
		dateAll.add("6b8999b70b48");
		dateAll.add("8ac9cbcfd31b");
		dateAll.add("9a143760d985");
		dateAll.add("c7c4c76404d4");
		dateAll.add("b17fb8cddec2");
		dateAll.add("cf2d90c6d0ae");
		dateAll.add("c5a849b7fa3b");
		dateAll.add("01b8c55df468");
		dateAll.add("b0ce01dc768b");
		dateAll.add("c37228a4352e");
		dateAll.add("90590ea2f549");
		dateAll.add("810964424230");
		dateAll.add("e01f3bcbb0cd");
		dateAll.add("47624f773c6d");
		dateAll.add("50a4e6faedcb");
		dateAll.add("fcce6d22990a");
		dateAll.add("65ea6bef64bf");
		dateAll.add("a4dcaac9ad51");
		dateAll.add("b58e0e6007af");
		dateAll.add("41265018b652");
		dateAll.add("165bf95c9274");
		dateAll.add("cca18d421f0c");
		dateAll.add("a870e12e208b");
		dateAll.add("2a1a2ece11d5");
		rdd = jsc.parallelize(dateAll);
		instanceMap.put("Level_Date_All-All", rdd);

		ArrayList<String> dateYear1 = new ArrayList<>();
		dateYear1.add("6b8999b70b48");
		dateYear1.add("8ac9cbcfd31b");
		dateYear1.add("b0ce01dc768b");
		dateYear1.add("c37228a4352e");
		dateYear1.add("90590ea2f549");
		dateYear1.add("810964424230");
		rdd = jsc.parallelize(dateYear1);
		instanceMap.put("8ba60c383168", rdd);
		ArrayList<String> dateYear2 = new ArrayList<>();
		dateYear2.add("9a143760d985");
		dateYear2.add("c7c4c76404d4");
		dateYear2.add("e01f3bcbb0cd");
		dateYear2.add("47624f773c6d");
		dateYear2.add("50a4e6faedcb");
		dateYear2.add("fcce6d22990a");
		rdd = jsc.parallelize(dateYear2);
		instanceMap.put("68311b884156", rdd);
		ArrayList<String> dateYear3 = new ArrayList<>();
		dateYear3.add("b17fb8cddec2");
		dateYear3.add("cf2d90c6d0ae");
		dateYear3.add("65ea6bef64bf");
		dateYear3.add("a4dcaac9ad51");
		dateYear3.add("b58e0e6007af");
		dateYear3.add("41265018b652");
		rdd = jsc.parallelize(dateYear3);
		instanceMap.put("124e0ea435f8", rdd);
		ArrayList<String> dateYear4 = new ArrayList<>();
		dateYear4.add("c5a849b7fa3b");
		dateYear4.add("01b8c55df468");
		dateYear4.add("165bf95c9274");
		dateYear4.add("cca18d421f0c");
		dateYear4.add("a870e12e208b");
		dateYear4.add("2a1a2ece11d5");
		rdd = jsc.parallelize(dateYear4);
		instanceMap.put("4d6aa02fea95", rdd);

		ArrayList<String> dateMonth1 = new ArrayList<>();
		dateMonth1.add("b0ce01dc768b");
		dateMonth1.add("c37228a4352e");
		rdd = jsc.parallelize(dateMonth1);
		instanceMap.put("6b8999b70b48", rdd);
		ArrayList<String> dateMonth2 = new ArrayList<>();
		dateMonth2.add("90590ea2f549");
		dateMonth2.add("810964424230");
		rdd = jsc.parallelize(dateMonth2);
		instanceMap.put("8ac9cbcfd31b", rdd);
		ArrayList<String> dateMonth3 = new ArrayList<>();
		dateMonth3.add("e01f3bcbb0cd");
		dateMonth3.add("47624f773c6d");
		rdd = jsc.parallelize(dateMonth3);
		instanceMap.put("9a143760d985", rdd);
		ArrayList<String> dateMonth4 = new ArrayList<>();
		dateMonth4.add("50a4e6faedcb");
		dateMonth4.add("fcce6d22990a");
		rdd = jsc.parallelize(dateMonth4);
		instanceMap.put("c7c4c76404d4", rdd);
		ArrayList<String> dateMonth5 = new ArrayList<>();
		dateMonth5.add("65ea6bef64bf");
		dateMonth5.add("a4dcaac9ad51");
		rdd = jsc.parallelize(dateMonth5);
		instanceMap.put("b17fb8cddec2", rdd);
		ArrayList<String> dateMonth6 = new ArrayList<>();
		dateMonth6.add("b58e0e6007af");
		dateMonth6.add("41265018b652");
		rdd = jsc.parallelize(dateMonth6);
		instanceMap.put("cf2d90c6d0ae", rdd);
		ArrayList<String> dateMonth7 = new ArrayList<>();
		dateMonth7.add("165bf95c9274");
		dateMonth7.add("cca18d421f0c");
		rdd = jsc.parallelize(dateMonth7);
		instanceMap.put("c5a849b7fa3b", rdd);
		ArrayList<String> dateMonth8 = new ArrayList<>();
		dateMonth8.add("a870e12e208b");
		dateMonth8.add("2a1a2ece11d5");
		rdd = jsc.parallelize(dateMonth8);
		instanceMap.put("01b8c55df468", rdd);
		if (instanceMap.get(instance) != null) {
			return instanceMap.get(instance);
		}
		return null;
	}

	// returns a list of all lower level of certain given level
	static ArrayList<String> getLowerlevels(String level) {
		HashMap<String, List<String>> lowerLevelMap = new HashMap<String, List<String>>();
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
		if (lowerLevelMap.get(level) != null) {
			return (ArrayList<String>) lowerLevelMap.get(level);
		}
		return null;
	}

	// returns all covered mods for specified mod
	// in progress
	static JavaRDD<String> getCoverage(String mod, JavaSparkContext jsc) {
		HashMap<String, JavaRDD<String>> coverageMap = new HashMap<String, JavaRDD<String>>();
		ArrayList<String> aircraftAll = new ArrayList<>();
		JavaRDD<String> rdd = jsc.parallelize(aircraftAll);
		coverageMap.put(mod, rdd);

		if (coverageMap.get(mod) != null) {
			return coverageMap.get(mod);
		}
		return null;
	}
}
