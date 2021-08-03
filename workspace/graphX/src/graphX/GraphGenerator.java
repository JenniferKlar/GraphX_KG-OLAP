package graphX;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaPairRDD;
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

	public static Graph<Object, Relation> generateGraph(JavaSparkContext jsc, ClassTag<Object> vertexTag,
			ClassTag<Relation> edgeTag, String path, String fileName) {

		JavaRDD<Quad> javaRDD = getJavaRDD(path, fileName, jsc);
				
		JavaRDD<Tuple2<Object, Object>> vertices = javaRDD.map(x -> (Object) new Resource(x.getSubject().toString()))
		.union(javaRDD.filter(x -> x.getObject().isLiteral()).map(x -> x.getObject().getLiteralValue().toString()))
		.union(javaRDD.filter(x -> !x.getObject().isLiteral()).map(x -> new Resource(x.getObject().toString()))).distinct()
		.map(x -> new Tuple2<Object, Object>(UUID.nameUUIDFromBytes(x.toString().getBytes()).getMostSignificantBits(), x));
		
		// all Objects that are Literals
		JavaRDD<Edge<Relation>> literalEdges = javaRDD.filter(x -> x.getObject().isLiteral())
				.map(x -> 
				new Edge<Relation>(UUID.nameUUIDFromBytes(x.getSubject().toString().getBytes()).getMostSignificantBits(),
						UUID.nameUUIDFromBytes(x.getObject().getLiteralValue().toString().getBytes()).getMostSignificantBits(),
						new Relation(new Resource(x.getPredicate().toString()), new Resource(x.getGraph().toString()),
								x.getObject().getLiteralDatatype().getJavaClass().getSimpleName().toString())));

		// all Objects that are Resources
		JavaRDD<Edge<Relation>> resourceEdges = javaRDD.filter(x -> !x.getObject().isLiteral())
				.map(x -> 
				new Edge<Relation>(UUID.nameUUIDFromBytes(x.getSubject().toString().getBytes()).getMostSignificantBits(),
						UUID.nameUUIDFromBytes(new Resource(x.getObject().toString()).toString().getBytes()).getMostSignificantBits(),
						new Relation(new Resource(x.getPredicate().toString()), new Resource(x.getGraph().toString()),
								"Resource")));

		JavaRDD<Edge<Relation>> quadEdgeRDD = literalEdges.union(resourceEdges);
		Graph<Object, Relation> quadGraph = Graph.apply(vertices.rdd(), quadEdgeRDD.rdd(), "",
				StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), vertexTag, edgeTag);

		return quadGraph;
	}

	// get JavaRDD from n-quad file
	private static JavaRDD<Quad> getJavaRDD(String path, String fileName,JavaSparkContext jsc) {

		JavaRDD<Quad> javaRDD = jsc.textFile(path + "\\"+ fileName).filter(line -> !line.startsWith("#"))
				.filter(line -> !line.isEmpty() || line.length() != 0).map(line -> RDFDataMgr
						.createIteratorQuads(new ByteArrayInputStream(line.getBytes()), Lang.NQUADS, null).next());
		javaRDD.persist(StorageLevel.MEMORY_AND_DISK());
		return javaRDD;
	}

	// returns a list of all rollup relationships of certain given instance
	static JavaRDD<String> getCoveredInstances(String instance, JavaSparkContext jsc) {
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
	 static JavaRDD<String> getCoverage(String mod, JavaSparkContext jsc) {
		HashMap<String, JavaRDD<String>> coverageMap = new HashMap<String, JavaRDD<String>>();
		ArrayList<String> covers = new ArrayList<>();
		covers.add("urn:uuid:77c42733-aea4-4f1c-93a1-3e02eea5548a");
		covers.add("urn:uuid:4c239bc4-cc63-46ec-91e2-31375dac798a");
		covers.add("urn:uuid:6de0c3d2-ec19-41d4-9f12-d22b7eda4883");
		covers.add("urn:uuid:566d4bf1-3d64-4582-b591-631143a69357");
		covers.add("urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86");
		covers.add("urn:uuid:841014a7-ff74-4b40-8b3f-a9b083e1339d");
		covers.add("urn:uuid:fb79b9b5-8258-4282-a25d-9e4e272fcab1");
		covers.add("urn:uuid:6bac6d27-c54e-403e-94f7-ab7437d0aac4");
		covers.add("urn:uuid:17af4a37-9312-4f14-8e5f-321ebacb7957");
		covers.add("urn:uuid:d3ae5ab1-e998-48b5-b6d5-81e5662368f4");
		covers.add("urn:uuid:c670034d-1b3c-4fd2-8879-ca293f525787");
		covers.add("urn:uuid:687c09d1-114f-4f59-8798-84f63ab27176");
		covers.add("urn:uuid:f94f180d-f09a-4a8e-b1c4-a75d48d56949");
		covers.add("urn:uuid:a846e25f-8c07-475c-8ab6-5fba4c6a366d");
		covers.add("urn:uuid:5d77f31f-6ffc-49d9-a940-e4cd6b0ba19f");
		covers.add("urn:uuid:675b1e11-06c0-4c6b-8083-075089853552");
		covers.add("urn:uuid:39314f3d-3975-42f4-9c15-f5cef8493f49");
		covers.add("urn:uuid:2c95e204-26ea-43ec-a997-774b5dc41c6d");
		covers.add("urn:uuid:50ad2e1a-40c6-41f5-ba7c-df134f75c678");
		covers.add("urn:uuid:0acc4b38-168d-4a33-898c-258b89881556");
		covers.add("urn:uuid:0384c6cf-e18c-426c-b707-0fa44383a2a3");
		covers.add("urn:uuid:d631c578-a4bb-4416-87c6-f44c929f03fc");
		covers.add("urn:uuid:8378d3c2-575d-4cb8-874b-eb4ae286d61b");
		covers.add("urn:uuid:3afbf060-1a39-495d-8a16-24d4f0aa983f");
		covers.add("urn:uuid:faef9e3f-5a2f-4cf7-87d2-ffb9c89ce54d");
		covers.add("urn:uuid:f051dba7-93df-429b-a730-f34d4ea1b522");
		covers.add("urn:uuid:8f7314f1-b7b6-434c-a8ac-b161809a66fe");
		covers.add("urn:uuid:8ab12162-5a0c-4223-a5a7-83b2aecd824d");
		covers.add("urn:uuid:c21e3593-f034-47ce-8cd5-6de6688001a3");
		covers.add("urn:uuid:2e936993-75e9-43f6-b272-137e535d1210");
		covers.add("urn:uuid:4f4b5d37-8742-4d58-86fc-f7465328e8a8");
		covers.add("urn:uuid:ef8b5795-53c9-4908-ad9b-3be517b1717a");
		covers.add("urn:uuid:269d3f53-2caf-4b4c-ace8-cd0fdf072365");
		covers.add("urn:uuid:5270afb5-8954-4734-a611-8e211ba58889");
		covers.add("urn:uuid:a62edf44-13e3-481e-a96d-831b9a1c4d29");
		covers.add("urn:uuid:47e6ea7c-b953-4cf9-9c75-0eb5e5b1ca4a");
		covers.add("urn:uuid:b51f0edd-d182-4bc0-adc6-6a1813fb8a1e");
		covers.add("urn:uuid:1de8b2e6-371c-4b30-aa30-1f4437105bf6");
		covers.add("urn:uuid:a92dbc19-69eb-4bac-ac38-e397b638779e");
		covers.add("urn:uuid:9984f36a-c2c0-41ec-9c73-dab819efab62");
		covers.add("urn:uuid:da1806d4-2cca-49e5-b94d-97acbfc4377f");
		covers.add("urn:uuid:4e12cad7-89ce-492e-8f81-f702c4b06adc");
		covers.add("urn:uuid:bbd95ab5-0d2b-4d59-8bd8-466aaaa42512");
		covers.add("urn:uuid:425a08e3-7def-4a14-b68c-fbca41ad1dfb");
		covers.add("urn:uuid:7e80979b-9003-4efe-8fce-fb97bd779059");
		covers.add("urn:uuid:af10333e-524e-49aa-8c6d-35c2bf0ce566");
		covers.add("urn:uuid:89264326-108f-46da-a504-cf62475b0f7a");
		covers.add("urn:uuid:e023f29a-06ff-4cec-9bb6-1a496c6025ea");
		covers.add("urn:uuid:f933c945-ac36-47c3-99e2-56d63095e07e");
		covers.add("urn:uuid:4dec2c1c-e428-4041-a4e0-c271c7e97823");
		covers.add("urn:uuid:03d427b2-6dfd-4eaa-9516-502d2e8eb386");
		covers.add("urn:uuid:b9bd92c1-bab6-495a-9b07-cf887c2ba971");
		covers.add("urn:uuid:d0c0888a-851f-4a80-b297-2d36492912c7");
		covers.add("urn:uuid:b8393cc4-5d7b-41bc-b129-ee8720c7fc68");
		covers.add("urn:uuid:645aa795-c1df-4584-833a-b0211f7a5b5a");
		covers.add("urn:uuid:8a99a96b-1adf-404f-8f61-ff5edf0c52ab");
		covers.add("urn:uuid:6f7cb308-26a3-4cb6-876f-742adff08a51");
		covers.add("urn:uuid:0daef85b-6431-475b-b80b-1eab7c2f54fe");
		covers.add("urn:uuid:8bc6bdeb-f30b-4f24-9f2c-8045135dc5f4");
		covers.add("urn:uuid:ec9a63db-45e4-4a69-afd2-6d5e7478afd3");
		covers.add("urn:uuid:e3896eeb-e862-499d-a03a-9599854087bd");
		covers.add("urn:uuid:c2da0d6a-7c8b-4202-87e7-058a83dfbc0e");
		JavaRDD<String> rdd = jsc.parallelize(covers);
		coverageMap.put("urn:uuid:c17f306c-3ca3-4bc0-a896-6c3702c539fb", rdd);
		ArrayList<String> covers2 = new ArrayList<>();
		covers2.add("urn:uuid:4c239bc4-cc63-46ec-91e2-31375dac798a");
		covers2.add("urn:uuid:6de0c3d2-ec19-41d4-9f12-d22b7eda4883");
		covers2.add("urn:uuid:566d4bf1-3d64-4582-b591-631143a69357");
		covers2.add("urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86");
		covers2.add("urn:uuid:841014a7-ff74-4b40-8b3f-a9b083e1339d");
		covers2.add("urn:uuid:fb79b9b5-8258-4282-a25d-9e4e272fcab1");
		covers2.add("urn:uuid:6bac6d27-c54e-403e-94f7-ab7437d0aac4");
		covers2.add("urn:uuid:17af4a37-9312-4f14-8e5f-321ebacb7957");
		covers2.add("urn:uuid:d3ae5ab1-e998-48b5-b6d5-81e5662368f4");
		covers2.add("urn:uuid:c670034d-1b3c-4fd2-8879-ca293f525787");
		covers2.add("urn:uuid:687c09d1-114f-4f59-8798-84f63ab27176");
		covers2.add("urn:uuid:f94f180d-f09a-4a8e-b1c4-a75d48d56949");
		covers2.add("urn:uuid:a846e25f-8c07-475c-8ab6-5fba4c6a366d");
		covers2.add("urn:uuid:5d77f31f-6ffc-49d9-a940-e4cd6b0ba19f");
		covers2.add("urn:uuid:675b1e11-06c0-4c6b-8083-075089853552");
		covers2.add("urn:uuid:39314f3d-3975-42f4-9c15-f5cef8493f49");
		covers2.add("urn:uuid:2c95e204-26ea-43ec-a997-774b5dc41c6d");
		covers2.add("urn:uuid:50ad2e1a-40c6-41f5-ba7c-df134f75c678");
		covers2.add("urn:uuid:0acc4b38-168d-4a33-898c-258b89881556");
		covers2.add("urn:uuid:0384c6cf-e18c-426c-b707-0fa44383a2a3");
		covers2.add("urn:uuid:d631c578-a4bb-4416-87c6-f44c929f03fc");
		covers2.add("urn:uuid:8378d3c2-575d-4cb8-874b-eb4ae286d61b");
		covers2.add("urn:uuid:3afbf060-1a39-495d-8a16-24d4f0aa983f");
		covers2.add("urn:uuid:faef9e3f-5a2f-4cf7-87d2-ffb9c89ce54d");
		covers2.add("urn:uuid:f051dba7-93df-429b-a730-f34d4ea1b522");
		covers2.add("urn:uuid:8f7314f1-b7b6-434c-a8ac-b161809a66fe");
		covers2.add("urn:uuid:8ab12162-5a0c-4223-a5a7-83b2aecd824d");
		covers2.add("urn:uuid:c21e3593-f034-47ce-8cd5-6de6688001a3");
		covers2.add("urn:uuid:2e936993-75e9-43f6-b272-137e535d1210");
		covers2.add("urn:uuid:4f4b5d37-8742-4d58-86fc-f7465328e8a8");
		JavaRDD<String> rdd2 = jsc.parallelize(covers2);
		coverageMap.put("urn:uuid:77c42733-aea4-4f1c-93a1-3e02eea5548a", rdd2);
		ArrayList<String> covers3 = new ArrayList<>();
		covers3.add("urn:uuid:6de0c3d2-ec19-41d4-9f12-d22b7eda4883");
		covers3.add("urn:uuid:566d4bf1-3d64-4582-b591-631143a69357");
		covers3.add("urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86");
		covers3.add("urn:uuid:841014a7-ff74-4b40-8b3f-a9b083e1339d");
		covers3.add("urn:uuid:fb79b9b5-8258-4282-a25d-9e4e272fcab1");
		covers3.add("urn:uuid:6bac6d27-c54e-403e-94f7-ab7437d0aac4");
		covers3.add("urn:uuid:17af4a37-9312-4f14-8e5f-321ebacb7957");
		covers3.add("urn:uuid:d3ae5ab1-e998-48b5-b6d5-81e5662368f4");
		covers3.add("urn:uuid:c670034d-1b3c-4fd2-8879-ca293f525787");
		covers3.add("urn:uuid:687c09d1-114f-4f59-8798-84f63ab27176");
		covers3.add("urn:uuid:f94f180d-f09a-4a8e-b1c4-a75d48d56949");
		covers3.add("urn:uuid:a846e25f-8c07-475c-8ab6-5fba4c6a366d");
		covers3.add("urn:uuid:5d77f31f-6ffc-49d9-a940-e4cd6b0ba19f");
		covers3.add("urn:uuid:675b1e11-06c0-4c6b-8083-075089853552");
		JavaRDD<String> rdd3 = jsc.parallelize(covers3);
		coverageMap.put("urn:uuid:4c239bc4-cc63-46ec-91e2-31375dac798a", rdd3);
		ArrayList<String> covers4 = new ArrayList<>();
		covers4.add("urn:uuid:566d4bf1-3d64-4582-b591-631143a69357");
		covers4.add("urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86");
		covers4.add("urn:uuid:841014a7-ff74-4b40-8b3f-a9b083e1339d");
		covers4.add("urn:uuid:fb79b9b5-8258-4282-a25d-9e4e272fcab1");
		covers4.add("urn:uuid:6bac6d27-c54e-403e-94f7-ab7437d0aac4");
		covers4.add("urn:uuid:17af4a37-9312-4f14-8e5f-321ebacb7957");
		JavaRDD<String> rdd4 = jsc.parallelize(covers4);
		coverageMap.put("urn:uuid:6de0c3d2-ec19-41d4-9f12-d22b7eda4883", rdd4);
		
		ArrayList<String> covers5 = new ArrayList<>();
		covers5.add("urn:uuid:c670034d-1b3c-4fd2-8879-ca293f525787");
		covers5.add("urn:uuid:687c09d1-114f-4f59-8798-84f63ab27176");
		covers5.add("urn:uuid:f94f180d-f09a-4a8e-b1c4-a75d48d56949");
		covers5.add("urn:uuid:a846e25f-8c07-475c-8ab6-5fba4c6a366d");
		covers5.add("urn:uuid:5d77f31f-6ffc-49d9-a940-e4cd6b0ba19f");
		covers5.add("urn:uuid:675b1e11-06c0-4c6b-8083-075089853552");
		JavaRDD<String> rdd5 = jsc.parallelize(covers5);
		coverageMap.put("urn:uuid:d3ae5ab1-e998-48b5-b6d5-81e5662368f4", rdd5);
		
		ArrayList<String> covers6 = new ArrayList<>();
		covers6.add("urn:uuid:2c95e204-26ea-43ec-a997-774b5dc41c6d");
		covers6.add("urn:uuid:50ad2e1a-40c6-41f5-ba7c-df134f75c678");
		covers6.add("urn:uuid:0acc4b38-168d-4a33-898c-258b89881556");
		covers6.add("urn:uuid:0384c6cf-e18c-426c-b707-0fa44383a2a3");
		covers6.add("urn:uuid:d631c578-a4bb-4416-87c6-f44c929f03fc");
		covers6.add("urn:uuid:8378d3c2-575d-4cb8-874b-eb4ae286d61b");
		covers6.add("urn:uuid:3afbf060-1a39-495d-8a16-24d4f0aa983f");

		JavaRDD<String> rdd6 = jsc.parallelize(covers6);
		coverageMap.put("urn:uuid:39314f3d-3975-42f4-9c15-f5cef8493f49", rdd6);
		
		
		ArrayList<String> covers7 = new ArrayList<>();
		covers7.add("urn:uuid:50ad2e1a-40c6-41f5-ba7c-df134f75c678");
		covers7.add("urn:uuid:0acc4b38-168d-4a33-898c-258b89881556");
		covers7.add("urn:uuid:0384c6cf-e18c-426c-b707-0fa44383a2a3");
		covers7.add("urn:uuid:d631c578-a4bb-4416-87c6-f44c929f03fc");
		covers7.add("urn:uuid:8378d3c2-575d-4cb8-874b-eb4ae286d61b");
		covers7.add("urn:uuid:3afbf060-1a39-495d-8a16-24d4f0aa983f");
		JavaRDD<String> rdd7 = jsc.parallelize(covers7);
		coverageMap.put("urn:uuid:2c95e204-26ea-43ec-a997-774b5dc41c6d", rdd7);
		
		ArrayList<String> covers8 = new ArrayList<>();
		covers7.add("urn:uuid:f051dba7-93df-429b-a730-f34d4ea1b522");
		covers7.add("urn:uuid:8f7314f1-b7b6-434c-a8ac-b161809a66fe");
		covers7.add("urn:uuid:8ab12162-5a0c-4223-a5a7-83b2aecd824d");
		covers7.add("urn:uuid:c21e3593-f034-47ce-8cd5-6de6688001a3");
		covers7.add("urn:uuid:2e936993-75e9-43f6-b272-137e535d1210");
		covers7.add("urn:uuid:4f4b5d37-8742-4d58-86fc-f7465328e8a8");
		JavaRDD<String> rdd8 = jsc.parallelize(covers8);
		coverageMap.put("urn:uuid:faef9e3f-5a2f-4cf7-87d2-ffb9c89ce54d", rdd8);		
		
		
		ArrayList<String> covers9 = new ArrayList<>();
		covers9.add("urn:uuid:269d3f53-2caf-4b4c-ace8-cd0fdf072365");
		covers9.add("urn:uuid:5270afb5-8954-4734-a611-8e211ba58889");
		covers9.add("urn:uuid:a62edf44-13e3-481e-a96d-831b9a1c4d29");
		covers9.add("urn:uuid:47e6ea7c-b953-4cf9-9c75-0eb5e5b1ca4a");
		covers9.add("urn:uuid:b51f0edd-d182-4bc0-adc6-6a1813fb8a1e");
		covers9.add("urn:uuid:1de8b2e6-371c-4b30-aa30-1f4437105bf6");
		covers9.add("urn:uuid:a92dbc19-69eb-4bac-ac38-e397b638779e");
		covers9.add("urn:uuid:9984f36a-c2c0-41ec-9c73-dab819efab62");
		covers9.add("urn:uuid:da1806d4-2cca-49e5-b94d-97acbfc4377f");
		covers9.add("urn:uuid:4e12cad7-89ce-492e-8f81-f702c4b06adc");
		covers9.add("urn:uuid:bbd95ab5-0d2b-4d59-8bd8-466aaaa42512");
		covers9.add("urn:uuid:425a08e3-7def-4a14-b68c-fbca41ad1dfb");
		covers9.add("urn:uuid:7e80979b-9003-4efe-8fce-fb97bd779059");
		covers9.add("urn:uuid:af10333e-524e-49aa-8c6d-35c2bf0ce566");
		covers9.add("urn:uuid:89264326-108f-46da-a504-cf62475b0f7a");
		covers9.add("urn:uuid:e023f29a-06ff-4cec-9bb6-1a496c6025ea");
		covers9.add("urn:uuid:f933c945-ac36-47c3-99e2-56d63095e07e");
		covers9.add("urn:uuid:4dec2c1c-e428-4041-a4e0-c271c7e97823");
		covers9.add("urn:uuid:03d427b2-6dfd-4eaa-9516-502d2e8eb386");
		covers9.add("urn:uuid:b9bd92c1-bab6-495a-9b07-cf887c2ba971");
		covers9.add("urn:uuid:d0c0888a-851f-4a80-b297-2d36492912c7");
		covers9.add("urn:uuid:b8393cc4-5d7b-41bc-b129-ee8720c7fc68");
		covers9.add("urn:uuid:645aa795-c1df-4584-833a-b0211f7a5b5a");
		covers9.add("urn:uuid:8a99a96b-1adf-404f-8f61-ff5edf0c52ab");
		covers9.add("urn:uuid:6f7cb308-26a3-4cb6-876f-742adff08a51");
		covers9.add("urn:uuid:0daef85b-6431-475b-b80b-1eab7c2f54fe");
		covers9.add("urn:uuid:8bc6bdeb-f30b-4f24-9f2c-8045135dc5f4");
		covers9.add("urn:uuid:ec9a63db-45e4-4a69-afd2-6d5e7478afd3");
		covers9.add("urn:uuid:e3896eeb-e862-499d-a03a-9599854087bd");
		covers9.add("urn:uuid:c2da0d6a-7c8b-4202-87e7-058a83dfbc0e");
		JavaRDD<String> rdd9 = jsc.parallelize(covers9);
		coverageMap.put("urn:uuid:ef8b5795-53c9-4908-ad9b-3be517b1717a", rdd9);
		
		
		ArrayList<String> covers10 = new ArrayList<>();
		covers10.add("urn:uuid:5270afb5-8954-4734-a611-8e211ba58889");
		covers10.add("urn:uuid:a62edf44-13e3-481e-a96d-831b9a1c4d29");
		covers10.add("urn:uuid:47e6ea7c-b953-4cf9-9c75-0eb5e5b1ca4a");
		covers10.add("urn:uuid:b51f0edd-d182-4bc0-adc6-6a1813fb8a1e");
		covers10.add("urn:uuid:1de8b2e6-371c-4b30-aa30-1f4437105bf6");
		covers10.add("urn:uuid:a92dbc19-69eb-4bac-ac38-e397b638779e");
		covers10.add("urn:uuid:9984f36a-c2c0-41ec-9c73-dab819efab62");
		covers10.add("urn:uuid:da1806d4-2cca-49e5-b94d-97acbfc4377f");
		covers10.add("urn:uuid:4e12cad7-89ce-492e-8f81-f702c4b06adc");
		covers10.add("urn:uuid:bbd95ab5-0d2b-4d59-8bd8-466aaaa42512");
		covers10.add("urn:uuid:425a08e3-7def-4a14-b68c-fbca41ad1dfb");
		covers10.add("urn:uuid:7e80979b-9003-4efe-8fce-fb97bd779059");
		covers10.add("urn:uuid:af10333e-524e-49aa-8c6d-35c2bf0ce566");
		covers10.add("urn:uuid:89264326-108f-46da-a504-cf62475b0f7a");
		JavaRDD<String> rdd10 = jsc.parallelize(covers10);
		coverageMap.put("urn:uuid:269d3f53-2caf-4b4c-ace8-cd0fdf072365", rdd10);
		
		ArrayList<String> covers11 = new ArrayList<>();
		covers11.add("urn:uuid:a62edf44-13e3-481e-a96d-831b9a1c4d29");
		covers11.add("urn:uuid:47e6ea7c-b953-4cf9-9c75-0eb5e5b1ca4a");
		covers11.add("urn:uuid:b51f0edd-d182-4bc0-adc6-6a1813fb8a1e");
		covers11.add("urn:uuid:1de8b2e6-371c-4b30-aa30-1f4437105bf6");
		covers11.add("urn:uuid:a92dbc19-69eb-4bac-ac38-e397b638779e");
		covers11.add("urn:uuid:9984f36a-c2c0-41ec-9c73-dab819efab62");
		JavaRDD<String> rdd11 = jsc.parallelize(covers11);
		coverageMap.put("urn:uuid:5270afb5-8954-4734-a611-8e211ba58889", rdd11);
		
		ArrayList<String> covers12 = new ArrayList<>();
		covers12.add("urn:uuid:4e12cad7-89ce-492e-8f81-f702c4b06adc");
		covers12.add("urn:uuid:bbd95ab5-0d2b-4d59-8bd8-466aaaa42512");
		covers12.add("urn:uuid:425a08e3-7def-4a14-b68c-fbca41ad1dfb");
		covers12.add("urn:uuid:7e80979b-9003-4efe-8fce-fb97bd779059");
		covers12.add("urn:uuid:af10333e-524e-49aa-8c6d-35c2bf0ce566");
		covers12.add("urn:uuid:89264326-108f-46da-a504-cf62475b0f7a");
		JavaRDD<String> rdd12 = jsc.parallelize(covers12);
		coverageMap.put("urn:uuid:da1806d4-2cca-49e5-b94d-97acbfc4377f", rdd12);
		
		ArrayList<String> covers13 = new ArrayList<>();
		covers13.add("urn:uuid:f933c945-ac36-47c3-99e2-56d63095e07e");
		covers13.add("urn:uuid:4dec2c1c-e428-4041-a4e0-c271c7e97823");
		covers13.add("urn:uuid:03d427b2-6dfd-4eaa-9516-502d2e8eb386");
		covers13.add("urn:uuid:b9bd92c1-bab6-495a-9b07-cf887c2ba971");
		covers13.add("urn:uuid:d0c0888a-851f-4a80-b297-2d36492912c7");
		covers13.add("urn:uuid:b8393cc4-5d7b-41bc-b129-ee8720c7fc68");
		covers13.add("urn:uuid:645aa795-c1df-4584-833a-b0211f7a5b5a");
		covers13.add("urn:uuid:8a99a96b-1adf-404f-8f61-ff5edf0c52ab");
		covers13.add("urn:uuid:6f7cb308-26a3-4cb6-876f-742adff08a51");
		covers13.add("urn:uuid:0daef85b-6431-475b-b80b-1eab7c2f54fe");
		covers13.add("urn:uuid:8bc6bdeb-f30b-4f24-9f2c-8045135dc5f4");
		covers13.add("urn:uuid:ec9a63db-45e4-4a69-afd2-6d5e7478afd3");
		covers13.add("urn:uuid:e3896eeb-e862-499d-a03a-9599854087bd");
		covers13.add("urn:uuid:c2da0d6a-7c8b-4202-87e7-058a83dfbc0e");
		JavaRDD<String> rdd13 = jsc.parallelize(covers13);
		coverageMap.put("urn:uuid:e023f29a-06ff-4cec-9bb6-1a496c6025ea", rdd13);
		
		ArrayList<String> covers14 = new ArrayList<>();
		covers14.add("urn:uuid:4dec2c1c-e428-4041-a4e0-c271c7e97823");
		covers14.add("urn:uuid:03d427b2-6dfd-4eaa-9516-502d2e8eb386");
		covers14.add("urn:uuid:b9bd92c1-bab6-495a-9b07-cf887c2ba971");
		covers14.add("urn:uuid:d0c0888a-851f-4a80-b297-2d36492912c7");
		covers14.add("urn:uuid:b8393cc4-5d7b-41bc-b129-ee8720c7fc68");
		covers14.add("urn:uuid:645aa795-c1df-4584-833a-b0211f7a5b5a");
		JavaRDD<String> rdd14 = jsc.parallelize(covers14);
		coverageMap.put("urn:uuid:f933c945-ac36-47c3-99e2-56d63095e07e", rdd14);
		
		ArrayList<String> covers15 = new ArrayList<>();
		covers15.add("urn:uuid:6f7cb308-26a3-4cb6-876f-742adff08a51");
		covers15.add("urn:uuid:0daef85b-6431-475b-b80b-1eab7c2f54fe");
		covers15.add("urn:uuid:8bc6bdeb-f30b-4f24-9f2c-8045135dc5f4");
		covers15.add("urn:uuid:ec9a63db-45e4-4a69-afd2-6d5e7478afd3");
		covers15.add("urn:uuid:e3896eeb-e862-499d-a03a-9599854087bd");
		covers15.add("urn:uuid:c2da0d6a-7c8b-4202-87e7-058a83dfbc0e");
		JavaRDD<String> rdd15 = jsc.parallelize(covers14);
		coverageMap.put("urn:uuid:8a99a96b-1adf-404f-8f61-ff5edf0c52ab", rdd15);
		if (coverageMap.get(mod) != null) {
			return coverageMap.get(mod);
		}
		return null;
	}
}
