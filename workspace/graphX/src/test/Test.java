package test;

import org.junit.After;
import org.junit.Before;

import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Graph;

import graphX.GraphGenerator;
import graphX.NQuadReader;
import graphX.Relation;
import scala.reflect.ClassTag;

public class Test {

	ClassTag<Object> objectTag = scala.reflect.ClassTag$.MODULE$.apply(Object.class);
	ClassTag<Relation> relationTag = scala.reflect.ClassTag$.MODULE$.apply(Relation.class);
	String path = "C:\\Users\\jenniffer\\Dropbox\\Masterarbeit\\";

	SparkConf sparkConf = new SparkConf().setAppName("NQuadReader").setMaster("local[*]")
			.set("spark.executor.memory", "2g").set("spark.executor.cores", "1")
			.set("spark.dynamicAllocation.enabled", "true")
			.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
			.set("spark.eventLog.enabled", "true")
			.set("spark.eventLog.dir", "C:\\Users\\jenniffer\\Desktop")
			;
	JavaSparkContext jsc = new JavaSparkContext(sparkConf);
	

	@Before
	public void setLogLevel() {
		this.jsc.setLogLevel("ERROR");
		
	}

	@After
	public void shutDownJSC() {
		jsc.stop();
	}

	@org.junit.Test
	public void testPivot() {
		String fileName = "pivot.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		
		ArrayList<String> context = new ArrayList<String>();
		context.add("urn:uuid:2c95e204-26ea-43ec-a997-774b5dc41c6d-mod");
		context.add("urn:uuid:2c95e204-26ea-43ec-a997-774b5dc41c6test-mod");
		
		Graph<Object, Relation> pivotGraph = NQuadReader.pivot(graph, jsc, objectTag, relationTag, "hasLocation",
		"http://example.org/kgolap/object-model#hasLocation",
		"http://example.org/kgolap/object-model#ManoeuvringAreaAvailability",
		context);
		pivotGraph.triplets().toJavaRDD().foreach(x -> System.out.println(x.srcAttr() + " " + x.attr().getRelationship() + " " + x.dstAttr() + " " + x.attr().getContext()));

		
		long count1 = graph.edges().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains("hasLocation")).count();
		long count2 = pivotGraph.edges().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains("hasLocation")).count();
		long count3 = pivotGraph.edges().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains("http://example.org/kgolap/object-model#hasLocation")).count();
		Assert.assertNotEquals(count1, count2);
		Assert.assertEquals(1, count1);
		Assert.assertEquals(3, count2);
		Assert.assertEquals(2, count3);
		
	}	
	
	
	@org.junit.Test
	public void testReification() {
		String fileName = "reification.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		ArrayList<String> contexts = new ArrayList<String>();
		contexts.add("urn:uuid:0acc4b38-168d-4a33-898c-258b89881556-mod");
		Graph<Object, Relation> reifiedGraph = 
		NQuadReader.reify(graph, jsc, objectTag, relationTag,
				"http://example.org/kgolap/object-model#usage",
				"http://example.org/kgolap/object-model#usage-type", 
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#object", 
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#subject");

		long count1 = graph.triplets().toJavaRDD().count();
		long count2 = reifiedGraph.triplets().toJavaRDD().count();
		Assert.assertNotEquals(count1, count2);
		long count3 = reifiedGraph.triplets().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains("http://example.org/kgolap/object-model#usage-type")).count();
		Assert.assertEquals(2, count3);
		Assert.assertEquals(reifiedGraph.triplets().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#object")).count() 
				,reifiedGraph.triplets().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject")).count());
		reifiedGraph.triplets().toJavaRDD().foreach(x -> System.out.println(x.srcAttr() + " " + x.attr().getRelationship() + " " + x.dstAttr() + " " + x.attr().getContext()));
	}
	
	@org.junit.Test
	public void testGroupByProperty() {
		String fileName = "groupByProperty.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);

		ArrayList<String> contexts = new ArrayList<String>();
		contexts.add("urn:uuid:8378d3c2-575d-4cb8-874b-eb4ae286d61b-mod");
		Graph<Object, Relation> resultGraph = NQuadReader.groupByProperty(graph, jsc, objectTag, relationTag,
				"operationalStatus", "http://example.org/kgolap/object-model#grouping",contexts
				);

		long groupingCount = resultGraph.triplets().toJavaRDD()
				.filter(x -> x.attr().getRelationship().toString().contains(
						"http://example.org/kgolap/object-model#grouping") && x.dstAttr().toString().contains("-Group"))
				.count();

		Assert.assertNotEquals(resultGraph, graph);
		Assert.assertEquals(6, groupingCount);

		long groupCountDistinct = resultGraph.triplets().toJavaRDD()
				.filter(x -> x.srcAttr().toString().contains("-Group")).map(x -> x.srcId()).distinct().count();
		Assert.assertEquals(3, groupCountDistinct);
	}

	@org.junit.Test
	public void testReplaceByGrouping() {
		String fileName = "replaceByGrouping.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		ArrayList<String> contexts = new ArrayList<String>();
		contexts.add("urn:uuid:test");
		 Graph<Object, Relation> resultGraph 
		 = NQuadReader.replaceByGrouping(graph, jsc, objectTag, relationTag,"ManoeuvringAreaUsage", "usageType", contexts);
		 
		 Assert.assertNotEquals(graph, resultGraph);
		 long graphSources = graph.triplets().toJavaRDD().map(x -> x.srcId()).distinct().count();
		 long resultGraphSources = resultGraph.triplets().toJavaRDD().map(x -> x.srcId()).distinct().count();
		 Assert.assertNotEquals(graphSources, resultGraphSources);
		 
		 long count1 = graph.triplets().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains("usageType")).count();
		 long count2 = resultGraph.triplets().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains("usageType")).distinct().count();
		 Assert.assertEquals(count1, count2);
	}
	
	@org.junit.Test
	public void testAggregateByPropertyGroupingSUM() {
		String fileName = "aggregateByProperty.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		ArrayList<String> contexts = new ArrayList<String>();
		contexts.add("urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod");
		contexts.add("urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86test-mod");
//		graph.triplets().toJavaRDD().foreach
//		(x -> System.out.println(x.srcAttr() + " " + x.attr().getRelationship() + " " + x.dstAttr()+ " " + x.attr().getContext()));
// 
		Graph<Object, Relation> replacedGraph 
		 = NQuadReader.replaceByGrouping(graph, jsc, objectTag, relationTag,"AircraftCharacteristic", "wingspanInterpretation", contexts);
//		replacedGraph.triplets().toJavaRDD().foreach
//		(x -> System.out.println(x.srcAttr() + " " + x.attr().getRelationship() + " " + x.dstAttr()+ " " + x.attr().getContext()));
 
		
		Graph<Object, Relation> wingspanSum = NQuadReader.aggregatePropertyValues(replacedGraph, jsc, objectTag, relationTag, 
				 "http://example.org/kgolap/object-model#wingspan","SUM", contexts); 
		 
//		wingspanSum.triplets().toJavaRDD().foreach
//		(x -> System.out.println(x.srcAttr() + " " + x.attr().getRelationship() + " " + x.dstAttr()+ " " + x.attr().getContext()));
		long sum1 = wingspanSum.triplets().toJavaRDD().filter(x -> x.dstAttr().toString().contains("3061074470")).count();
		long sum2 = wingspanSum.triplets().toJavaRDD().filter(x -> x.dstAttr().toString().contains("1227604772")).count();
		
		Assert.assertEquals(1, sum1);
		Assert.assertEquals(1, sum2);
	}
	
	@org.junit.Test
	public void testAggregateByPropertyGroupingCOUNT() {
		String fileName = "aggregateByProperty.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		ArrayList<String> contexts = new ArrayList<String>();
		contexts.add("urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod");
		contexts.add("urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86test-mod");
		Graph<Object, Relation> replacedGraph 
		 = NQuadReader.replaceByGrouping(graph, jsc, objectTag, relationTag,"AircraftCharacteristic", "wingspanInterpretation", contexts);
		replacedGraph.triplets().toJavaRDD().foreach
			(x -> System.out.println(x.srcAttr() + " " + x.attr().getRelationship() + " " + x.dstAttr()+ " " + x.attr().getContext()));
		System.out.println("----------------");
		
		Graph<Object, Relation> wingspanSum = NQuadReader.aggregatePropertyValues(replacedGraph, jsc, objectTag, relationTag, 
				 "http://example.org/kgolap/object-model#wingspan", "COUNT",contexts); 
		 wingspanSum.triplets().toJavaRDD().foreach
			(x -> System.out.println(x.srcAttr() + " " + x.attr().getRelationship() + " " + x.dstAttr()+ " " + x.attr().getContext()));
		long count = wingspanSum.triplets().toJavaRDD().filter(x -> x.dstAttr().toString().equals("2")).count();
		
		Assert.assertEquals(3, count);
	}
	
	@org.junit.Test
	public void testAggregateByPropertyGroupBySUM() {
		String fileName = "aggregateByProperty.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		ArrayList<String> contexts = new ArrayList<String>();
		contexts.add("urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod");
		contexts.add("urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86test-mod"); 
		Graph<Object, Relation> replacedGraph 
		 = NQuadReader.groupByProperty(graph, jsc, objectTag, relationTag,"http://example.org/kgolap/object-model#wingspanInterpretation", "http://example.org/kgolap/object-model#grouping", contexts);
		 Graph<Object, Relation> resultGraph = NQuadReader.aggregatePropertyValues(replacedGraph, jsc, objectTag, relationTag, 
		"http://example.org/kgolap/object-model#wingspan", "SUM", contexts); 
		 
			long sum1 = resultGraph.triplets().toJavaRDD().filter(x -> x.dstAttr().toString().contains("3061074470")).count();
			long sum2 = resultGraph.triplets().toJavaRDD().filter(x -> x.dstAttr().toString().contains("1227604772")).count();
			
			Assert.assertEquals(1, sum1);
			Assert.assertEquals(1, sum2);
	}
	
	@org.junit.Test
	public void testAggregateByPropertyGroupByCOUNT() {
		String fileName = "aggregateByProperty.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		ArrayList<String> contexts = new ArrayList<String>();
		contexts.add("urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod");
		contexts.add("urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86test-mod");
		Graph<Object, Relation> replacedGraph 
		 = NQuadReader.groupByProperty(graph, jsc, objectTag, relationTag,"http://example.org/kgolap/object-model#wingspanInterpretation", "http://example.org/kgolap/object-model#grouping", 
				 contexts);
		 Graph<Object, Relation> resultGraph = NQuadReader.aggregatePropertyValues(replacedGraph, jsc, objectTag, relationTag, 
		"http://example.org/kgolap/object-model#wingspan",  "COUNT", contexts); 
		 
			long count = resultGraph.triplets().toJavaRDD().filter(x -> x.dstAttr().toString().equals("2")).count();
			Assert.assertEquals(3, count);
	}

}
