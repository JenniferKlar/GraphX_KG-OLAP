package test;

import org.junit.After;
import org.junit.Before;
import org.junit.Assert;
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
	String path = "C:\\Users\\jenniffer\\Dropbox\\Masterarbeit";

	SparkConf sparkConf = new SparkConf().setAppName("NQuadReader").setMaster("local[*]")
			.set("spark.executor.memory", "2g").set("spark.executor.cores", "1")
			.set("spark.dynamicAllocation.enabled", "true")
			.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
			.set("spark.eventLog.enabled", "true");
	JavaSparkContext jsc = new JavaSparkContext(sparkConf);

	@Before
	public void setLogLevel() {
		this.jsc.setLogLevel("ERROR");
	}

	@After
	public void shutDownJSC() {
		jsc.close();
	}

	@org.junit.Test
	public void testGraphGenerator() {
		String fileName = "generateGraph.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		Assert.assertEquals(graph.vertices().count(), 7);
		Assert.assertEquals(graph.edges().count(), 6);
		
	}
	
	@org.junit.Test
	public void testSliceDiceAll() {
		String fileName = "sliceDice.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		Graph<Object, Relation> graph2 = NQuadReader.sliceDice(graph, jsc, objectTag, relationTag,
				"Level_Importance_All-All",
				"Level_Aircraft_All-All",
				"Level_Location_All-All",
				"Level_Date_All-All");
		Assert.assertEquals(graph.triplets().count(), graph2.triplets().count());
		
	}
	
	@org.junit.Test
	public void testSliceDice() {
		String fileName = "sliceDice.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		Graph<Object, Relation> graph2 = NQuadReader.sliceDice(graph, jsc, objectTag, relationTag,
				"Level_Importance_All-All",
				"c82c7d6ae614",
				"8663b59fe537",
				"Level_Date_All-All");
		
		Assert.assertNotEquals(graph.triplets().count(), graph2.triplets().count());
		
	}

	@org.junit.Test
	public void testMerge() {
		String fileName = "merge.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);

		Graph<Object, Relation> mergedGraph = NQuadReader.merge(graph, jsc, "Level_Importance_Package","Level_Aircraft_All","Level_Location_Region","Level_Date_Year", objectTag, relationTag);
				
		long count1 = graph.edges().toJavaRDD().map(x -> x.attr().getContext().toString()).distinct().count();
		long count2 = mergedGraph.edges().toJavaRDD().map(x -> x.attr().getContext().toString()).distinct().count();

		Assert.assertNotEquals(count1, count2);
		Assert.assertEquals(7, count1);
		Assert.assertEquals(4, count2);

	}
	
	@org.junit.Test
	public void testMergeAll() {
		String fileName = "merge.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);

		Graph<Object, Relation> mergedGraph = NQuadReader.merge(graph, jsc, "Level_Importance_All","Level_Aircraft_All","Level_Location_Region","Level_Date_All", objectTag, relationTag);
		
		long count1 = graph.edges().toJavaRDD().map(x -> x.attr().getContext().toString()).distinct().count();
		long count2 = mergedGraph.edges().toJavaRDD().map(x -> x.attr().getContext().toString()).distinct().count();
				
		Assert.assertEquals(count1, count2);
		Assert.assertEquals(7, count1);
		Assert.assertEquals(7, count2);
	}
	
	@org.junit.Test
	public void testPivot() {
		String fileName = "pivot.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		
		Graph<Object, Relation> pivotGraph = NQuadReader.pivot(graph, jsc, objectTag, relationTag, "hasLocation",
		"http://example.org/kgolap/object-model#hasLocation", "type",
		"http://example.org/kgolap/object-model#ManoeuvringAreaAvailability",
		"urn:uuid:2c95e204-26ea-43ec-a997-774b5dc41c6d-mod");	
		
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
		Graph<Object, Relation> reifiedGraph = 
		NQuadReader.reify(graph, jsc, objectTag, relationTag,"urn:uuid:0acc4b38-168d-4a33-898c-258b89881556-mod", "http://example.org/kgolap/object-model#usage",
				"http://example.org/kgolap/object-model#usage-type", "http://www.w3.org/1999/02/22-rdf-syntax-ns#object", "http://www.w3.org/1999/02/22-rdf-syntax-ns#subject");

		long count1 = graph.triplets().toJavaRDD().count();
		long count2 = reifiedGraph.triplets().toJavaRDD().count();
		Assert.assertNotEquals(count1, count2);
		long count3 = reifiedGraph.triplets().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains("http://example.org/kgolap/object-model#usage-type")).count();
		Assert.assertEquals(2, count3);
		Assert.assertEquals(reifiedGraph.triplets().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#object")).count() 
				,reifiedGraph.triplets().toJavaRDD().filter(x -> x.attr().getRelationship().toString().contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject")).count());
	}
	
	@org.junit.Test
	public void testGroupByProperty() {
		String fileName = "groupByProperty.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);

		Graph<Object, Relation> resultGraph = NQuadReader.groupByProperty(graph, jsc, objectTag, relationTag,
				"operationalStatus", "http://example.org/kgolap/object-model#grouping",
				"urn:uuid:8378d3c2-575d-4cb8-874b-eb4ae286d61b-mod");

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
		
		 Graph<Object, Relation> resultGraph 
		 = NQuadReader.replaceByGrouping(graph, jsc, objectTag, relationTag,"ManoeuvringAreaUsage", "usageType");
		 
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
		 Graph<Object, Relation> replacedGraph 
		 = NQuadReader.replaceByGrouping(graph, jsc, objectTag, relationTag,"AircraftCharacteristic", "wingspanInterpretation");
		 Graph<Object, Relation> wingspanSum = NQuadReader.aggregatePropertyValues(replacedGraph, jsc, objectTag, relationTag, 
				 "http://example.org/kgolap/object-model#wingspan", "<urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod>", "SUM"); 
		 
		long sum1 = wingspanSum.triplets().toJavaRDD().filter(x -> x.dstAttr().toString().contains("3061074470")).count();
		long sum2 = wingspanSum.triplets().toJavaRDD().filter(x -> x.dstAttr().toString().contains("1227604772")).count();
		
		Assert.assertEquals(1, sum1);
		Assert.assertEquals(1, sum2);
	}
	
	@org.junit.Test
	public void testAggregateByPropertyGroupingCOUNT() {
		String fileName = "aggregateByProperty.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		 Graph<Object, Relation> replacedGraph 
		 = NQuadReader.replaceByGrouping(graph, jsc, objectTag, relationTag,"AircraftCharacteristic", "wingspanInterpretation");
		 Graph<Object, Relation> wingspanSum = NQuadReader.aggregatePropertyValues(replacedGraph, jsc, objectTag, relationTag, 
				 "http://example.org/kgolap/object-model#wingspan", "<urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod>", "COUNT"); 
		 
		long count = wingspanSum.triplets().toJavaRDD().filter(x -> x.dstAttr().toString().equals("2")).count();
		
		Assert.assertEquals(2, count);
	}
	
	@org.junit.Test
	public void testAggregateByPropertyGroupBySUM() {
		String fileName = "aggregateByProperty.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		 Graph<Object, Relation> replacedGraph 
		 = NQuadReader.groupByProperty(graph, jsc, objectTag, relationTag,"http://example.org/kgolap/object-model#wingspanInterpretation", "http://example.org/kgolap/object-model#grouping", "<urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod>");
		 Graph<Object, Relation> resultGraph = NQuadReader.aggregatePropertyValues(replacedGraph, jsc, objectTag, relationTag, 
		"http://example.org/kgolap/object-model#wingspan", "<urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod>", "SUM"); 
		 
			long sum1 = resultGraph.triplets().toJavaRDD().filter(x -> x.dstAttr().toString().contains("3061074470")).count();
			long sum2 = resultGraph.triplets().toJavaRDD().filter(x -> x.dstAttr().toString().contains("1227604772")).count();
			
			Assert.assertEquals(1, sum1);
			Assert.assertEquals(1, sum2);
	}
	
	@org.junit.Test
	public void testAggregateByPropertyGroupByCOUNT() {
		String fileName = "aggregateByProperty.nq";
		Graph<Object, Relation> graph = GraphGenerator.generateGraph(jsc, objectTag, relationTag, path, fileName);
		 Graph<Object, Relation> replacedGraph 
		 = NQuadReader.groupByProperty(graph, jsc, objectTag, relationTag,"http://example.org/kgolap/object-model#wingspanInterpretation", "http://example.org/kgolap/object-model#grouping", "<urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod>");
		 Graph<Object, Relation> resultGraph = NQuadReader.aggregatePropertyValues(replacedGraph, jsc, objectTag, relationTag, 
		"http://example.org/kgolap/object-model#wingspan", "<urn:uuid:cca945a9-1aa3-41ef-86ba-72074cc46b86-mod>", "COUNT"); 
		 
			long count = resultGraph.triplets().toJavaRDD().filter(x -> x.dstAttr().toString().equals("2")).count();
			
			Assert.assertEquals(2, count);
	}

}
