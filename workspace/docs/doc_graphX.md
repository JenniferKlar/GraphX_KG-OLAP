# **NQuadReader**

**getIdOfObject**

This methods basically creates a UUID which is a hash of a value since GraphX Vertices have to have an identifier and then their value

Therefore a Vertex basically is a tuple consisting of the value (the value that is contained in the base dataset, most of the time it’s already some sort of a URI) and then the MD5 hash of this value at the identifier: _f = Tuple2 (MD5(value), value)_

**Operations - general**

Each method is one OLAP-function

For each OLAP-function there are two different methods

One uses the “ArrayList < String > contexts” parameter and therefore the operation is only done on all of the contexts that are defined in this list – the dataset is basically filtered by looking whether the RDF statement belongs to a context that is in the list

The other one ignores contexts and executes the operation on the whole dataset

Whenever the context list is used, it is transformed into a broadcast variable so that it can be used by all tasks at the same time when needed (My research said that was the only way to make it faster to go through collected lists in Spark)

Generally speaking all the transformations have some sort of a similar logic:

 - Filtering triplets for a certain relationship type or subject type
 - Mapping the found triplets to new Vertices and or new Edges (by generating new values or replacing individuals with others)
 - Adding or replacing the Vertices/Edges with the new ones
 - Generating a new graph

**Illustrations – general**

Illustrations are going to be slightly adapted however in general they do show the truth

At the moment for all illustrations in this document the following note has to be kept in mind:

***The type relationship is represented as an actual Edge, in the current implementation however, that would not look like that but the type relation is just added as a Vertex-attribute directly***

**Reification – method “reify”**


![Reification](Reification.png)


 - This method basically performs reification on RDF statements that
   contain a certain predicate
   
 - and therefore creates a new Vertex (that represents the statement
   itself),
 - another Vertex that represents the “type” of the
   statement 
 - and also three new Edges that connect the statement-Vertex to it's orginal subject-Vertex, the original object-Vertex and the type-Vertex


the steps in the code are therefore as follows:

 - filter all the triplets of the graph that have the specified predicate as Edge attribute
 - create the type-Vertex (the type-Vertex value has to be specified and the identifier is then generated with the getIdOfObject method)
 - the type-Vertex is put in a list and parallelized to make it into a RDD (since it later has to be joined with the other Vertices)
 - then all of the filtered statements from the first step are mapped to a new tuple4 which contains:
	 - the ID of the subject-Vertex of the statement
	 - the ID of the object-Vertex of the statement
	 - a new resource that represents the statement as an individual (Vertex attribute)
	 - the context for which the statement is valid
 - those tuple4s are then further processed by adding a 5th attribute (making it a Tuple5) which is the ID of the newly generated statement individual which is generated with the “getIDofObject” method
 - out of the tuple5 then the new Vertices are created for the statement which then results in a Vertex of the following form: ***(ID, Statement-value)***
 - for the new Edges three mappings from the tuple5-RDD are needed
	 - new Edges with the statement as the Source Vertex (subject), the subject-relation as the predicate (Edge value) as well as the context, and the subject of the statement as the Destination Vertex (object)
	 - new Edges with the statement as the Source Vertex (subject), the object-relation as the predicate (Edge value) as well as the context, and the object of the statement as the Destination Vertex (object)
	 - new Edges with the statement as the Source Vertex (subject), the type-relation as the predicate (Edge value) as well as the context, and the generated type-Vertex as the Destination Vertex (object)
 - then all the already existing Edges and Vertices are joined with the new ones and a new transformed graph is created.

*C and D in the illustration are the newly generated statement-objects, “usage-type” is the newly generated type-Vertex*
  

**Pivot**


![Pivot](Pivot.png)


the steps in the code are as follows:

 - Filter all the triplets of the graph that contain the
   dimensionProperty as a predicate (e.g. “hasLocation”, “hasDate”,..)
   and map those to new tuple which contain
   
 - The source Attribute + ‘mod’ (this then is the relevant context which is needed for the generation of new Edges later)
 - The ID of the destination (object) Vertex (this is the value of the dimension e.g. ‘Linz’ or some date)
 - Those tuples are put into a hashmap since they will later be used to find the correct dimension value depending on the context for which it is applicable (like a lookup table)
 - Then the triplets are filtered again by finding statements in which the subject is of a certain type (selectionCondition) for example “ManoeuvringAreaAvailability”
 - Those filtered statements are then mapped to tuples with the following values
	 - The ID of the subject
	 - The relevant context
	 - ("distinct()" is used since otherwise too many Edges would be created later since there are a lot of statements where the fltered individual is the subject)
 - Then those tuples are mapped to new Edges with
	 - The subject as the Source (Subject) Vertex
	 - The applicable destination (e.g. Linz) depending on the contexts which is looked up in the hashmap as Destination Vertex
	 - A new relation (Edge) containing the pirvotProperts (e.g. ‘object-model#hasLocation’)
 - Then the new Edges are added to the old ones and a new transformed graph is created.
 - As it can be seen in the graph everything basically stays the same just that the subject-Vertices that are of type ManoeuvringAreaAvailability get a new Edge (within their context = module) that points to the dimension value (e.g. Linz)  

**aggregatePropertyValues (value-generating abstraction)**


![aggregatePropertyValue_after_groupByProperty](aggregatePropertyValue_after_groupByProperty.png)


![aggregatePropertyValue_after_replcaeByGrouping](aggregatePropertyValue_after_replcaeByGrouping.png)


the steps in the code are as follows:

 - Filter all the triplets that contain the aggregateProperty as predicate and map them to a pairRDD (basically a RDD containing Tuples with key and value) that contains:
	 - A tuple with the source-Vertex (subject) of the triplet and the context of the (Edge)triplet
	 - The destination-Vertex (object) attribute (here we assume that it is always numeric)

The next steps are dependent on the type of aggregation that should be performed (sum, count, average, max or min)

 - New Edges are created by mapping the tuples in the pairRDD (that are of the form (tuple, numeric value)) to Edges where the Source (subject) of the tuple within the tuple stays the subject and the numeric value is aggregated depending on the type (e.g. when counting, the numeric value is mapped to 1 and then just summed, when summing the actual values are summed and so on)
 - New Vertices are created by doing the same aggregation/calculation and then storing the calculated result as the value of a the new Vertex and generating an ID by again using the getIDOfobject method
 - Then the new Edges and Vertices are added to the old ones (the statements that were aggregated are removed) and a new transformed graph is created

**Group by property (individual generating abstraction**


![GroupByProperty](GroupByProperty.png)


the steps in the code are as follows:

 - Filter all triplets that contain the groupingProperty as predicate (the destination Vertex is the object by which the subjects of the triplets should later be grouped)
 - Map those to new Tuples that represent the “Group” by just adding the String “-Group” to the already existing attribute value
 - For each filtered triplet: generate a new Edge that represents the link between the subjects and their grouping Vertex they belong to
 - Put all the subjects that should be replaced and their replacement-Group-Vertex into a hashmap
 - Then go through all triplets of the graph and find statements where individuals are present that should be replaced (according to what is in the hashmap) and create new Edges for all tuplets where either the Verteuces are replaced or stay the same
 - Created a new graph from the new Edges and old and new Vertices

**Replace by Grouping (triple generating abstraction)**


![ReplaceByGrouping](ReplaceByGrouping.png)


the steps in the code are as follows:

 - Filter all triplets that contain the groupingValue as the predicate (Edge attribute) and where the Source Vertex (subject) is of a certain type (replacementObject) and map them to Edges
 - Put all of them (the subjects that should be replaced and their replacement) into a hashmap
 - Then go through all triplets of the graph and find statements where individuals are present that should be replaced (according to what is in the hashmap) and create new Edges for all tuplets where either the Verteuces are replaced or stay the same
 - Create a new graph out of the old Vertices and the new Edges

# **GraphGenerator**

The Graph Generator class is basically there to create a new initial base Graph directly from the data source. It therefore in my case takes an NQ-File and analyses each row. It creates GraphX Vertices from all the subjects and objects in the RDF statements, then it created Edges using the created Vertices and assigning the RDF-predicate as the value of the GraphX Edge. Also the Graphname within the RDF statements is also stored in the Edge as an additional Attribute.

It is important to note that in general every RDF statement is translated into one GraphX Edge. However all RDF statements that describe a type-relation (predicate = [http://www.w3.org/1999/02/22-rdf-syntax-ns#type](http://www.w3.org/1999/02/22-rdf-syntax-ns#type) is not stored as an Edge but the type property is directly stored with the subject-Vertex.

**getJavaRDD**

This method loads a nquad-File and reads every line of it and creates a quadriple (class quad) for each valid line (not empty, not a comment, length >1) and stores it in a JavaRDD< Quad > which is then returned

**generateGraph**

 - the generateGraph method first uses the getJavaRDD method to generate the JavaRDD of quads.
 - Then all statements are are searched that do not contain the type-relation as predicate.
 - All subjects of such statements are mapped to a new Vertex (of type Resource since there can only be Resources and no Literals in the subject of a RDF statement)
 - All objects are also mapped to a Vertex but it is first checked whether it is a literal (then the literal value is used as the Vertex attribute) or if it is also a resource (then the whole resource – the uri – is used as the Vertex attribute)
 - Then all statements with the type-relation are searched and mapped to
   new tuples with ID and the Vertex itself which is constructed by the
   subject itself and also the object of the type-relationship statement
   
 - Then the Vertices without a type are used and the Vertices with a type are "substracted" (removed) from them since we want to use the ones that have the type as an attribute rather than the ones that do not have this information
 -  then the rest of the Vertices without a type that stayed after the substraction are combined with the Vertices that have a type
 -  those are then mapped to new tuple2 of type < Object, Object> since otherwise GraphX does not allow graph generation
 - then the graph is generated out of the Edges and Vertices and returned
   

**Relation**

 - Edges are of the Type “Relation”. The relation class represents the
   relation between two Vertices  - or rather the attributes of the
   Edge. The attributes that are used are:
	
	 - The “relationship” which is basically the predicate of the original
   RDF statement
   
	 - The “Context” which is the URI of the named graph the relation
   belongs to
  
	 - The “targedatatype” which stores either the type of the literal   
   (Integer, String, Boolean,..) of the object of a RDF statement or the
   String “Resource” when it’s not a literal but a resource (URI) so   
   that when aggregating for example it could be made sure that only   
   integer values are summed up and so on (this was an idea at the   
   beginning however it is not really used at the moment but I guess it 
   is still good to have)

# **Vertex – Resource – Literal**

I used the Vertex interface in order to be able to create the initial Graph in “GraphGenerator” where I have some functions that accept Resources AND Literals in the same collection so I needed some sort of interface or abstract class as a placeholder (Maybe there is a better way to do so or to use the interface differently or change it to an abstract class?)

The Classes Resource and Literal then implement the Interface.