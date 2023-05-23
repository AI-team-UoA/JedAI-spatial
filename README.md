# Three-dimensional Geospatial Interlinking with JedAI-spatial

[![DOI](https://zenodo.org/badge/488920144.svg)](https://zenodo.org/badge/latestdoi/488920144)

**JedAI-spatial** is an open-source system for computing topological relations according to the DE9IM model between datasets with geometric entities.
It consists of three modules:
1. The serial one, which runs on a single CPU with Java 8 or later
2. The parallel one, based on Scala 2.11.12 and Apache Spark 2.4.4
3. The GUI
We describe each module in more detail in the following. Please 
For more details, please refer to the technical report [Three-dimensional Geospatial Interlinking with JedAI-spatial)](https://arxiv.org/pdf/2205.01905.pdf).
# 1. Serial Processing

This component comprises the state-of-the-art methods for Geospatial Interlinking in the literature.

All methods implement the Filtering-Verification framework, which consists of two consequtive steps:

1. Filtering 
2. Verification

The **Filtering step** generates a set of candidate pairs, based on the minimum bounding rectangle (**MBR**) of each geometry.

The **Verification step** examines every candidate pair as long as the MBRs of its constituent geometries are intersecting. The detected topological relations
are added to the output set of triples.

Based on the type of the index used in Filtering, JedAI-spatial organizes the available algorithms into three categories:

1. Grid-based algorithms build an Equigrid by dividing the Earth’s surface into cells (a.k.a. tiles) of the same dimensions. Every geometry is placed into the tiles that intersect its MBR.
2. Partition-based algorithms sort all input geometries in ascending order of their lower boundary on the horizontal axis and move a sweepline across the Earth’s surface. Geometry pairs whose MBRs simultaneously intersect the sweep line are verified.
3. Tree-based Algorithms build a tree index for the source geometries and for each target geometry verify the candidates in the leaf nodes with intersecting MBRs.

In more detail, the following algorithms are implemented per category:

#### 1. Grid-based algorithms

1. [RADON](/serial/src/main/java/batch/tilebased/RADON.java)
    - Filtering: Loads both datasets into main memory and indexes all geometries.
    - Verification: Computes the Intersection Matrix for all candidate pairs.
2. [Static RADON](/serial/src/main/java/batch/tilebased/StaticRADON.java)
    - Same as RADON, but the size of the Equigrid is determined by the user.
3. [GIA.nt](/serial/src/main/java/batch/tilebased/GIAnt.java)
    - Filtering: Loads only the input dataset with the fewer geometries into main memory.
    - Verification: Reads input geometries of the other dataset from disk and computes the Intersection Matrix.
4. [Static GIA.nt](/serial/src/main/java/batch/tilebased/StaticGIAnt.java)
    - Same as GIA.nt, but the size of the Equigrid is determined by the user.

#### 2. Partition-based algorithms

1. [Plane Sweep](/serial/src/main/java/batch/planesweep/PlaneSweep.java)
    - Applies the approach of partition-based algorithms to all input geometries.
2. [PBSM](/serial/src/main/java/batch/partitionbased/PBSM.java)
    - Splits the geometries into manually defined partitions and applies Plane Sweep on each one.
3. [Stripe Sweep](/serial/src/main/java/batch/stripebased/StripeSweep.java)
    - The Filtering step sorts only the source geometries.
    - The Verification step probes every target geometry against the stripes and aggregates the set of the source geometries that intersect the same stripes with the target ones.

#### 3. Tree-based algorithms

1. [R-Tree](/serial/src/main/java/batch/treebased/RTree.java)
2. [Quadtree](/serial/src/main/java/batch/treebased/QuadTree.java)
3. [CR-Tree](/serial/src/main/java/batch/treebased/CRTree.java)

The scalability experiments were performed using [this class](/serial/src/main/java/experiments/ScalabilityExperiments.java).

# 2. Parallel Processing

This component comprises parallel methods from famous spatial processing systems in the literature

### Tools

1. Scala 2.11.12  
2. Apache Spark 2.4.4

### How to run

In order to run an experiment on the cluster, one has to first build a fat jar (Java Archive) file using the command `sbt assembly` and provide the configuration for the execution according to the configuration file at `config/configurationTemplate.yaml`. 

Process: 

1. `$ sbt assembly`
2. `$ spark-submit --master <master> --class experiments.<ParallelMethodExperiment> target/scala-2.11/DS-JedAI-assembly-0.1.jar <options> -conf </path/to/configuration.yaml>`

The available `<ParallelMethodExperiment>` are:

- `GeoSparkExp`
- `SedonaExp`
- `SpatialSparkPartitionedExp`
- `SpatialSparkExp`
- `LocationSparkExp`
- `MagellanExp`

### Methods

Each parallel implementation is divided in three consecutive phases:

1. Preprocessing Stage
2. Global Join Stage
3. Local Join Stage

The **Preprocessing Stage** prepares the data for the main processing phases. First of all, the source and target datasets are read from the HDFS in order to be transformed into RDDs. Then, they are split into logical/physical partitions based on a partitioning method. This is a classic technique in Apache Spark. Remember that it is a distributed framework running on a cluster. The main idea is to group the data into partitions so as to reduce the Spark shuffles and hence increase the performance. A Spark shuffle is when data is rearranged between partitions. 

In more detail, each framework uses its own set of partitioning techniques, such as Quad Trees or Z-Order Curves, in order to partition the source data. Then, the geometries of both source and target RDD datasets are assigned an identifier based on the partitions they lie in space. The RDD is shaped as `<K,V>` where K is the identifier of the partition and V the geometry. In case a geometry belongs to multiple partitions, it is assigned multiple identifiers, i.e. `<K1,V>`, `<K2,V>`.

In the **Global Join Stage**, the source and target RDDs are joined in the same partitions based on their key K, the partition identifier. Each partition is a processing unit in the cluster. The joined RDD is shaped as `<K,V>`, with V changing to a tuple of `(Iterable[SourceGeometries], Iterable[TargetGeometries])`.

The third and final phase, **Local Join Stage**, performs the spatial join on the candidate geometries within the same partition. Two are the available techniques, each having two steps:

- Nested Loop Index Join: 
	- Filtering Step: Identifies all intersecting MBRs between target and source candidates using a Spatial Index built from the `Iterable[SourceGeometries]`.
	- Verification Step: Verify the topological relations of each pair of source-target candidates.

- Nested Loop Join:
	- Filtering Step: Filter all the intersecting MBRs between target and source candidates by comparing each target from `Iterable[TargetGeometries]` with  each source geometry from `Iterable[SourceGeometries]`.
	- Verification Step: Verify the topological relations of each pair of source-target candidates.

#### 1. Spatial Spark [1], [2]

Spatial Spark has two implementations, a Partitioned Join (process afore-mentioned) and a Broadcast Join. The Broadcast Join leverages the Apache Spark's broadcast variables, however it is not useful for big datasets due to KryoSerializer's 2GB maximum buffer value. 

For the experiments on the paper, we solely used the Partitioned Join variant. It supports 3 partitioning techniques (Binary Tree, Fixed Grid and STR partitioning) and Nested Loop Local Join.

Hereby, we present sample configuration files for each method, which we used in our experiments for the D1 dataset:

```
source:
  path: "hdfs:///<path>/AREAWATER.tsv"
  realIdField: "2"
  geometryField: "0"

target:
  path: "hdfs:///<path>/LINEARWATER.tsv"
  realIdField: "2"
  geometryField: "0"

relation: "DE9IM"

configurations:
  partitions: "400"
  spatialSparkMethod: "FGP"
  spatialSparkMethodConf: "512:512"
```

#### 2. GeoSpark - Apache Sedona [3], [4], [5]

Apache Sedona supports two partitioning techniques (KDB-Tree and Quad-Tree) and both a Nested Loop Join and a Nested Loop Index Join (R-Tree and QuadTree).

Bear in mind, JedAI-spatial has integrated GeoSpark as well.

Hereby, we present sample configuration files for each method, which we used in our experiments for the D1 dataset:

```
source:
  path: "hdfs:///<path>/AREAWATER.tsv"
  realIdField: "2"
  geometryField: "0"

target:
  path: "hdfs:///<path>/LINEARWATER.tsv"
  realIdField: "2"
  geometryField: "0"

relation: "DE9IM"

configurations:
  sedonaGridType: "KDBTREE"
  sedonaLocalIndexType: "RTREE"
  partitions: "400"
```

#### 3. Location Spark [6], [7], [8]

Location Spark is the only spatial frameworks that uses a form of Skew Analysis. It supports three partitioning techniques(EqualGrid and QuaTree) and a Nested Loop Index Join (R-Tree, EqualGrid, QuadTree).

Hereby, we present sample configuration files for each method, which we used in our experiments for the D1 dataset:

```
source:
  path: "hdfs:///<path>/AREAWATER.tsv"
  realIdField: "2"
  geometryField: "0"

target:
  path: "hdfs:///<path>/LINEARWATER.tsv"
  realIdField: "2"
  geometryField: "0"

relation: "DE9IM"

configurations:
  locationSparkLocalIndexType: "QUADTREE"
  partitions: "400"
```

#### 4. Magellan [9]

Magellan utilizes Z-Order Curves for partitioning the data and a Nested Loop Join for spatial join.

Hereby, we present sample configuration files for each method, which we used in our experiments for the D1 dataset:

```
source:
  path: "hdfs:///<path>/AREAWATER.tsv"
  realIdField: "2"
  geometryField: "0"

target:
  path: "hdfs:///<path>/LINEARWATER.tsv"
  realIdField: "2"
  geometryField: "0"

relation: "DE9IM"

configurations:
  partitions: "400"
  magellanZOrderCurvePrecision: "20"
```

### References

1. S. You, J. Zhang, and L. Gruenwald, “Large-scale spatial join query processing in Cloud,” in 2015 31st IEEE International Conference on Data Engineering Workshops, 2015.
2. https://github.com/syoummer/SpatialSpark
3. J. Yu, Z. Zhang, and M. Sarwat, “Spatial data management in apache spark: the GeoSpark perspective and beyond,” Geoinformatica, vol. 23, no. 1, pp. 37–78, 2019.
4. J. Yu, J. Wu, and M. Sarwat, “GeoSpark: A cluster computing framework for processing large-scale spatial data,” in Proceedings of the 23rd SIGSPATIAL International Conference on Advances in Geographic Information Systems - GIS ’15, 2015.
5. https://sedona.apache.org/
6. M. Tang, Y. Yu, Q. M. Malluhi, M. Ouzzani, and W. G. Aref, “LocationSpark: A distributed in-memory data management system for big spatial data,” Proceedings VLDB Endowment, vol. 9, no. 13, pp. 1565–1568, 2016.
7. M. Tang, Y. Yu, A. R. Mahmood, Q. M. Malluhi, M. Ouzzani, and W. G. Aref, “LocationSpark: In-memory distributed spatial query processing and optimization,” Front. Big Data, vol. 3, p. 30, 2020.
8. https://github.com/purduedb/LocationSpark
9. https://github.com/harsha2010/magellan

## 3. GUI

<p  align="center">
<img  src="https://github.com/gpapadis/JedAI-spatial/blob/main/documentation/JS-gui.gif">
</p>


## Datasets

All real-world datasets that are used in the experimental analysis of [JedAI-spatial](https://arxiv.org/abs/2205.01905) are publicly available [here](https://zenodo.org/record/6384164#.YnOkk1BByUk).

### Supported Geometry Types
- 1D Linestrings/Polylines
- 2D Polygons

## Run JedAI-spatial Docker

The Docker file for JedAI-spatial Web application is available [here](https://drive.google.com/file/d/11ZiiFgAh2kvcBURwTj6ozsLlAbdz3Qal/view?usp=sharing).

### Load Docker from TAR

	sudo docker load < geolinker-docker.tar 

### Execute Docker

	sudo docker run -e JAVAOPTIONS=‘-Xmx4g’ -p 8080:8080 geolinker
	
## Run JedAI-spatial serial

To compile and run JedAI-spatial serial:

1. Change to the directory that serial source code resides in

		cd serial
		
2. Build JedAI-spatial serial using maven

		mvn clean package
		
3. Execute the program through the CLI class

		java -cp target/geospatialinterlinking-1.0-SNAPSHOT-jar-with-dependencies.jar workflowManager.CommandLineInterface
		
4. Through the CLI select input and target files, execution method and wether to export results or not
5. If the choice to export the results was selected, the output file will be a NTRIPLES file of the following format:
```
<0> <http://www.opengis.net/ont/geosparql#sfContains> <1> .
<0> <http://www.opengis.net/ont/geosparql#sfCoverdBy> <1> .
<0> <http://www.opengis.net/ont/geosparql#sfCovers> <1> .
<0> <http://www.opengis.net/ont/geosparql#sfEquals> <1> .
<0> <http://www.opengis.net/ont/geosparql#sfIntersects> <1> .
<0> <http://www.opengis.net/ont/geosparql#sfWithin> <1> .
<1> <http://www.opengis.net/ont/geosparql#sfContains> <2> .
<1> <http://www.opengis.net/ont/geosparql#sfCoverdBy> <2> .
<1> <http://www.opengis.net/ont/geosparql#sfCovers> <2> .
<1> <http://www.opengis.net/ont/geosparql#sfEquals> <2> .
<1> <http://www.opengis.net/ont/geosparql#sfIntersects> <2> .
<1> <http://www.opengis.net/ont/geosparql#sfWithin> <2> .
<2> <http://www.opengis.net/ont/geosparql#sfContains> <3> .
<2> <http://www.opengis.net/ont/geosparql#sfCoverdBy> <3> .
<2> <http://www.opengis.net/ont/geosparql#sfCovers> <3> .
<2> <http://www.opengis.net/ont/geosparql#sfEquals> <3> .
<2> <http://www.opengis.net/ont/geosparql#sfIntersects> <3> .
...
```
In this (Subject, Predicate, Object) triples represent:

-Subject: the id of a geometry from the source file. The id given to each entity is related to its position in the source file. Ids from the source file start from 0. The first entity in the source file will have an id <0>, the second <1> etc.<br><br>
-Predicate: the discovered relation between subject and object.<br><br>
-Object: the id of a geometry from the target file. The id given to each entity is related to its position in the target file. Ids from the target file start from 1. The first entity in the source file will have an id <1>, the second <2> etc.<br><br>

## Comments

1. The ids from source and target files are different even when the source and target files are the same. Source files always start with ids from 0, while target files always start with ids from 1.
2. When operation on TSV files the serial version of JedAI-Spatial may get stuck and stop functioning. Please use the CSV counterparts of the input files instead.
3. The JedAI-Spatial GUI will be extended so that it can export the results.

