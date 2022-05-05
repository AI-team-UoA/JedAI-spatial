# Parallel Processing

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
