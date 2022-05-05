# Serial Processing

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
