# Three-dimensional Geospatial Interlinking with JedAI-spatial

[![DOI](https://zenodo.org/badge/488920144.svg)](https://zenodo.org/badge/latestdoi/488920144)

## Datasets

All real-world datasets that are used in the experimental analysis of [JedAI-spatial](https://arxiv.org/abs/2205.01905) are publicly available [here](https://zenodo.org/record/6384164#.YnOkk1BByUk).

### Supported Geometry Types
- 1D Linestrings/Polylines
- 2D Polygons

## Run Serial Experiments

See instructions [here](serial/README.md).

## Run Parallel Experiments

See instructions [here](parallel/README.md).

## Run JedAI-spatial Docker

The Docker file for JedAI-spatial Web application is available [here](https://drive.google.com/file/d/11ZiiFgAh2kvcBURwTj6ozsLlAbdz3Qal/view?usp=sharing).

### Load Docker from TAR

	sudo docker load < geolinker-docker.tar 

### Execute Docker

	sudo docker run -e JAVAOPTIONS=‘-Xmx4g’ -p 8080:8080 geolinker
	

## GUI

<p  align="center">
<img  src="https://github.com/gpapadis/JedAI-spatial/blob/main/documentation/JS-gui.gif">
</p>

## Run JedAI-spatial serial

To compile and run JedAI-spatial serial:

1. Change to the directory that serial source code resides in

		cd serial
		
2. Build JedAI-spatial serial using maven

		mvn clean pacakge
		
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
2. TSV files are not working properly. When operation on TSV files the serial version of JedAI-Spatial may get stuck and stop functioning. The CSV counterparts of the files seem to work fine.
3. The JedAI-Spatial GUI, is not fully functional. One major problem is that results cannot be exported.

