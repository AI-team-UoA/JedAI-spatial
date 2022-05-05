# Three-dimensional Geospatial Interlinking with JedAI-spatial

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
