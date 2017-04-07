# Mastering Data Science with Spark

Using a sequence of tutorials that deliver a working news intelligence service, we explain advanced Spark architectures, unveil sophisticated data science methods, demonstrate how to work with geographic data in Spark, and explain how to tune Spark algorithms so they scale linearly. 

## Dependencies

The parsing of HTML page is delegated to the apache licensed [Goose Library](https://github.com/GravityLabs/goose) that I had to recompile for Scala 2.10.

```xml
<dependency>
    <groupId>com.gravity</groupId>
    <artifactId>goose_2.10</artifactId>
    <version>2.1.30</version>
</dependency>
```

The project itself is a child project of [gzet-parent](https://bitbucket.org/gzet_io/gzet-parent) where we manage shared dependencies versions and testing framework across all our different modules.

```xml
<parent>
    <artifactId>gzet-parent</artifactId>
    <groupId>io.gzet</groupId>
    <version>1.0</version>
</parent>
```

## Installation

As a maven project, gzet-scraper can be packaged as follows

```bash
git clone git@bitbucket.org:gzet_io/gzet-scraper.git 
cd gzet-scraper
mvn clean package
```

This project depends on both [GZET Parent](https://bitbucket.org/gzet_io/gzet-parent) and the modified version of Goose library. 
Please make sure you installed both of them first using a simple `mvn clean install`

## Configuration

```
gzet.geoname.input.hdfs="/Users/antoine/Workspace/gzet/data/geonames/GeoNames"
gzet.gdelt.input.hdfs="/Users/antoine/Workspace/gzet/data/gdelt/input"
gzet.gdelt.output.hdfs="/Users/antoine/Workspace/gzet/data/gdelt/output"
gzet.es.enable=true
gzet.es.nodes="localhost"
gzet.es.port=9200
gzet.es.resource="gzet/articles"
gzet.gdelt.html.partitions=10
```

You'll need to supply the path to a Serialized copy of GeoNames data. 
If you don't have such a copy, please download the following files and execute below code snippet on spark.

- [allCountries.zip](http://download.geonames.org/export/dump/allCountries.zip)
- [admin1CodesASCII.txt](http://download.geonames.org/export/dump/admin1CodesASCII.txt)

```scala
spark-shell --master local --driver-memory 3g --jars web-scraper-1.0-SNAPSHOT.jar
import io.gzet.GeoLookup
val geonames = GeoLookup.load(sc, "admin1CodesASCII.txt", "allCountries.txt")
geonames.repartition(100).saveAsObjectFile("/path/to/serialized/copy")
```

This also requires the Gdelt data (obviously). 
You can download 1 day worth of data from [Gdelt website](http://data.gdeltproject.org/gdeltv2/masterfilelist.txt). 
Alternatively, use a simple shell command below in the expected directory.

```bash
cd /path/to/gdelt/data
for URL in `lynx -dump http://data.gdeltproject.org/gdeltv2/masterfilelist.txt -width=300 | grep 20160110 | grep export.CSV | awk '{print $3}'` ; do wget $URL; done
for ZIP in `ls -1rt *.zip`; do unzip $ZIP ; rm $ZIP ; done
```

## Usage

Because the main app processes each gdelt file sequentially, processing a 15mn batch file takes around 2mn using below setup.

```
spark-submit --driver-memory 2g --master spark://pathogen:7077 --num-executors 2 --executor-cores 4 --executor-memory 4g
```

The application itself can be started as follows.

```
spark-submit --class io.gzet.App web-scraper-1.0-SNAPSHOT.jar
```

For information, it will iterate through all your gdelt files previously downloaded, process and delete them afterwards. 
It will save content on elasticsearch (if enabled) and on HDFS as a json format.
An example of output data can be found [here](resources/output.json). 

## Author

Antoine Amend, <antoine.amend@gmail.com>

## Version

1.0
