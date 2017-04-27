# Mastering Data Science with Spark

Mirrored from https://bitbucket.org/gzet_io/profilers

Chapter 4 presents a selection of tools, techniques and methods for "data profiling" at scale using Spark architectures. These ostensibly basic practices are the essence of good data handling and provide not only the building blocks needed to advance understanding of data, but also underpin the mechanisms required to regulate data quality, adapt to data drift and ultimately transform "dark data" into actionable insights.

## Dependencies

The project itself is a child project of [gzet-parent](https://bitbucket.org/gzet_io/gzet-parent) where we manage shared dependencies versions and testing framework across all our different modules.

```xml
<parent>
    <artifactId>gzet-parent</artifactId>
    <groupId>io.gzet</groupId>
    <version>1.0</version>
</parent>
```

## Installation

As a maven project, gzet-profilers can be packaged as follows

```bash
git clone git@bitbucket.org/gzet_io/profilers.git
cd profilers
mvn clean package
```

This project depends on [GZET Parent](https://bitbucket.org/gzet_io/gzet-parent)
Please make sure you installed it first using a simple `mvn clean install`

## Configuration


## Usage


```
spark-submit --class io.gzet.App profilers-1.0.0.jar <your data file> <delimiter>
```


## Author

gzet_io

## Version

1.0
