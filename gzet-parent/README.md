# Mastering Data Science on Spark

Mirrored from https://bitbucket.org/gzet_io/gzet-parent/overview

## Description

Using a sequence of tutorials that deliver a working news intelligence service, we explain advanced Spark architectures, unveil sophisticated data science methods, demonstrate how to work with geographic data in Spark, and explain how to tune Spark algorithms so they scale linearly. 

## GZET Parent

Compile / Install GZET parent project as follows

```
git clone git@bitbucket.org:gzet_io/gzet-parent.git
cd gzet-parent
mvn clean install
```

This will create a GZET parent project with core dependencies versions all set (e.g. Spark version), a Unit Test framework for Spark, and, most importantly, an archetype that can be used as a maven template for any GZET module. 

## GZET Module

Maven archetypes are the most convenient way of creating self packaged GZET modules. A GZET module called `my-awesome-module` can be created as follows. 

```
mvn archetype:generate \
    -DinteractiveMode=false \
    -DarchetypeGroupId=io.gzet \
    -DarchetypeArtifactId=gzet-archetype \
    -DarchetypeVersion=1.0 \
    -DgroupId=io.gzet \
    -DartifactId=my-awesome-module \
    -Dversion=1.0-SNAPSHOT
```

This child project of `gzet-parent` is a fully packaged Spark application (shaded jar) that comes wih a predefined `pom.xml` and a Spark test framework. 

### Main class (App)

```scala
package io.gzet

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf().setAppName("GZET"))
    sc.parallelize(1 to 100).collect.foreach(println)
  }

}
```

### Test class (AppTest)

```scala
package io.gzet

import io.gzet.test.SparkFunSuite

class AppTest extends SparkFunSuite {

  localTest("Test GZET") { sc =>
    val rdd = sc.parallelize(1 to 100)
    rdd.collect.foreach(println)
    rdd.max should be(100)
  }

}
```

All you need to do is to modify `pom.xml` with any 3rd part dependencies required for your module, add your Spark logic within `App.scala` and you're good to go!

```
cd my-awesome-module
mvn clean install
spark-submit --master local[*] --class io.gzet.App target/my-awesome-module-1.0-SNAPSHOT.jar arg1 arg2...
```

## Version
`1.0`

## Author

gzet.io





