# geomesa-utils-1.5

At the time of writing, the current version of Geomesa supports Spark 1.5.
This repository contains tools for interacting with Geomesa, which has been configured for Spark 1.5, and can therefore be run independently of the Spark 2.0 code in the Chapter 05 repository.

**com.example.geomesa.gdelt** - A Java Map Reduce job to write gdelt data to Geomesa Accumulo.
Original code found here: http://www.geomesa.org/documentation/tutorials/geomesa-examples-gdelt.html

**io.gzet.geomesa.ingest.GeomesaAccumuloWrite** - A Spark job to write Gdelt Events to Accumulo.
**io.gzet.geomesa.ingest.GeomesaAccumuloRead** - A Spark job to read Geomesa Accumulo data.



