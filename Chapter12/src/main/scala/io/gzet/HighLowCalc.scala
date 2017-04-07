package io.gzet

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class HighLowCalc extends UserDefinedAggregateFunction {

  def inputSchema: org.apache.spark.sql.types.StructType = StructType(
    StructField("date", StringType) ::
      StructField("price", DoubleType) :: Nil)

  def bufferSchema: StructType = StructType(
    StructField("HighestHighDate", StringType) ::
      StructField("HighestHighPrice", DoubleType) ::
      StructField("LowestLowDate", StringType) ::
      StructField("LowestLowPrice", DoubleType) :: Nil
  )

  def dataType: DataType = DataTypes.createStructType(
    Array(
      StructField("HighestHighDate", StringType),
      StructField("HighestHighPrice", DoubleType),
      StructField("LowestLowDate", StringType),
      StructField("LowestLowPrice", DoubleType),
      StructField("firstPrice", DoubleType),
      StructField("secondPrice", DoubleType)
    )
  )

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
    buffer(1) = 0d
    buffer(2) = ""
    buffer(3) = 1000000d
  }

  def update(buffer: MutableAggregationBuffer,input: Row): Unit = {
    // if new value is higher than current high, update highprice and date
    // if new value is lower than current low, update lowprice and date
    // if new value is same as current high, update date if before current highdate
    // if new value is same as current low, update date if before current lowdate
    (input.getDouble(1) compare buffer.getAs[Double](1)).signum match {
      case -1 => {}
      case  1 => {
        buffer(1) = input.getDouble(1)
        buffer(0) = input.getString(0)
      }
      case  0 => {
        // if new date earlier than current date, replace
        (parseDate(input.getString(0)),parseDate(buffer.getAs[String](0))) match {
          case (Some(a), Some(b)) => {
            if(a.before(b)){
              buffer(0) = input.getString(0)
            }
          }
          case _ => {}
        }
      }
    }
    (input.getDouble(1) compare buffer.getAs[Double](3)).signum match {
      case -1 => {
        buffer(3) = input.getDouble(1)
        buffer(2) = input.getString(0)
      }
      case  1 => {}
      case  0 => {
        // if new date later than current date, replace
        (parseDate(input.getString(0)),parseDate(buffer.getAs[String](2))) match {
          case (Some(a), Some(b)) => {
            if(a.after(b)){
              buffer(2) = input.getString(0)
            }
          }
          case _ => {}
        }
      }
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // compare two buffers
    (buffer2.getDouble(1) compare buffer1.getAs[Double](1)).signum match {
      case -1 => {}
      case  1 => {
        buffer1(1) = buffer2.getDouble(1)
        buffer1(0) = buffer2.getString(0)
      }
      case  0 => {
        // if new date earlier than current date, replace
        (parseDate(buffer2.getString(0)),parseDate(buffer1.getAs[String](0))) match {
          case (Some(a), Some(b)) => {
            if(a.before(b)){
              buffer1(0) = buffer2.getString(0)
            }
          }
          case _ => {}
        }
      }
    }
    (buffer2.getDouble(3) compare buffer1.getAs[Double](3)).signum match {
      case -1 => {
        buffer1(3) = buffer2.getDouble(3)
        buffer1(2) = buffer2.getString(2)
      }
      case  1 => {}
      case  0 => {
        // if new date later than current date, replace
        (parseDate(buffer2.getString(2)),parseDate(buffer1.getAs[String](2))) match {
          case (Some(a), Some(b)) => {
            if(a.after(b)){
              buffer1(2) = buffer2.getString(2)
            }
          }
          case _ => {}
        }
      }
    }
  }

  def evaluate(buffer: Row): Any = {
    (parseDate(buffer.getString(0)), parseDate(buffer.getString(2))) match {
      case (Some(a), Some(b)) => {
        if(a.before(b)){
          (buffer(0), buffer(1), buffer(2), buffer(3), buffer(1), buffer(3))
        }
        else {
          (buffer(0), buffer(1), buffer(2), buffer(3), buffer(3), buffer(1))
        }
      }
      case _ => (buffer(0), buffer(1), buffer(2), buffer(3), buffer(1), buffer(3))
    }
  }

  def parseDate(value: String): Option[Date] = {
    try {
      Some(new SimpleDateFormat("yyyy-MM-dd").parse(value))
    } catch {
      case e: Exception => None
    }
  }
}
