package io.gzet.crypto

import java.security.spec.AlgorithmParameterSpec
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import org.apache.hadoop.io.compress.CryptoCodec
import org.apache.spark.{SparkConf, SparkContext}


object EncryptionLib {



  def doFinal(cipher: Cipher): Array[Byte] = {
    cipher.doFinal()
  }

  def update(cipher: Cipher, value: String): Array[Byte] = {
    cipher.update(value.getBytes())
  }

  def doFinalBytes(cipher: Cipher, valu: Array[Byte]): Array[Byte] = {
    cipher.doFinal(valu)
  }


  def initEncrypt(key: String, initVector: String): Cipher = {
    val iv: AlgorithmParameterSpec = new IvParameterSpec(initVector.getBytes("UTF-8"))
    //    val skeySpec: SecretKeySpec  = new SecretKeySpec(key.getBytes("UTF-8"), "AES")
    val skeySpec: SecretKeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");

    val cipher: Cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
    cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv)
    cipher
  }

  def initDecrypt(key: String, initVector: String): Cipher = {
    val iv: AlgorithmParameterSpec = new IvParameterSpec(initVector.getBytes("UTF-8"))
    //    val skeySpec: SecretKeySpec  = new SecretKeySpec(key.getBytes("UTF-8"), "AES")
    val skeySpec: SecretKeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");

    val cipher: Cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
    cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv)
    cipher
  }

  def decrypt(key: Array[Byte], initVector: String, encrypted: Array[Byte]): Array[Byte] = {
    val iv: AlgorithmParameterSpec = new IvParameterSpec(initVector.getBytes("UTF-8"))
    //    val skeySpec: SecretKeySpec  = new SecretKeySpec(key.getBytes("UTF-8"), "AES")
    val skeySpec: SecretKeySpec = new SecretKeySpec(key, "AES")

    val cipher: Cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
    cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv)

    val original: Array[Byte] = cipher.doFinal(encrypted)

    original
  }


  def main(args: Array[String]): Unit = {

    //    EncryptionUtils.createJceksStore();
    //      val secretKey: SecretKey = EncryptionUtils.createSecretKey("password")
    //      EncryptionUtils.storeKey(secretKey)

//    EncryptionUtils.createJceksStoreAddKey

    // Spark job with Encryption
    val conf = new SparkConf()

    conf.set("spark.hadoop.io.compression.codecs", "org.apache.hadoop.io.compress.CryptoCodec")

    val sc = new SparkContext(conf.setAppName("Spark test"))
//    val rdd = sc.parallelize(List("This is the first line", "This is the second line", "This is the third line", "This is the forth line", "This is the fifth line", "This is the sixth line", "This is the seventh line", "This is the eigth line", "This is the ninth line", "This is the tenth line", "This is the eleventh line", "This is the twelth line","This is the thirteenth line", "This is the fourteenth line", "This is the fifthteenth line","This is the sixteenth line", "This is the seventeenth line", "This is the eighteenth line","This is the nineteenth line", "This is the twentyeth line", "This is the twentyfirst line","This is the twentysecond line", "This is the twentythird line", "This is the twentyforth line","This is the twentyfifth line", "This is the twentysixth line", "This is the twentyseventh line"), 1)
    val rdd = sc.textFile("/Users/uktpmhallett/Documents/gdelt/20150109.export.CSV")
    rdd.saveAsTextFile("/Users/uktpmhallett/Downloads/tempout.txt", classOf[CryptoCodec])

    val read = sc.textFile("/Users/uktpmhallett/Downloads/tempout.txt", 20)
    read.collect().foreach(println)

//    val secretKey: SecretKey = EncryptionUtils.createSecretKey("keystorepassword")
    val initVector: String  = "RandomInitVector"; // 16 bytes IV
////    println("Key: " + Hex.encodeHexString(secretKey.getEncoded))
//
//    val out = encrypt("keystorepassword", initVector, "Just a simple single line of text")
//    println(Hex.encodeHexString(out))
//
//    println(new String(decrypt("keystorepassword", initVector, out)))
//
//    println(new String(decrypt("keystorepassword", initVector, Hex.decodeHex("16ff3f1e008bd293d12fc0f163abb9d2e3a1327c61dbf5beb42434016784dee00d713171cb71528044b4ae7cfd28cede".toCharArray))))



    // working code
//    val iv: AlgorithmParameterSpec = new IvParameterSpec(initVector.getBytes("UTF-8"))
//    //    val skeySpec: SecretKeySpec  = new SecretKeySpec(key.getBytes("UTF-8"), "AES")
//    val skeySpec: SecretKeySpec = new SecretKeySpec("keystorepassword".getBytes("UTF-8"), "AES");
//
//    val cipher: Cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING")
//    cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv)
//
//    val input = "Just a simple single line of text".getBytes
//
//    val bArray = new ArrayBuffer[Byte]()
//    var output: Array[Byte] = cipher.update(input, 0, 10)
//    bArray ++= output
//    output = cipher.update(input, 10, input.length - 10)
//    bArray ++= output
//
//    val outputf = cipher.doFinal()
//    bArray ++= outputf
//
//    println(Hex.encodeHexString(bArray.toArray))
//
//    cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv)
//
//    println(new String(cipher.doFinal(bArray.toArray)))


  }

}
