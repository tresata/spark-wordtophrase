package com.tresata.spark.wordtophrase

import com.twitter.algebird.{ CMS, CMSMonoid }
import com.twitter.algebird.CMSHasherImplicits._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{ DataFrame }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StructField, StringType }
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

/** This implementation follows section 4: Learning Phrases from this paper: http://arxiv.org/pdf/1310.4546.pdf */
object WordToPhrase {

  type Tokenize = String => TraversableOnce[String]

  /** CountMinSketch parameters
   *
   * @param epsilon One-sided error bound on the error of each point query, i.e. frequency estimate.
   * @param delta A bound on the probability that a query estimate does not lie within some small interval (an interval that depends on `epsilon`) around the truth.
   * @param seed A seed to initialize the random number generator used to create the pairwise independent hash functions.
   */
  case class CMSParams(epsilon: Double = 1E-3, delta: Double = 1E-10, seed: Int = 1)

  /** Score parameters
   *
   * @param delta: discounting coefficient - prevents too many phrases consisting of very infrequent words to be formed
   * @param threshold: minimun score necessary to form a phrase
   */
  case class ScoreParams(delta: Double, threshold: Double)

  /** Run count min sketch on both unique words and word combinations */
  def countWordPhrases(token: Tokenize, params: CMSParams): RDD[String] => CMS[String] = { rdd =>
    val tokens = rdd.flatMap { s: String => token(s).toIterator ++ token(s).toIterator.sliding(2).map(_.mkString(" ")) }
    val cmsMonoid = new CMSMonoid[String](params.epsilon, params.delta, params.seed)
    tokens.aggregate(cmsMonoid.zero)((cms, token) => cmsMonoid.plus(cms, cmsMonoid.create(token)), cmsMonoid.plus)
  }

  /* Score each phrase and join the high scoring one's */
  def calculateScore(token: Tokenize, cms: Broadcast[CMS[String]], params: ScoreParams): String => String = { s =>
    new Iterator[String] {
      val buff = token(s).toIterator.buffered
      override def hasNext: Boolean = buff.hasNext
      override def next: String = {
        val x  = buff.next()
        if (buff.hasNext) {
          val head = buff.head
          val score = (cms.value.frequency(s"${x} ${head}").estimate - params.delta) / (cms.value.frequency(x).estimate * cms.value.frequency(head).estimate)
          if (score >= params.threshold) {
            buff.next() // want to skip 'head' since it was already combined
            s"${x}_${head}"
          } else x
        } else x
      }
    }.mkString(" ")
  }

  /* Apply method for RDD */
  def apply(rdd: RDD[String], cmsParams: CMSParams, scoreParams: ScoreParams, token: Tokenize): RDD[String] = {
    val broadcastCMS = rdd.context.broadcast(countWordPhrases(token, cmsParams)(rdd))
    rdd.map(calculateScore(token, broadcastCMS, scoreParams))
  }

  /* Apply method for DataFrame */
  def apply(df: DataFrame, field: String, cmsParams: CMSParams, scoreParams: ScoreParams, token: Tokenize): DataFrame = {
    val sqlc = df.sqlContext
    import sqlc.implicits._

    val rdd = df.select(field).as[String].rdd
    val broadcastCMS = rdd.context.broadcast(countWordPhrases(token, cmsParams)(rdd))
    val scoreUDF = udf(calculateScore(token, broadcastCMS, scoreParams))
    df.withColumn(field, scoreUDF(df(field)))
  }

  /* Iterative apply method for RDD */
  def apply(rdd: RDD[String], cmsParams: CMSParams, scoreParams: ScoreParams, token: Tokenize, iterations: Int, storageLevel: StorageLevel): RDD[String] = {
     var result = rdd
     (1 to iterations).foreach { i =>
       val broadcastCMS = result.context.broadcast(countWordPhrases(token, cmsParams)(result))
       val newResult = result.map(calculateScore(token, broadcastCMS, scoreParams))
       newResult.persist(storageLevel)
       newResult.foreach{ _ => () } // trigger persistence
       broadcastCMS.unpersist()
       if (i > 1)
         result.unpersist(false)
       result = newResult
     }
     result
  }

  /* Iterative apply method for DataFrame - works on RDD to avoid persisting entire DF over each iteration */
  def apply(df: DataFrame, field: String, cmsParams: CMSParams, scoreParams: ScoreParams, token: Tokenize, iterations: Int, storageLevel: StorageLevel): DataFrame = {
    val sqlc = df.sqlContext
    val sc = sqlc.sparkContext
    import sqlc.implicits._

    val rdd = df.select(field).as[String].rdd
    val dfNoField = df.drop(field)
    val result = apply(rdd, cmsParams, scoreParams, token, iterations, storageLevel)
    val rows = result.zip(dfNoField.rdd).map { case (str, row) => Row.fromSeq(str +: row.toSeq) }
    val schema = StructType(StructField(field, StringType, true) +: dfNoField.schema.fields)
    sqlc.createDataFrame(rows, schema).select(df.columns.head, df.columns.tail: _*)
  }
}
