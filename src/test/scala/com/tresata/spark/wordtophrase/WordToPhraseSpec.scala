package com.tresata.spark.wordtophrase

import org.scalatest.funspec.AnyFunSpec
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel

class WordToPhraseSpec extends AnyFunSpec {
  import WordToPhrase._
  import SparkSuite.{sc, sqlc}
  import sqlc.implicits._

  val rdd = sc.makeRDD(List(("we work sometimes"),
                            ("we dont play risk"),
                            ("we play super smash"),
                            ("we work sometimes")))
  val rddMulti = sc.makeRDD(List(("we work sometimes", "we promise"),
                            ("we dont play risk", "but we actually do"),
                            ("we play super smash", "a bit too much"),
                            ("we work sometimes", "we promise")))

  val df = rdd.toDF("words")
  val dfMulti = rddMulti.toDF("words", "moreWords")
  val cmsParams = CMSParams(epsilon= 1E-3, delta = 1E-10, seed = 1)
  val scoreParams = ScoreParams(delta = 1, threshold = 0.125)
  def token: Tokenize = _.split("\\W+")

  describe("Creating phrases from string combinations") {
    it("should run with an RDD") {
      val phrases = WordToPhrase(rdd, cmsParams, scoreParams, token)
      assert(phrases.collect().take(1) === Seq("we_work sometimes"))

      val secondPhrases = WordToPhrase(phrases, cmsParams, scoreParams, token)
      assert(secondPhrases.collect().take(1) === Seq("we_work_sometimes"))
    }

    it("should run on a DataFrame that has a single column") {
      val phrases = WordToPhrase(df, "words", cmsParams, scoreParams, token)
      assert(phrases.columns.toSeq  === Seq("words"))
      assert(phrases.collect().take(1) === Seq(Row("we_work sometimes")))
    }

    it("should run on a DataFrame with multiple columns") {
      val phrases = WordToPhrase(dfMulti, "words", cmsParams, scoreParams, token)
      assert(phrases.columns.toSeq  === Seq("words", "moreWords"))
      assert(phrases.collect().take(1) === Seq(Row("we_work sometimes", "we promise")))
    }

    it("should run iteratively on an RDD") {
      val phrases = WordToPhrase(rdd, cmsParams, scoreParams, token, 2, StorageLevel.MEMORY_ONLY)
      assert(phrases.collect().take(1) === Seq("we_work_sometimes"))
    }

    it("should run iteratively on a DataFrame with a single column") {
      val phrases = WordToPhrase(df, "words", cmsParams, scoreParams, token, 2, StorageLevel.MEMORY_ONLY)
      assert(phrases.columns.toSeq  === Seq("words"))
      assert(phrases.collect().take(1) === Seq(Row("we_work_sometimes")))
    }

    it("should run iteratively with a DataFrame") {
      val phrases = WordToPhrase(dfMulti, "words", cmsParams, scoreParams, token, 2, StorageLevel.MEMORY_ONLY)
      assert(phrases.columns.toSeq  === Seq("words", "moreWords"))
      assert(phrases.collect().take(1) === Seq(Row("we_work_sometimes", "we promise")))
    }

    it("should run README rdd example") {
      def token2: Tokenize = _.toLowerCase.replaceAll("'", "").replaceAll("\\W+", " ").trim.split("\\s+")
      val rdd = sc.makeRDD(List(("Winter is coming, and when the Long Night falls, only the Night's Watch will stand between the realm and the darkness that sweeps from the north."),
                                ("What will this place be like in the winter"),
                                ("And winter is coming"),
                                ("He could feel the eyes of the dead. They were all listening, he knew. And winter was coming."),
                                ("'The winters are hard,' Ned admitted. 'But the Starks will endure. We always have.'"),
                                ("Because winter is coming"),
                                ("'Winter is coming,' Arya whispered."),
                                ("Fear is for the winter, my little lord, when the snows fall a hundred feet deep and the ice wind comes howling out of the north."),
                                ("'You are a young man, Tyrion,' Mormont said. 'How many winters have you seen?'"),
                                ("He could almost hear him, and their lord father as well. Winter is coming, and you are almost a man grown, Bran. You have a duty."),
                                ("Promise me, Ned, his sister had whispered from her bed of blood. She had loved the scent of winter roses.")))
      val scoreParams2 = ScoreParams(delta = 2, threshold = 0.05)
      val phrases = WordToPhrase(rdd, cmsParams, scoreParams2, token2, 2, StorageLevel.MEMORY_ONLY)
    }

    it("should run README dataframe example") {
      def token2: Tokenize = _.toLowerCase.replaceAll("'", "").replaceAll("\\W+", " ").trim.split("\\s+")
      val rdd = sc.makeRDD(List(("Winter is coming, and when the Long Night falls, only the Night's Watch will stand between the realm and the darkness that sweeps from the north."),
                                ("What will this place be like in the winter"),
                                ("And winter is coming"),
                                ("He could feel the eyes of the dead. They were all listening, he knew. And winter was coming."),
                                ("'The winters are hard,' Ned admitted. 'But the Starks will endure. We always have.'"),
                                ("Because winter is coming"),
                                ("'Winter is coming,' Arya whispered."),
                                ("Fear is for the winter, my little lord, when the snows fall a hundred feet deep and the ice wind comes howling out of the north."),
                                ("'You are a young man, Tyrion,' Mormont said. 'How many winters have you seen?'"),
                                ("He could almost hear him, and their lord father as well. Winter is coming, and you are almost a man grown, Bran. You have a duty."),
                                ("Promise me, Ned, his sister had whispered from her bed of blood. She had loved the scent of winter roses.")))
      val scoreParams2 = ScoreParams(delta = 2, threshold = 0.05)
      val phrases = WordToPhrase(rdd.toDF("words"), "words", cmsParams, scoreParams2, token2, 2, StorageLevel.MEMORY_ONLY)
    }
  }
}
