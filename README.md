[![Build Status](https://travis-ci.org/tresata/spark-wordtophrase.svg?branch=master)](https://travis-ci.org/tresata/spark-wordtophrase)

# spark-wordtophrase
This is an RDD based implementation of word2phrase algorithm explained in "Section 4. Learning Phrases" of the paper [Distributed Representations of Words and Phrases and their Compositionality](http://arxiv.org/pdf/1310.4546.pdf)

There is already a Spark based [implementation](https://github.com/s4weng/word2phrase), but it did not fit our needs.

## Usage
Say you have an RDD[String] of different sentences from the Game of Thrones books that contain the word 'winter', and you are interested in finding common phrases present in the text.

```scala
import SparkSuite.{ sc, sqlc }

// Define then CountMinSketch and scoring function parameters
val cmsParams = CMSParams(epsilon= 1E-3, delta = 1E-10, seed = 1)
val scoreParams = ScoreParams(delta = 1, threshold = 2)

// Definte how you want to tokenize the text
def token: Tokenize = _.toLowerCase.replaceAll("'", "").replaceAll("\\W+", " ").trim.split("\\s+")
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

WordToPhrase(rdd, cmsParams, scoreParams, token, 2, StorageLevel.MEMORY_ONLY)
```

This can also be applied to a specific column of a Spark dataframe
```
import sqlc.implicits._
val phrases = WordToPhrase(rdd.toDF("words"), "words", cmsParams, scoreParams, token)

WordToPhrase(rdd.toDF("words"), "words", cmsParams, scoreParams, token, 2, StorageLevel.MEMORY_ONLY).collect().foreach(println)
```

The printed output would look something like this:
```
[winter_is_coming and when the long night falls only the nights watch will stand between the realm and the darkness that sweeps from the north]
[what will this place be like in the winter]
[and winter_is_coming]
[he could feel the eyes of the dead they were all listening he knew and winter was coming]
[the winters are hard ned admitted but the starks will endure we always have]
[because winter_is_coming]
[winter_is_coming arya whispered]
[fear is for the winter my little lord when the snows fall a hundred feet deep and the ice wind comes howling out of the north]
[you are a young man tyrion mormont said how many winters have you seen]
[he could almost hear him and their lord father as well winter_is_coming and you are almost a man grown bran you have a duty]
[promise me ned his sister had whispered from her bed of blood she had loved the scent of winter roses]
```

Stay warm,
Team @Tresata
