package org.kunicki.reactive_streams_with_monix

import java.nio.file.Paths
import java.util.UUID

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import monix.execution.Scheduler.Implicits.global
import monix.nio.file.writeAsync
import monix.reactive.Observable
import org.kunicki.reactive_streams_with_monix.model.ValidReading

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Random

object RandomDataGenerator extends App with LazyLogging {

  val config = ConfigFactory.load()
  val numberOfFiles = config.getInt("generator.number-of-files")
  val numberOfPairs = config.getInt("generator.number-of-pairs")
  val invalidLineProbability = config.getDouble("generator.invalid-line-probability")

  logger.info("Starting generation")

  val f = Observable.range(1, numberOfFiles)
    .map(_ => UUID.randomUUID().toString)
    .mapAsync(numberOfFiles) { fileName =>
      Observable.range(1, numberOfPairs).map { _ =>
        val id = Random.nextInt(1000000)
        Seq(ValidReading(id), ValidReading(id)).map { reading =>
          val value = if (Random.nextDouble() > invalidLineProbability) reading.value.toString else "invalid_value"
          s"${reading.id};$value\n"
        }.reduce(_ + _).getBytes
      }.consumeWith(writeAsync(Paths.get(s"data/$fileName.csv")))
    }
    .completedL
    .runAsync

  Await.ready(f, Duration.Inf)
  logger.info("Generated random data")
}
