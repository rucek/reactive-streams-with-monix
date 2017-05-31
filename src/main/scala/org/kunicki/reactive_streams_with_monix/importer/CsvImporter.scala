package org.kunicki.reactive_streams_with_monix.importer

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.nio.file.Paths
import java.util.zip.GZIPInputStream

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import monix.eval.Task
import monix.reactive.{Consumer, Observable}
import monix.reactive.observables.ObservableLike.Transformer
import org.kunicki.reactive_streams_with_monix.importer.CsvImporter.mapAsyncOrdered
import org.kunicki.reactive_streams_with_monix.model.{InvalidReading, Reading, ValidReading}
import org.kunicki.reactive_streams_with_monix.repository.ReadingRepository

class CsvImporter(config: Config, readingRepository: ReadingRepository) extends LazyLogging {

  private val importDirectory = Paths.get(config.getString("importer.import-directory")).toFile
  private val linesToSkip = config.getInt("importer.lines-to-skip")
  private val concurrentFiles = config.getInt("importer.concurrent-files")
  private val concurrentWrites = config.getInt("importer.concurrent-writes")
  private val nonIOParallelism = config.getInt("importer.non-io-parallelism")

  def parseLine(line: String): Task[Reading] = Task {
    val fields = line.split(";")
    val id = fields(0).toInt
    try {
      val value = fields(1).toDouble
      ValidReading(id, value)
    } catch {
      case t: Throwable =>
        logger.error(s"Unable to parse line $line: ${t.getMessage}")
        InvalidReading(id)
    }
  }

  val parseFile: Transformer[File, Reading] = _.concatMap { file =>
    Observable.fromLinesReader(new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)))))
      .drop(linesToSkip)
      .transform(mapAsyncOrdered(nonIOParallelism)(parseLine))
  }

  val computeAverage: Transformer[Reading, ValidReading] = _.bufferTumbling(2).mapAsync(nonIOParallelism) { readings =>
    Task {
      val validReadings = readings.collect { case r: ValidReading => r }
      val average = if (validReadings.nonEmpty) validReadings.map(_.value).sum / validReadings.size else -1
      ValidReading(readings.head.id, average)
    }
  }

  val storeReadings: Consumer[ValidReading, Unit] =
    Consumer.foreachParallelAsync(concurrentWrites)(readingRepository.save)
}

object CsvImporter {

  def mapAsyncOrdered[A, B](parallelism: Int)(f: A => Task[B]): Transformer[A, B] =
    _.map(f).bufferTumbling(parallelism).flatMap { tasks =>
      val gathered = Task.gather(tasks)
      Observable.fromTask(gathered).concatMap(Observable.fromIterable)
    }
}
