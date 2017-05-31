package org.kunicki.reactive_streams_with_monix.repository

import com.datastax.driver.core.Cluster
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import monix.eval.Task
import org.kunicki.reactive_streams_with_monix.model.ValidReading

import scala.concurrent.Promise

class ReadingRepository {

  private val session = Cluster.builder.addContactPoint("127.0.0.1").build.connect

  private val preparedStatement = session.prepare("insert into akka_streams.readings (id, value) values (?, ?)")

  def save(validReading: ValidReading): Task[Unit] = {
    val boundStatement = preparedStatement.bind(validReading.id: java.lang.Integer, validReading.value.toFloat: java.lang.Float)
    Task
      .fromFuture(toScalaFuture(session.executeAsync(boundStatement)))
      .map(_ => ())
  }

  def shutdown(): Unit = session.getCluster.close()

  private def toScalaFuture[T](listenableFuture: ListenableFuture[T]) = {
    val promise = Promise[T]

    Futures.addCallback(listenableFuture, new FutureCallback[T] {

      override def onFailure(t: Throwable): Unit = promise.failure(t)

      override def onSuccess(result: T): Unit = promise.success(result)
    })

    promise.future
  }
}
