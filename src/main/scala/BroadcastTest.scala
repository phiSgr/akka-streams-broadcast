import java.util.concurrent.ThreadLocalRandom

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ClosedShape
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source, _}

import scala.concurrent.Future
import scala.util.Success

object BroadcastTest extends App {

  implicit val system = ActorSystem("QuickStart")
  implicit val ec = system.dispatcher

  val start = System.currentTimeMillis()

  def spin(ms: Long): Unit = {
    val start = System.currentTimeMillis()
    while (System.currentTimeMillis() - start < ms) {}
  }

  val source: Source[Int, NotUsed] =
    Source.fromIterator(() => Iterator.continually(ThreadLocalRandom.current().nextInt(100))).take(100)

  val countSink: Sink[Int, Future[Int]] = Flow[Int].toMat(Sink.fold(0)((acc, elem) => acc + 1))(Keep.right)
  val minSink: Sink[Int, Future[Int]] = Flow[Int].toMat(Sink.fold(0)((acc, elem) => math.min(acc, elem)))(Keep.right)
  val maxSink: Sink[Int, Future[Int]] = Flow[Int].toMat(Sink.fold(0)((acc, elem) => math.max(acc, elem)))(Keep.right)

  val (count: Future[Int], min: Future[Int], max: Future[Int]) =
    RunnableGraph
      .fromGraph(GraphDSL.create(countSink, minSink, maxSink)(Tuple3.apply) {
        implicit builder => (countS, minS, maxS) =>
          import GraphDSL.Implicits._
          val broadcast = builder.add(Broadcast[Int](3))
          source ~> broadcast
          broadcast.out(0) ~> Flow[Int].async ~> countS
          broadcast.out(0) ~> Flow[Int].async ~> minS
          broadcast.out(0) ~> Flow[Int].async ~> maxS
          ClosedShape
      })
      .run()

  Future.sequence(List(count, min, max)).onComplete { res =>
    println(s"Done in ${System.currentTimeMillis() - start}ms")
    val Success(List(count, min, max)) = res
    println(s"Count is $count, min is $min, max is $max.")
    system.terminate()
  }
}
