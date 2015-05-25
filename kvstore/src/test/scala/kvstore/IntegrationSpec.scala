/**
 * Copyright (C) 2013-2015 Typesafe Inc. <http://www.typesafe.com>
 */
package kvstore

import akka.actor.{ Actor, Props, ActorRef, ActorSystem }
import akka.testkit.{ TestProbe, ImplicitSender, TestKit }
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }
import scala.concurrent.duration._
import org.scalatest.FunSuiteLike
import org.scalactic.ConversionCheckedTripleEquals

class IntegrationSpec(_system: ActorSystem) extends TestKit(_system)
    with FunSuiteLike
        with Matchers
    with BeforeAndAfterAll
    with ConversionCheckedTripleEquals
    with ImplicitSender
    with Tools {

  import Replica._
  import Replicator._
  import Arbiter._

  def this() = this(ActorSystem("ReplicatorSpec"))

  override def afterAll: Unit = system.shutdown()

  private def randomInt(implicit n: Int = 16) = (Math.random * n).toInt

  private def randomQuery(client: Session) {
    val rnd = Math.random
    if (rnd < 0.3) client.setAcked(s"k$randomInt", s"v$randomInt")
    else if (rnd < 0.6) client.removeAcked(s"k$randomInt")
    else client.getAndVerify(s"k$randomInt")
  }

  test("case1: Random ops") {
    val arbiter = TestProbe()

    val primary = system.actorOf(
      Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case1-primary")
    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val secondary = system.actorOf(
      Replica.props(arbiter.ref, Persistence.props(flaky = true)), "case1-secondary")
    arbiter.expectMsg(Join)
    arbiter.send(secondary, JoinedSecondary)

    arbiter.send(primary, Replicas(Set(primary, secondary)))

    val client = session(primary)
    for (_ <- 0 until 1000) randomQuery(client)
  }

  ignore("case2: Random ops with 3 secondaries") {
    val arbiter = TestProbe()

    val primary = system.actorOf(
      Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case2-primary")
    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val secondaries = (1 to 3).map(id =>
      system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = true)),
        s"case2-secondary-$id"))

    secondaries foreach { secondary =>
      arbiter.expectMsg(Join)
      arbiter.send(secondary, JoinedSecondary)
    }

    val client = session(primary)
    for (i <- 0 until 1000) {
      randomQuery(client)
      if      (i == 100) arbiter.send(primary, Replicas(Set(secondaries(0))))
      else if (i == 200) arbiter.send(primary, Replicas(Set(secondaries(0), secondaries(1))))
      else if (i == 300) arbiter.send(primary, Replicas(Set(secondaries(0), secondaries(1), secondaries(2))))
      else if (i == 400) arbiter.send(primary, Replicas(Set(secondaries(0), secondaries(1))))
      else if (i == 500) arbiter.send(primary, Replicas(Set(secondaries(0))))
      else if (i == 600) arbiter.send(primary, Replicas(Set()))
    }
  }
  
  ignore("case3: Random ops with multiple clusters") {
    val arbiter = TestProbe()

    val primary = system.actorOf(
      Replica.props(arbiter.ref, Persistence.props(flaky = false)), "case3-primary")
    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    val cluster = (1 to 10) map { i =>
      (1 to 5).toSet map { (j: Int) =>
        val secondary = system.actorOf(
          Replica.props(arbiter.ref, Persistence.props(flaky = true)),
          s"case3-secondary-$i-$j")
        arbiter.expectMsg(Join)
        arbiter.send(secondary, JoinedSecondary)
        secondary
      }
    }

    val client = session(primary)
    for (i <- 0 until 1000) {
      randomQuery(client)
      if (randomInt(10) < 3)
        arbiter.send(primary, Replicas(cluster(randomInt(10))))
    }

  }

  
}
