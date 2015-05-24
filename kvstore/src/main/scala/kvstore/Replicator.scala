package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.actor.Scheduler

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  import akka.actor.Cancellable
  val system = akka.actor.ActorSystem("system")
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate, Cancellable)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, valueOption, id) => 
      val sequence = nextSeq 
      val cancellable = system.scheduler.schedule(Duration.Zero, Duration.create(100, MILLISECONDS), replica, Snapshot(key,valueOption,sequence));
      acks += sequence -> (sender,Replicate(key, valueOption, id),cancellable)
      
    case SnapshotAck(key, seq) =>  
      acks.get(seq) match {
        case Some((sender, Replicate(k,value,id),cancellable)) =>
              sender ! Replicated(k, id)
              acks -= seq
              cancellable.cancel
        case None => {}
      }
  }

}
