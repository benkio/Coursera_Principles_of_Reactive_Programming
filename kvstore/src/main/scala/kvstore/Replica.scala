package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import collection.mutable.{ HashMap, MultiMap, Set }
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout
import akka.actor.OneForOneStrategy

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher
  import akka.actor.Cancellable
  val system = akka.actor.ActorSystem("system")
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  
  val persistenceActor = context.actorOf(persistenceProps)
  
  var counter = 0
  
  override def preStart() ={
    //Call the arbiter to join only on re first start of the actor Replica
    arbiter ! Join  
  }
  
  override val supervisorStrategy = OneForOneStrategy() {
    case _: Exception => Restart
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  var primaryPersistingAcks = Map.empty[Long, (ActorRef, Cancellable)]
  var replicatorAcks = Map.empty[Long,(ActorRef, Int)] 
  var replicationAcks = new HashMap[ActorRef, Set[Long]] with MultiMap[ActorRef, Long]
  val leader: Receive = {
    case Insert(key , value, id) => 
      kv += key -> value
      primaryPersistingAcks += id -> (sender, system.scheduler.schedule(Duration.Zero, Duration.create(100, MILLISECONDS), persistenceActor, Persist(key, Some(value), id)))

      if (!replicators.isEmpty){
        replicators foreach {r =>
          r ! Replicate(key, Some(value), id)
          replicationAcks.addBinding(r, id)
        }
        replicatorAcks += id -> (sender,replicators.size)
      }
      
      system.scheduler.scheduleOnce(1 second) {
        primaryPersistingAcks get id match {
          case Some((s, c)) =>
            c.cancel
            primaryPersistingAcks -= id
            s ! OperationFailed(id)
          case None => 
            replicatorAcks get id match {
              case Some((s,rl)) =>
                replicatorAcks -= id
                s ! OperationFailed(id)
              case None => {}  
            }
        }
      }
      
    case Remove(key,id) => 
      kv -= key
      primaryPersistingAcks += id -> (sender, system.scheduler.schedule(Duration.Zero, Duration.create(100, MILLISECONDS), persistenceActor, Persist(key, None, id)))
      
      if (!replicators.isEmpty){
        replicators foreach {r =>
          r ! Replicate(key, None, id)
          replicationAcks.addBinding(r, id)
        }
        replicatorAcks += id -> (sender,replicators.size)
      }
      
      system.scheduler.scheduleOnce(1 second) {
        primaryPersistingAcks get id match {
          case Some((s, c)) =>
            c.cancel
            primaryPersistingAcks -= id
            s ! OperationFailed(id)
          case None => 
            replicatorAcks get id match {
              case Some((s,rl)) =>
                replicatorAcks -= id
                s ! OperationFailed(id)
              case None => {}  
            }
        }
      }
    case Get(key,id) =>
      val value = kv.get(key)
      sender ! GetResult(key,value,id)
    case Persisted(key,id) =>
      primaryPersistingAcks get id match {
        case Some((s,c)) => 
          c.cancel
          primaryPersistingAcks -= id
          if (!(replicatorAcks contains(id)))
            s ! OperationAck(id)
        case None => {}
      }
    case Replicated(key, id) =>
      replicationAcks get sender match {
        case Some(i) => i-=id 
        case None => {}
      }
      replicatorAcks get id match {
        case Some((s,rl)) => 
          val newList = rl - 1
          replicatorAcks -= id
          if (newList == 0 && !(primaryPersistingAcks contains id)){
            s ! OperationAck(id)
          }
          else
            replicatorAcks += id -> (s,newList)
        case None => {}
      }
    case Replicas(rl) => 
      val allButMe = rl - self
      val joined = allButMe -- secondaries.keySet
      val removed = secondaries.keySet -- allButMe
      joined foreach { a =>
        val replicator = context.actorOf(Replicator.props(a))
        secondaries += a -> replicator
        replicators += replicator
        
        kv foreach { case (k,v) =>
          replicator ! Replicate(k,Some(v),counter)
       }
      }
      removed foreach { r =>
        secondaries get r match {
          case Some(replicator) =>
            context.stop(replicator)
            secondaries -= r
            replicators -= replicator
             
            replicationAcks get replicator match {
              case Some(ids) => ids foreach { i => self ! Replicated("", i)}
              case None => {}
            }
          case None => {}
        }
     }
  }

  
  // used to check the replicator sequence
  var expectedReplicatorSequence : Long = 0
  var secondaryPersistingAcks = Map.empty[Long, (ActorRef, Cancellable)]
  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key,id) =>
      val value = kv.get(key)
      sender ! GetResult(key,value,id)
    case Snapshot(key, valueOption, seq) =>
      if (seq < expectedReplicatorSequence){
        sender ! SnapshotAck(key, seq)
      }
      if (seq == expectedReplicatorSequence){
        valueOption match {
          case Some(v) => kv += key -> v
          case None => kv -= key
        }
        secondaryPersistingAcks += seq -> (sender, system.scheduler.schedule(Duration.Zero, Duration.create(100, MILLISECONDS), persistenceActor, Persist(key, valueOption, seq)))
        expectedReplicatorSequence = (seq+1)
      }
    case Persisted(key,id) =>
      secondaryPersistingAcks.get(id) match {
        case Some((replicator,cancellable)) =>
          cancellable.cancel
          secondaryPersistingAcks -= id
          replicator ! SnapshotAck(key, id)
        case None => {}
      }
  }

}

