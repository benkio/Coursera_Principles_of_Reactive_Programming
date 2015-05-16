/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue
import actorbintree.Insert
import actorbintree.OperationFinished
import actorbintree.CopyTo

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /**
   * Request with identifier `id` to insert an element `elem` into the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
   * Request with identifier `id` to check whether an element `elem` is present
   * in the tree. The actor at reference `requester` should be notified when
   * this operation is completed.
   */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /**
   * Request with identifier `id` to remove the element `elem` from the tree.
   * The actor at reference `requester` should be notified when this operation
   * is completed.
   */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /**
   * Holds the answer to the Contains request with identifier `id`.
   * `result` is true if and only if the element is present in the tree.
   */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert => ???
    case Contains => ???
    case Remove => ???
    case GC => ???
  }

  // optional
  /**
   * Handles messages while garbage collection is performed.
   * `newRoot` is the root of the new binary tree where we want to copy
   * all non-removed elements into.
   */
  def garbageCollecting(newRoot: ActorRef): Receive = ???

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  def addNewActorChild(p: Position, elem: Int, initiallyRemoved: Boolean) = (p -> context.actorOf(BinaryTreeNode.props(elem, initiallyRemoved)))
  def leftNode = subtrees.get(Left)
  def rightNode = subtrees.get(Right)
  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester: ActorRef, id: Int, e: Int) => (e, leftNode, rightNode) match {
      case (b, l, _) if b < elem => if (l.isEmpty) {
        subtrees += addNewActorChild(BinaryTreeNode.Left, e, false)
        requester ! OperationFinished(id)
      } else l.get forward Insert

      case (a, _, r) if a > elem =>
        if (r.isEmpty) {
          subtrees += addNewActorChild(BinaryTreeNode.Right, e, false)
          requester ! OperationFinished(id)
        } else r.get forward Insert

      case _ =>
        if (removed) removed = false
        requester ! OperationFinished(id)
    }

    case Contains(requester: ActorRef, id: Int, e: Int) => (e, leftNode, rightNode) match {
      case (b, l, _) if b < elem => if (l.isEmpty) requester ! ContainsResult(id, false) else l.get forward Contains
      case (a, _, r) if a > elem => if (r.isEmpty) requester ! ContainsResult(id, false) else r.get forward Contains
      case elem => if (removed) requester ! ContainsResult(id, false) else requester ! ContainsResult(id, true)
    }
    case Remove(requester: ActorRef, id: Int, e: Int) => (e, leftNode, rightNode) match {
      case (b, l, _) if b < elem => if (l.isEmpty) requester ! OperationFinished(id) else l.get forward Remove
      case (a, _, r) if a > elem => if (r.isEmpty) requester ! OperationFinished(id) else r.get forward Remove
      case elem =>
        if (removed)
          requester ! OperationFinished(id)
        else {
          removed = true
          requester ! OperationFinished(id)
        }
    }

    case CopyTo(treeNode) => 
      if (subtrees.isEmpty && removed) 
        sender ! CopyFinished
      else {
        val subtreesToCopy = subtrees.values
        subtreesToCopy.foreach(_ ! CopyTo(treeNode))
        if (!removed) treeNode ! Insert(self, -1,elem)
        context.become(copying(subtreesToCopy.toSet, removed))
      } 
  }

  // optional
  /**
   * `expected` is the set of ActorRefs whose replies we are waiting for,
   * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
   */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished => 
      if (expected.isEmpty)
        context.parent ! CopyFinished
      else
        context.become(copying(expected,true))
        
    case CopyFinished =>
      if (expected.size == 1 && insertConfirmed)
        context.parent ! CopyFinished
      else
        context become copying(expected - sender, insertConfirmed)
  }
}
