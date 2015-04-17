package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  /**
   * Min in a heap of 1 element
   */
  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  /**
   * Min of heaps of 2 elements in different order
   */
  property("min2") = forAll { (a: Int,b:Int) =>
    val h = insert(b,insert(a, empty))
    val h1 = insert(a,insert(b, empty))
    if (a < b)
      findMin(h1) == a && findMin(h) == a
    else
      findMin(h1) == b && findMin(h) == b
  }
  
  /**
   * Delete the min in the 1-element heap and check if is empty 
   */
  property("empty1") = forAll { a: Int =>
    val h = insert(a, empty)
    val h1 = deleteMin(h)
    !isEmpty(h) && isEmpty(h1)
  }
  
  /**
   * Same as previous but with 2-element heap
   */
  property("empty2") = forAll { (a: Int,b:Int) =>
    val h = insert(b,insert(a, empty))
    val h1 = deleteMin(deleteMin(h))
    !isEmpty(h) && isEmpty(h1)
  }
  
  /**
   * Get an heap and check in the min is always the true min
   */
  property("sorted seq") = forAll{ (heap:H) =>
    def checkMinOrder(h:H,prevMin:Int): Boolean = {
      val min = findMin(h)
      val delHeap = deleteMin(h)
      (prevMin <= min) && (isEmpty(delHeap) || checkMinOrder(delHeap, min)) 
    }
    isEmpty(heap) || checkMinOrder(heap,Int.MinValue)
  }
  
  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h))==m
  }
  /**
   * Melt two heaps and check if the min of the result is one of the two component
   */
  property("minimumMelted") = forAll{ (h1:H,h2:H) =>
    val meldedHeaps = meld(h1,h2)
    (findMin(meldedHeaps) == findMin(h1)) || (findMin(meldedHeaps) == findMin(h2))
  }
  /**
   * Insert and ordered list to the heap and check in the return of the heap match the order of the list.
   */
  property("insert list and check") = forAll { l: List[Int] =>
    def pop(h: H, l: List[Int]): Boolean = {
      if (isEmpty(h)) {
        l.isEmpty
      } else {
        !l.isEmpty && findMin(h) == l.head && pop(deleteMin(h), l.tail)
      }
    }
    val sl = l.sorted
    val h = l.foldLeft(empty)((he, a) => insert(a, he))
    pop(h, sl)
  }
  
  lazy val genHeap: Gen[H] = for {
    k <- arbitrary[Int]
    m <- oneOf(const(empty), genHeap)
  } yield insert(k,m)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
