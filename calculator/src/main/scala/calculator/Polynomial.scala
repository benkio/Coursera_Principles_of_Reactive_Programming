package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Signal(
        Math.pow(b(), 2) - 4*a()*c()
    )
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    Signal(
        delta() match {
          case _ if delta() < 0 => Set()
          case _ if delta() == 0 => Set((-b()/2*a()))
          case _ if delta() > 0 => Set(((-b()+Math.sqrt(delta()))/2*a()),((-b()-Math.sqrt(delta()))/2*a()))
        }
    )
  }
}
