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
    Signal({
      val d = delta()
         d match {
          case _ if d < 0 	=> Set()
          case _ if d == 0 	=> Set(( (-b()) / (2*a()) ))
          case _ if d > 0 	=> {
            val notDelta = (-b()) / (2*a())
            val deltaDivided = (Math.sqrt(delta()))/(2*a())
            Set((notDelta + deltaDivided),( notDelta - deltaDivided ))}
        }
    })
  }
}
