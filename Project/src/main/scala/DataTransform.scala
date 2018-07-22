package scala

object DataTransform {
  def mirroring(input: Array[Double]): Array[Double] = {
    val res: Array[Double] = new Array[Double](input.length)
    for (i <- 0 until 10*21*7) {
      val xyz = getXYZ(i)
      val x = xyz(0)
      val y = xyz(1)
      val z = xyz(2)
      res.update(i, input((20-x)*21*7+y*7+z))
      res.update((20-x)*21*7+y*7+z, input(i))
    }
    return res
  }

  def rotate90(input: Array[Double]): Array[Double] = {
    val res: Array[Double] = new Array[Double](input.length)
    for (i <- 0 until 21*21*7) {
      val xyz = getXYZ(i)
      val x = xyz(0)
      val y = xyz(1)
      val z = xyz(2)
      res.update(i, input(y*21*7+(20-x)*7+z))
    }
    return res
  }

  def getXYZ(i: Int): Array[Int] = {
    val xyz: Array[Int] = new Array[Int](3)
    xyz.update(0, i/(21*7))
    xyz.update(1, i%(21*7)/7)
    xyz.update(2, i%7)
    return xyz
  }
}
