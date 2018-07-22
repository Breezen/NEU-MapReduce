val weather = sc.textFile("input")
val TMAX = weather.map(text => text.split(","))
                  .filter(entry => entry(2) == "TMAX")
val TMIN = weather.map(text => text.split(","))
                  .filter(entry => entry(2) == "TMIN")

val sumTMAX = TMAX.map(entry => Integer.parseInt(entry(3)))
                  .combineByKey()
                  .reduce((sum, t) => sum + t)
val sumTMIN = TMIN.map(entry => Integer.parseInt(entry(3)))
                  .combineByKey()
                  .reduce((sum, t) => sum + t)

val numTMAX = TMAX.count
val numTMIN = TMIN.count

println(sumTMAX / numTMAX, sumTMIN / numTMIN)
