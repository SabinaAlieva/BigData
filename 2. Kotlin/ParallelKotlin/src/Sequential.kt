import java.io.File
import java.math.BigInteger

// Простой последовательный подсчет суммы простых множителей для списка чисел
fun justCountNumFactors() : Unit{
    println("Последовательный расчет")
    var sum = 0
    val lines: List<String> = File(fileName).readLines()
    lines.forEach { line -> sum += numFactors(BigInteger(line.toString()))?.size!! }
    println(sum)
}