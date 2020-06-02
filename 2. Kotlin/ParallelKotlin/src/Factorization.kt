/*
Посчитать, какое суммарное количество простых множителей присутствует при факторизации всех чисел.
Например, пусть всего два числа: 6 и 8. 6 = 2 * 3, 8 = 2 * 2 * 2. Ответ 5.
При реализации нужно использовать операции с длинной арифметикой (*BigInteger* и т.д.)
 */

import java.io.File
import java.math.BigInteger
import java.util.*

// Метод взят со StackOverFlow, адаптирован под Kotlin
// https://stackoverflow.com/questions/16802233/faster-prime-factorization-for-huge-bigintegers-in-java
fun numFactors(n: BigInteger): LinkedList<*>? {
    var n = n
    val two = BigInteger.valueOf(2)
    val fs: LinkedList<BigInteger> = LinkedList<BigInteger>()
    require(n.compareTo(two) >= 0) { "must be greater than one" }
    while (n.mod(two) == BigInteger.ZERO) {
        fs.add(two)
        n = n.divide(two)
    }
    if (n.compareTo(BigInteger.ONE) > 0) {
        var f = BigInteger.valueOf(3)
        while (f.multiply(f).compareTo(n) <= 0) {
            if (n.mod(f) == BigInteger.ZERO) {
                fs.add(f)
                n = n.divide(f)
            } else {
                f = f.add(two)
            }
        }
        fs.add(n)
    }
    return fs
}

// Вывод массива с простыми множителями для тестирования
fun printNumsFactors() : Unit{
    val lines: List<String> = File(fileName).readLines()
    lines.forEach { line -> println(numFactors(BigInteger(line.toString()))) }
}