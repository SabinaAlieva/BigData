// Задача 2.1
// Нужно сгенерировать файл, содержащий 2000 128-битных случайных целых чисел, каждое число на отдельной строке.
//

import java.io.File
import java.math.BigInteger
import java.util.*

//var fileName = "random64numbers.txt"
var fileName = "somefile.txt" // для тестирования
var bits = 64 // на 128 битах мой компьютер почти умер, поэтому решила оставить 64

fun generateNums() : Unit{
    val random = Random()
    val nums = arrayListOf<BigInteger>()
    for (i in (0..2000)) {
        nums.add(BigInteger(64, random))
    }
    File(fileName).writeText(nums.joinToString("\n"))
}


fun main(args: Array<String>) {
    generateNums()
}