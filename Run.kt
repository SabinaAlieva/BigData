/* Задача 2

1. Нужно сгенерировать файл, содержащий 2000 128-битных случайных целых чисел, каждое число на отдельной строке.
2. Посчитать, какое суммарное количество простых множителей присутствует при факторизации всех чисел.
Например, пусть всего два числа: 6 и 8. 6 = 2 * 3, 8 = 2 * 2 * 2. Ответ 5.
При реализации нужно использовать операции с длинной арифметикой (*BigInteger* и т.д.)
3. Реализовать подсчет
- простым последовательным алгоритмом
- многопоточно, с использованием примитивов синхронизации
- с помощью Akka (или аналога)
- c помощью RxJava (или аналога)
4. Измерить время выполнения для каждого случая. Использовать уровень параллельности в соответствии с числом ядер вашего CPU.
*/

import java.io.File
import java.math.BigInteger
import java.util.*
import kotlin.system.measureTimeMillis

// Библиотеки для параллельности
import kotlin.concurrent.thread

import java.util.concurrent.CyclicBarrier

import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


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

// Простой последовательный подсчет суммы простых множителей
fun justCountNumFactors() : Unit{
    println("Последовательный расчет")
    var sum = 0
    val lines: List<String> = File(fileName).readLines()
    lines.forEach { line -> sum += numFactors(BigInteger(line.toString()))?.size!! }
    println(sum)
}

// Подсчет времени с выводом
fun countTime(funcname : () -> Unit) {
    val elapsedTime = measureTimeMillis {
        funcname()
    }
    println("Время выполнения $elapsedTime ms")
}

///////////////////////////////////////////////////////////////////////////////////////////
// Решение с многопоточностью
fun startAndJoin(vararg blocks: () -> Unit) {
    val threads = blocks.map { thread(block = it, isDaemon = true, start = true) }
    threads.forEach {it.join()}
}

fun countFactorsCompletableFuture() : Unit {
    println("Расчет с помощью 4 CompletableFuture")

    var sum = 0
    val lines: List<String> = File(fileName).readLines()

    //val cb = CyclicBarrier(1)

    for (i in (0..(lines.size-1) step 4)) {
        val f1 = CompletableFuture.supplyAsync { numFactors(BigInteger(lines[i].toString())) }
        val f2 = CompletableFuture.supplyAsync { numFactors(BigInteger(lines[i+1].toString())) }
        val f3 = CompletableFuture.supplyAsync { numFactors(BigInteger(lines[i+2].toString())) }
        val f4 = CompletableFuture.supplyAsync { numFactors(BigInteger(lines[i+3].toString())) }

        //cb.await()
        sum += f1.get()?.size!! + f2.get()?.size!! + f3.get()?.size!! + f4.get()?.size!!
    }

    println(sum)
}

fun countFactorsLock() {
    println("Расчет с помощью блокировки суммирования и 3 потоков")
    val lock: Lock = ReentrantLock()
    var sum = 0
    val lines: List<String> = File(fileName).readLines()

    val r1 = {
        var num = 0
        for (line in lines.slice(0..6)) {
            num = numFactors(BigInteger(line.toString()))?.size!!
            lock.withLock {
                sum += num
            }
        }
    }

    val r2 = {
        var num = 0
        for (line in lines.slice(7..12)) {
            num = numFactors(BigInteger(line.toString()))?.size!!
            lock.withLock {
                sum += num
            }
        }
    }

    val r3 = {
        var num = 0
        for (line in lines.slice(13..19)) {
            num = numFactors(BigInteger(line.toString()))?.size!!
            lock.withLock {
                sum += num
            }
        }
    }

    startAndJoin(r1, r2, r3)
    println(sum)
}

@Volatile var flag1 = true
@Volatile var flag2 = true
@Volatile var flag3 = false

fun countFactorsSynchronizedList() : Unit{
    println("Расчет с помощью 2 потоков и Synchronized List")

    val lst = Collections.synchronizedList(ArrayList<Int>())
    val lock = ReentrantLock()

    var sum = 0
    val lines: List<String> = File(fileName).readLines()

    val s = {
        while(flag1 or flag2 or flag3) {
            if (lst.size > 0) {
                sum += lst.get(0)
                lst.removeAt(0)
            }
        }
        println(sum)
    }

    val t1 = {
        for (line in lines.slice(0..9)) {
        //for (line in lines.slice(0..6)) {
            val v = numFactors(BigInteger(line.toString()))?.size!!
            lst.add(v)
        }
        flag1 = false
    }

    val t2 = {
        for (line in lines.slice(10..19)) {
        //for (line in lines.slice(7..12)) {
            val v = numFactors(BigInteger(line.toString()))?.size!!
            lst.add(v)
        }
        flag2 = false
    }

    /*val t3 = {
        for (line in lines.slice(13..19)) {
            val v = numFactors(BigInteger(line.toString()))?.size!!
            lst.add(v)
        }
        flag3 = false
    }*/

    startAndJoin(s, t1, t2)
    //startAndJoin(s, t1, t2, t3)
}

fun countFactorsProducerConsumer() {
    println("Расчет при помощи BlockingQueue, 2 потоков и ProductConsumer")

    val workers = 2
    val iterations = 10

    var sum = 0
    val lines: List<String> = File(fileName).readLines()

    val bq: BlockingQueue<Int> = ArrayBlockingQueue(10)

    val producer1 =  {
        for (i in 0..iterations) {
            bq.put(numFactors(BigInteger(lines[i].toString()))?.size!!)
        }
    }

    val producer2 =  {
        for (i in iterations..(workers * iterations - 1)) {
            bq.put(numFactors(BigInteger(lines[i].toString()))?.size!!)
        }
    }

    val consumer =  {
        for (i in 0..(workers * iterations - 1)) {
            val head = bq.take()
            sum += head
        }
        println(sum)
    }

    startAndJoin(producer1, producer2, consumer)
}


///////////////////////////////////////////////////////////////////////////////////////////
fun main() {
    // generateNums()
    // printNumsFactors()

    countTime({ justCountNumFactors() }) // Обычная реализация          // 55
    countTime({ countFactorsCompletableFuture() }) // 46 sec            // 50
    countTime({ countFactorsLock() }) // 48 sec                         // 47
    countTime({ countFactorsSynchronizedList() }) // добавление t3 время ухудшает // 41
    countTime({ countFactorsProducerConsumer() }) // 64 sec             // 55
}