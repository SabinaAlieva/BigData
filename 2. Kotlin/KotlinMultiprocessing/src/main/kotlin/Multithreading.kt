import java.io.File
import java.math.BigInteger
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock

// Решение с многопоточностью
fun startAndJoin(vararg blocks: () -> Unit) {
    val threads = blocks.map { thread(block = it, isDaemon = true, start = true) }
    threads.forEach {it.join()}
}

// Расчет с помощью 4 CompletableFuture
fun countFactorsCompletableFuture() : Unit {
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

    println("4 CompletableFuture: " + sum.toString())
}

// Расчет с помощью блокировки суммирования и 3 потоков
fun countFactorsLock() {
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
    println("3 threads + blocking sum: " + sum.toString())
}

@Volatile var flag1 = true
@Volatile var flag2 = true
@Volatile var flag3 = false

// Расчет с помощью 2 потоков и Synchronized List
fun countFactorsSynchronizedList() : Unit{
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
        println("2 threads +  Synchronized List: " + sum.toString())
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

// Расчет при помощи BlockingQueue, 2 потоков и ProductConsumer
fun countFactorsProducerConsumer() {
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
        println("BlockingQueue, 2 threads + ProductConsumer: " + sum.toString())
    }

    startAndJoin(producer1, producer2, consumer)
}

var summa = 0
@Synchronized fun updateSum(num : Int){
    summa = summa + num
}
// Расчет с помощью synchronizedList, 4 потоков и Synchronized переменной для суммирования
fun countFactorsThreadsSynchronizedSum() : Unit {
    var lst = Collections.synchronizedList(mutableListOf<String>())
    lst = File(fileName).readLines()
    val lock: Lock = ReentrantLock()

    val r = {
        var st : String = " "
        var num : Int = 0
        var stop : Boolean = false
        while (!stop) {
            lock.withLock {
                if (lst.isNotEmpty()) {
                    st = lst[0]
                    lst.removeAt(0)
                    num = numFactors(BigInteger(st.toString()))?.size!!
                    updateSum(num)
                } else {
                    TimeUnit.MICROSECONDS.sleep(100)
                    if (lst.isEmpty()) {
                        stop = true
                    } else {
                        stop = false
                    }
                }
            }
        }
    }

    startAndJoin(r,r,r,r)
    println("SynchronizedList, 4 threads и Synchronized sum: " + summa.toString())
}

// https://proandroiddev.com/synchronization-and-thread-safety-techniques-in-java-and-kotlin-f63506370e6d
