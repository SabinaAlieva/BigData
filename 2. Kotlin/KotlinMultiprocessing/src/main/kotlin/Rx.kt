import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.schedulers.Schedulers
import java.io.File
import java.util.*

var array = Collections.synchronizedList(mutableListOf<Int>())

fun addToList(num : Int) : Unit {
    array.add(num)
}

fun RxObservable() {
    val numbers = File(fileName).readLines().map { it.toBigInteger() }

    Observable.fromIterable(numbers)
            .map { x -> numFactors(x)?.size!! }
            .subscribe({ x -> addToList(x) })
    //.forEach { x -> println(x)}

    println("RxObservable: " + array.sum().toString())
}

var sum = 0

fun addSum(num : Int) : Unit {
    sum += num
}

fun RxFlowable() {
    val numbers = File(fileName).readLines().map { it.toBigInteger() }

    Flowable.fromIterable(numbers)
            .onBackpressureBuffer()
            .parallel()
            .runOn(Schedulers.computation())
            .map { x -> numFactors(x)?.size!! }
            .sequential()
            .blockingSubscribe({ x -> addSum(x) })
    //.forEach { x -> println(x)}

    println("RxFlowable: " + sum.toString())
}

fun main(args: Array<String>) {
    RxObservable()
    RxFlowable()
}