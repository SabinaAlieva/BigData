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

import kotlin.system.measureTimeMillis

// Подсчет времени с выводом
fun countTime(funcname : () -> Unit) {
    val elapsedTime = measureTimeMillis {
        funcname()
    }
    println("Время выполнения $elapsedTime ms")
}

fun main() {

    //countTime({ justCountNumFactors() }) // Обычная реализация          // 55
    //countTime({ countFactorsCompletableFuture() }) // 46 sec            // 44
    //countTime({ countFactorsLock() }) // 48 sec                         // 44
    //countTime({ countFactorsSynchronizedList() }) // добавление t3 время ухудшает // 36
    //countTime({ countFactorsProducerConsumer() }) // 64 sec             // 51
    //countTime({  countFactorsThreadsSynchronizedSum()  })                 // 55
}