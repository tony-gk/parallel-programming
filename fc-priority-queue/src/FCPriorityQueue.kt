import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.atomicArrayOfNulls
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.random.Random

class FCPriorityQueue<E : Comparable<E>> {
    private val q = PriorityQueue<E>()
    private val arraySize = Runtime.getRuntime().availableProcessors()
    private val publicationArray = atomicArrayOfNulls<Operation<E>>(arraySize)
    private val locked = atomic(false)

    /**
     * Retrieves the element with the highest priority
     * and returns it as the result of this function;
     * returns `null` if the queue is empty.
     */
    fun poll(): E? {
        return executeOperation(Operation.Poll()) {
            q.poll()
        }
    }

    /**
     * Returns the element with the highest priority
     * or `null` if the queue is empty.
     */
    fun peek(): E? {
        return executeOperation(Operation.Peek()) {
            q.peek()
        }
    }

    /**
     * Adds the specified element to the queue.
     */
    fun add(element: E) {
        executeOperation(Operation.Add(element)) {
            q.add(element)
            null
        }
    }

    private fun executeOperation(opPublication: Operation<E>, queueOp: () -> E?): E? {
        var i: Int
        while (true) {
//            Thread.yield()
            i = Random.nextInt(0, arraySize)
            if (publicationArray[i].compareAndSet(null, opPublication)) {
                break
            }
        }
        while (true) {
//           Thread.yield()
            if (tryLock()) {
                try {
                    val pub = publicationArray[i].value
                    publicationArray[i].value = null
                    if (pub is Operation.Done) {
                        return pub.result
                    }
                    val res = queueOp()
                    checkPublications()
                    return res
                } finally {
                    unlock()
                }
            }
            val pub = publicationArray[i].value
            if (pub is Operation.Done) {
                publicationArray[i].value = null
                return pub.result
            }
        }
    }

    private fun checkPublications() {
        for (i in 0 until arraySize) {
            val op = publicationArray[i].value
            if (op != null) {
                val opRes: E? = when (op) {
                    is Operation.Poll -> q.poll()
                    is Operation.Peek -> q.peek()
                    is Operation.Add -> {
                        q.add(op.arg)
                        null
                    }
                    is Operation.Done -> null
                }
                if (op !is Operation.Done) {
                    publicationArray[i].compareAndSet(op, Operation.Done(opRes))
                }
            }
        }
    }

    private fun tryLock() = locked.compareAndSet(expect = false, update = true)

    private fun unlock() {
        locked.value = false
    }
}

private sealed class Operation<E> {
    class Poll<E> : Operation<E>()
    class Peek<E> : Operation<E>()
    class Add<E>(val arg: E) : Operation<E>()
    class Done<E>(val result: E?) : Operation<E>()
}
