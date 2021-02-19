package dijkstra

import java.lang.Integer.max
import java.lang.Integer.min
import java.util.*
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.Comparator
import kotlin.concurrent.thread
import kotlin.random.Random

private val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> o1!!.distance.compareTo(o2!!.distance) }

// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    start.distance = 0
    val q = MultiQueue(workers, NODE_DISTANCE_COMPARATOR)
    q.add(start)
    val onFinish = Phaser(workers + 1)
    val activeNodes = AtomicInteger(1)
    repeat(workers) {
        thread {
            parallelDijkstra(activeNodes, q)
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}

private fun parallelDijkstra(activeNodes: AtomicInteger, q: MultiQueue<Node>) {
    while (activeNodes.get() > 0) {
        val cur = q.poll()
        for (e in cur.outgoingEdges) {
            while (true) {
                val curDistance = e.to.distance
                val newDistance = cur.distance + e.weight
                if (curDistance > newDistance) {
                    if (e.to.casDistance(curDistance, newDistance)) {
                        q.add(e.to)
                        activeNodes.incrementAndGet()
                    }
                } else {
                    break
                }
            }
        }
        activeNodes.decrementAndGet()
    }
}

class MultiQueue<T : Any>(n: Int, private val comparator: Comparator<T>) {
    private val queues = Array(2 * n) { PriorityQueue<T>(comparator) }
    private val locks = Array(2 * n) { ReentrantLock() }

    fun add(x: T) {
        val q = queues.random()
        synchronized(q) { q.add(x) }
    }

    fun poll(): T {
        while (true) {
            val i1 = queues.randomIndex()
            val i2 = queues.randomIndex()
            val lock1 = locks[i1]
            val lock2 = locks[i2]
            val q1 = queues[i1]
            val q2 = queues[i2]

            if (lock1.tryLock() && lock2.tryLock()) {
                try {
                    val peeked = listOf(q1.peek(), q2.peek())
                    val min = peeked.filterNotNull().minWith(comparator) 
                    return if (min === peeked[0]) {
                        q1.poll()
                    } else {
                        q2.poll()
                    }
                } finally {
                    lock1.unlock()
                    lock2.unlock()
                }
            }
        }
    }
}

fun <E> Array<E>.randomIndex() = this.indices.random()