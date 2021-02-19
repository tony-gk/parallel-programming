import kotlinx.atomicfu.*

class FAAQueue<T> {
    private val head: AtomicRef<Segment> // Head pointer, similarly to the Michael-Scott queue (but the first node is _not_ sentinel)
    private val tail: AtomicRef<Segment> // Tail pointer, similarly to the Michael-Scott queue

    init {
        val firstSegment = Segment()
        head = atomic(firstSegment)
        tail = atomic(firstSegment)
    }

    /**
     * Adds the specified element [x] to the queue.
     */
    fun enqueue(x: T) {
        while (true) {
            val curTail = this.tail.value
            val enqIdx = curTail.enqIdx.getAndIncrement()
            if (enqIdx >= SEGMENT_SIZE) {
                val nextSegment = Segment(x)
                if (curTail.next.compareAndSet(null, nextSegment)) {
                    this.tail.compareAndSet(curTail, nextSegment)
                    return
                } else {
                    this.tail.compareAndSet(curTail, curTail.next.value!!)
                }
            } else {
                if (curTail.elements[enqIdx].compareAndSet(null, x)) {
                    return
                }
            }
        }
    }

    /**
     * Retrieves the first element from the queue
     * and returns it; returns `null` if the queue
     * is empty.
     */
    fun dequeue(): T? {
        while (true) {
            val curHead = this.head.value
            val deqIdx = curHead.deqIdx.getAndIncrement()
            if (deqIdx >= SEGMENT_SIZE) {
                val nextHead = curHead.next.value ?: return null
                this.head.compareAndSet(curHead, nextHead)
                continue
            }
            val res = curHead.elements[deqIdx].getAndSet(DONE) ?: continue
            return res as T?
        }
    }

    /**
     * Returns `true` if this queue is empty;
     * `false` otherwise.
     */
    val isEmpty: Boolean
        get() {
            while (true) {
                val curHead = this.head.value
                if (curHead.isEmpty) {
                    if (curHead.next.value == null) return true
                    this.head.compareAndSet(curHead, curHead.next.value!!)
                    continue
                } else {
                    return false
                }
            }
        }
}

private class Segment {
    val next = atomic<Segment?>(null)
    val enqIdx = atomic(0) // index for the next enqueue operation
    val deqIdx = atomic(0) // index for the next dequeue operation
    val elements = atomicArrayOfNulls<Any>(SEGMENT_SIZE)

    constructor() // for the first segment creation

    constructor(x: Any?) { // each next new segment should be constructed with an element
        enqIdx.value = 1
        elements[0].value = x
    }

    val isEmpty: Boolean get() = deqIdx.value >= enqIdx.value || deqIdx.value >= SEGMENT_SIZE

}

private val DONE = Any() // Marker for the "DONE" slot state; to avoid memory leaks
const val SEGMENT_SIZE = 2 // DO NOT CHANGE, IMPORTANT FOR TESTS

