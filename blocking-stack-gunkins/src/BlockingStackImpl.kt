import kotlinx.atomicfu.*
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

@Suppress("UNCHECKED_CAST")
class BlockingStackImpl<E> : BlockingStack<E> {

    // ==========================
    // Segment Queue Synchronizer
    // ==========================

    private val segmentQueue = SegmentQueue<Continuation<E>>()

    private suspend fun suspend(): E {
        return suspendCoroutine { c -> segmentQueue.enqueue(c) }
    }

    private fun resume(element: E) {
        segmentQueue.dequeue().resume(element)
    }

    // ==============
    // Blocking Stack
    // ==============

    private val head = atomic<Node<E>?>(null)
    private val elements = atomic(0)

    override fun push(element: E) {
        val elements = this.elements.getAndIncrement()
        if (elements >= 0) {
            while (true) {
                val curHead = head.value
                if (curHead?.element === SUSPENDED) {
                    if (head.compareAndSet(curHead, curHead.next)) {
                        curHead.continuation!!.resume(element as Any)
                        return
                    }
                } else {
                    if (head.compareAndSet(curHead, Node(element, curHead))) {
                        return
                    }
                }
            }
        } else {
            // resume the next waiting receiver
            resume(element)
        }
    }

    override suspend fun pop(): E {
        val elements = this.elements.getAndDecrement()
        if (elements > 0) {
            // remove the top element from the stack
            while (true) {
                val curHead = head.value
                if (curHead == null) {
                    val res = suspendCoroutine<Any> { c ->
                        if (!head.compareAndSet(curHead, Node(SUSPENDED, continuation = c))) {
                            c.resume(RETRY)
                        }
                    }
                    if (res !== RETRY) {
                        return res as E
                    }
                } else {
                    if (head.compareAndSet(curHead, curHead.next)) {
                        return curHead.element as E
                    }
                }
            }
        } else {
            return suspend()
        }
    }
}

private class Node<E>(
    val element: Any?,
    val next: Node<E>? = null,
    var continuation: Continuation<Any>? = null
)

private val RETRY = Any()
private val SUSPENDED = Any()


class SegmentQueue<T> {
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
    fun dequeue(): T {
        while (true) {
            val curHead = this.head.value
            val deqIdx = curHead.deqIdx.getAndIncrement()
            if (deqIdx >= SEGMENT_SIZE) {
                val nextHead = curHead.next.value ?: continue
                this.head.compareAndSet(curHead, nextHead)
                continue
            }
            val res = curHead.elements[deqIdx].getAndSet(DONE) ?: continue
            return res as T
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

}

private val DONE = Any() // Marker for the "DONE" slot state; to avoid memory leaks
const val SEGMENT_SIZE = 2 // DO NOT CHANGE, IMPORTANT FOR TESTS
