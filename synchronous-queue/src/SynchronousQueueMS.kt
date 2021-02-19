import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

class SynchronousQueueMS<E> : SynchronousQueue<E> {
    private val head: AtomicRef<Node<E>>
    private val tail: AtomicRef<Node<E>>

    init {
        val dummy = Node.Dummy<E>()
        head = atomic(dummy)
        tail = atomic(dummy)
    }

    override suspend fun send(element: E) {
        while (true) {
            val curHead = head.value
            val curTail = tail.value

            if (curHead === curTail || curTail is Node.Sender) {
                val res = enqueueAndSuspend(curTail) { c -> Node.Sender(c, element) }
                if (res !== RETRY) break
            } else {
                val next = curHead.next.value
                if (next != null && head.compareAndSet(curHead, next)) {
                    (next as Node.Receiver).cont.resume(element as Any)
                    return
                }
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    override suspend fun receive(): E {
        while (true) {
            val curHead = head.value
            val curTail = tail.value

            if (curHead === curTail || curTail is Node.Receiver) {
                val res = enqueueAndSuspend(curTail) { c -> Node.Receiver(c) }
                if (res === RETRY) continue
                return res as E
            } else {
                val next = curHead.next.value
                if (next != null && head.compareAndSet(curHead, next)) {
                    val sender = next as Node.Sender
                    sender.cont.resume(Any())
                    return sender.element
                }
            }
        }
    }

    private suspend fun enqueueAndSuspend(
        curTail: Node<E>,
        elemBuilder: (Continuation<Any>) -> Node<E>
    ): Any {
        return suspendCoroutine { c ->
            val newElem = elemBuilder(c)
            if (curTail.next.compareAndSet(null, newElem)) {
                tail.compareAndSet(curTail, newElem)
            } else {
                tail.compareAndSet(curTail, curTail.next.value!!)
                c.resume(RETRY)
            }
        }
    }
}

private val RETRY = Any()

private sealed class Node<E> {
    val next = atomic<Node<E>?>(null)

    class Sender<E>(val cont: Continuation<Any>, val element: E) : Node<E>()
    class Receiver<E>(val cont: Continuation<Any>) : Node<E>()
    class Dummy<E> : Node<E>()
}
