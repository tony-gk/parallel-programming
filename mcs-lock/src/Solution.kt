import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

class Solution(val env: Environment) : Lock<Solution.Node> {
    private val tail = AtomicReference<Node?>(null)

    override fun lock(): Node {
        val my = Node()
        my.locked.value = true
        val pred = tail.getAndSet(my)
        if (pred != null) {
            pred.next.value = my
            while (my.locked.get()) {
                env.park()
            }
        }
        return my
    }

    override fun unlock(node: Node) {
        if (node.next.value == null) {
            if (tail.compareAndSet(node, null)) {
                return
            } else {
                while (node.next.value == null) {
                    // pass
                }
            }
        }
        val next = node.next.value!!
        next.locked.value = false
        env.unpark(next.thread)
    }

    class Node {
        val thread = Thread.currentThread() // запоминаем поток, которые создал узел
        val locked = AtomicReference<Boolean>()
        val next = AtomicReference<Node?>(null)
    }
}