/**
 * @author :TODO: Gunkin Anton
 */
class Solution : AtomicCounter {
    private val common = Node(0)
    private val last = ThreadLocal.withInitial { common }

    override fun getAndAdd(x: Int): Int {
        while (true) {
            val oldValue = last.get().value
            val newValue = oldValue + x
            val newNode = Node(newValue)
            val nextNode = last.get().nextConsensus.decide(newNode)
            last.set(nextNode)
            if (nextNode === newNode) {
                return oldValue
            }
        }
    }

    // вам наверняка потребуется дополнительный класс
    private class Node(val value: Int, val nextConsensus: Consensus<Node> = Consensus())
}
