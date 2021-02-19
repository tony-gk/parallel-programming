import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.atomicArrayOfNulls
import java.lang.IllegalArgumentException

class DynamicArrayImpl<E> : DynamicArray<E> {
    private val _size = atomic(0)
    private val core = atomic(Core<E>(INITIAL_CAPACITY))

    override fun get(index: Int): E {
        if (index >= size) {
            throw IllegalArgumentException()
        }

        return core.value.array[index].value!!.element
    }

    override fun put(index: Int, element: E) {
        if (index >= size) {
            throw IllegalArgumentException()
        }

        val newElement = JustElement(element)

        var curCore = core.value
        while (true) {
            val elemRef = curCore.array[index]
            when (val el = elemRef.value!!) {
                is JustElement -> {
                    if (elemRef.compareAndSet(el, newElement)) {
                        return
                    }
                }
                is Moved, is Fixed -> {
                    curCore.helpGrow(index)
                    core.compareAndSet(curCore, curCore.next.value!!)
                    curCore = curCore.next.value!!
                }
            }
        }
    }

    override fun pushBack(element: E) {
        val newElement = JustElement(element)

        var curCore = core.value

        while (true) {
            val curSize = size
            if (curSize >= curCore.capacity) {
                if (curSize == curCore.capacity) {
                    curCore.grow()
                    core.compareAndSet(curCore, curCore.next.value!!)
                }
                curCore = core.value
            } else {
                val elemRef = curCore.array[curSize]
                if (elemRef.compareAndSet(null, newElement)) {
                    _size.incrementAndGet()
                    return
                }
            }
        }
    }

    override val size: Int
        get() {
            return _size.value
        }
}

private class Core<E>(
    val capacity: Int
) {
    val array = atomicArrayOfNulls<CoreElement<E>>(capacity)
    val next = atomic<Core<E>?>(null)

    fun grow() {
        next.compareAndSet(null, Core(capacity * 2))

        helpGrow(0)
    }

    fun helpGrow(start: Int) {
        val nextCore = next.value!!
        var i = start

        while (i < capacity) {
            val elemRef = array[i]
            when (val el = elemRef.value!!) {
                is JustElement -> {
                    val fixed = el.fix()
                    if (elemRef.compareAndSet(el, fixed)) {
                        nextCore.array[i].compareAndSet(null, el)
                        elemRef.compareAndSet(fixed, fixed.move())
                        i++
                    }
                }
                is Fixed -> {
                    nextCore.array[i].compareAndSet(null, el.justElem())
                    elemRef.compareAndSet(el, el.move())
                    i++
                }
                is Moved -> {
                    i++
                }
            }
        }
    }
}

private sealed class CoreElement<E>(val element: E) {
    fun justElem() = JustElement(element)

    fun move() = Moved(element)

    fun fix() = Fixed(element)
}

private class JustElement<E>(element: E) : CoreElement<E>(element)

private class Moved<E>(element: E) : CoreElement<E>(element)

private class Fixed<E>(element: E) : CoreElement<E>(element)

private const val INITIAL_CAPACITY = 1 // DO NOT CHANGE ME