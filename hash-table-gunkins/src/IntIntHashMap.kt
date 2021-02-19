import kotlinx.atomicfu.AtomicIntArray
import kotlinx.atomicfu.atomic

/**
 * Int-to-Int hash map with open addressing and linear probes.
 *
 * TODO: This class is **NOT** thread-safe.
 */
class IntIntHashMap {
    private val core = atomic(Core(INITIAL_CAPACITY))

    /**
     * Returns value for the corresponding key or zero if this key is not present.
     *
     * @param key a positive key.
     * @return value for the corresponding or zero if this key is not present.
     * @throws IllegalArgumentException if key is not positive.
     */
    operator fun get(key: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        return toValue(getAndRehashWhileNeeded(key))
    }

    /**
     * Changes value for the corresponding key and returns old value or zero if key was not present.
     *
     * @param key   a positive key.
     * @param value a positive value.
     * @return old value or zero if this key was not present.
     * @throws IllegalArgumentException if key or value are not positive, or value is equal to
     * [Integer.MAX_VALUE] which is reserved.
     */
    fun put(key: Int, value: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        require(isValue(value)) { "Invalid value: $value" }
        return toValue(putAndRehashWhileNeeded(key, value))
    }

    /**
     * Removes value for the corresponding key and returns old value or zero if key was not present.
     *
     * @param key a positive key.
     * @return old value or zero if this key was not present.
     * @throws IllegalArgumentException if key is not positive.
     */
    fun remove(key: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        return toValue(putAndRehashWhileNeeded(key, DEL_VALUE))
    }

    private fun putAndRehashWhileNeeded(key: Int, value: Int): Int {
        while (true) {
            val curCore = core.value
            val oldValue = curCore.putInternal(key, value)
            if (oldValue != NEEDS_REHASH) return oldValue

            curCore.rehash()
            core.compareAndSet(curCore, curCore.next.value!!)
        }
    }

    private fun getAndRehashWhileNeeded(key: Int): Int {
        while (true) {
            val curCore: Core = core.value
            val res = curCore.getInternal(key)
            if (res != NEEDS_REHASH) return res ?: 0

            curCore.rehash()
            core.compareAndSet(curCore, curCore.next.value!!)
        }
    }

    private class Core internal constructor(capacity: Int) {
        // Pairs of <key, value> here, the actual
        // size of the map is twice as big.
        val size = capacity * 2
        val map = AtomicIntArray(size)
        val next = atomic<Core?>(null)
        val shift: Int

        init {
            val mask = capacity - 1
            assert(mask > 0 && mask and capacity == 0) { "Capacity must be power of 2: $capacity" }
            shift = 32 - Integer.bitCount(mask)
        }

        fun getInternal(key: Int): Int {
            var index = index(key)
            var probes = 0
            while (probes < MAX_PROBES) {
                val mapValue = map[index + 1].value
                val mapKey = map[index].value

                if (mapValue == MOVED_VALUE) return NEEDS_REHASH
                if (mapKey == NULL_KEY) return NULL_VALUE
                if (mapKey == key) return fromFixed(mapValue)

                if (index == 0) {
                    index = size
                }
                probes++
                index -= 2
            }
            return NULL_VALUE
        }


        fun putInternal(key: Int, value: Int): Int {
            var index = index(key)
            var probes = 0
            while (probes < MAX_PROBES) {
                val mapKey: Int = map[index].value
                val mapValue: Int = map[index + 1].value
                if (isFixed(mapValue)) return NEEDS_REHASH
                if (mapKey == NULL_KEY) {
                    if (value == DEL_VALUE) {
                        return NULL_VALUE
                    }
                    if (map[index].compareAndSet(NULL_KEY, key)) {
                        if (map[index + 1].compareAndSet(NULL_VALUE, value)) {
                            return mapValue
                        }
                    }
                    continue
                }
                if (mapKey == key) {
                    if (map[index + 1].compareAndSet(mapValue, value)) {
                        return mapValue
                    }
                    continue
                }
                if (index == 0) {
                    index = size
                }
                index -= 2
                probes++
            }
            return NEEDS_REHASH
        }

        fun rehash() {
            next.compareAndSet(null, Core(size * 2))
            for (i in 0 until size step 2) {
                if (map[i + 1].value == MOVED_VALUE) continue

                val value = fixValue(i + 1)
                if (isValue(value)) {
                    next.value!!.moveFixed(map[i].value, value)
                }
                map[i + 1].getAndSet(MOVED_VALUE)
            }
        }

        fun fixValue(index: Int): Int {
            while (true) {
                val value: Int = map[index].value
                if (isFixed(value) || map[index].compareAndSet(value, toFixed(value))) {
                    return fromFixed(value)
                }
            }
        }

        fun moveFixed(key: Int, value: Int) {
            check(key > 0) { "Move key <= 0" }
            var probes = 0
            var index = index(key)
            while (probes < MAX_PROBES) {
                val mapKey: Int = map[index].value
                if (mapKey == NULL_KEY) {
                    if (map[index].compareAndSet(NULL_KEY, key)) {
                        if (map[index + 1].compareAndSet(NULL_VALUE, value)) {
                            return
                        }
                    }
                    continue
                }
                if (mapKey == key) {
                    map[index + 1].compareAndSet(NULL_VALUE, value)
                    return
                }
                if (index == 0) {
                    index = size
                }
                index -= 2
                probes++
            }
            throw IllegalStateException("Exceeded prob limit during elements moving")
        }

        /**
         * Returns an initial index in map to look for a given key.
         */
        fun index(key: Int): Int = (key * MAGIC ushr shift) * 2
    }
}

private const val MAGIC = -0x61c88647 // golden ratio
private const val INITIAL_CAPACITY = 2 // !!! DO NOT CHANGE INITIAL CAPACITY !!!
private const val MAX_PROBES = 8 // max number of probes to find an item
private const val NULL_KEY = 0 // missing key (initial value)
private const val NULL_VALUE = 0 // missing value (initial value)
private const val DEL_VALUE = Int.MAX_VALUE // mark for removed value
private const val MOVED_VALUE = Int.MIN_VALUE // mark for moved value
private const val NEEDS_REHASH = -1 // returned by `putInternal` to indicate that rehash is needed

// Checks is the value is in the range of allowed values
private fun isValue(value: Int): Boolean = value in (1 until DEL_VALUE)

// Converts internal value to the public results of the methods
private fun toValue(value: Int): Int = if (isValue(value)) value else 0

private fun toFixed(value: Int): Int {
    return value or (1 shl 31)
}

private fun fromFixed(value: Int): Int {
    return value and (1 shl 31).inv()
}

private fun isFixed(value: Int): Boolean {
    return (value and (1 shl 31)) != 0
}
