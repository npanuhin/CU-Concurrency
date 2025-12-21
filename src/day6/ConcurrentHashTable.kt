@file:Suppress("UNCHECKED_CAST")

package day6

import java.util.concurrent.atomic.*

class ConcurrentHashTable<K : Any, V : Any>(initialCapacity: Int) {
    private val table = AtomicReference(Table<K, V>(initialCapacity))

    fun put(key: K, value: V): V? {
        while (true) {
            // Try to insert the key/value pair.
            val curTable = table.get()
            val putResult = table.get().put(key, value)
            if (putResult === NEEDS_REHASH) {
                // The current table is too small to insert a new key.
                // Create a new table of x2 capacity,
                // copy all elements to it,
                // and restart the current operation.
                resize(curTable)
            } else {
                // The operation has been successfully performed,
                // return the previous value associated with the key.
                return putResult as V?
            }
        }
    }

    fun get(key: K): V? {
        return table.get().get(key)
    }

    fun remove(key: K): V? {
        return table.get().remove(key)
    }

    private fun resize(curTable: Table<K, V>) {
        curTable.nextTable.compareAndSet(null, Table(curTable.capacity * 2))
        repeat(curTable.capacity) { curTable.move(it) }
        table.compareAndSet(curTable, curTable.nextTable.get())
    }

    class Table<K : Any, V : Any>(val capacity: Int) {
        val keys = AtomicReferenceArray<Any?>(capacity)
        val values = AtomicReferenceArray<Any?>(capacity)
        val nextTable = AtomicReference<Table<K, V>>(null)

        fun put(key: K, value: V): Any? {
            val start = hashKey(key)
            var index = start

            while (true) {
                if (nextTable.get() != null) move(index)

                when (val curKey = keys.get(index)) {
                    null -> {
                        val entry = Entry(key, value)
                        if (keys.compareAndSet(index, null, entry)) {
                            putEntry(index, entry)
                            return null
                        }
                        continue
                    }

                    is Entry -> {
                        putEntry(index, curKey)
                        continue
                    }

                    is ForwardingKey -> {
                        if (curKey.key == key) return nextTable.get().put(key, value)
                    }

                    key -> while (true) {
                        val oldValue = values.get(index)
                        if (oldValue is ForwardingValue) {
                            move(index)
                            break
                        }
                        if (values.compareAndSet(index, oldValue, value)) {
                            return if (oldValue === REMOVED) null else oldValue
                        }
                    }
                }
                index = (index + 1) % capacity
                if (index == start) return NEEDS_REHASH
            }
        }

        fun get(key: K): V? {
            val start = hashKey(key)
            var index = start

            while (true) {
                if (nextTable.get() != null) move(index)

                when (val curKey = keys.get(index)) {
                    null -> return null

                    is Entry -> {
                        putEntry(index, curKey)
                        continue
                    }

                    is ForwardingKey -> {
                        if (curKey.key == key) return nextTable.get().get(key)
                    }

                    key -> return when (val curValue = values.get(index)) {
                        REMOVED -> null
                        is ForwardingValue -> curValue.value
                        else -> curValue
                    } as V?
                }
                index = (index + 1) % capacity
                if (index == start) return null
            }
        }

        fun remove(key: K): V? {
            val start = hashKey(key)
            var index = start

            while (true) {
                if (nextTable.get() != null) move(index)

                when (val curKey = keys.get(index)) {
                    null -> return null

                    is Entry -> {
                        putEntry(index, curKey)
                        continue
                    }

                    is ForwardingKey -> {
                        if (curKey.key == key) return nextTable.get().remove(key)
                    }

                    key -> while (true) {
                        when (val curVal = values.get(index)) {
                            REMOVED -> return null

                            is ForwardingValue -> {
                                move(index)
                                return nextTable.get().remove(key)
                            }

                            else -> if (values.compareAndSet(index, curVal, REMOVED)) {
                                return curVal as V?
                            }
                        }
                    }
                }

                index = (index + 1) % capacity
                if (index == start) return null
            }
        }

        fun move(index: Int) {
            val nextTableValues = nextTable.get() ?: return

            while (true) {
                when (val curKey = keys.get(index)) {
                    null,
                    is ForwardingKey
                        -> return

                    is Entry -> {
                        putEntry(index, curKey)
                        continue
                    }

                    else -> {
                        val toInsert = when (val value = values.get(index)) {
                            null,
                            REMOVED
                                -> return

                            is ForwardingValue -> value.value

                            else -> if (!values.compareAndSet(index, value, ForwardingValue(value))) {
                                continue
                            } else {
                                value
                            }
                        }
                        nextTableValues.keys.compareAndSet(
                            nextTableValues.hashKey(curKey as K),
                            null,
                            Entry(curKey, toInsert)
                        )
                        keys.set(index, ForwardingKey(curKey))
                    }
                }
            }
        }

        private fun putEntry(index: Int, kv: Entry) {
            values.compareAndSet(index, null, kv.value)
            keys.compareAndSet(index, kv, kv.key)
        }

        private fun hashKey(key: K): Int = (key.hashCode() and Int.MAX_VALUE) % capacity
    }
}

private val NEEDS_REHASH = Any()
private val REMOVED = Any()

private class Entry(val key: Any?, val value: Any?)
private class ForwardingKey(val key: Any?)
private class ForwardingValue(val value: Any?)
