@file:Suppress("DuplicatedCode", "UNCHECKED_CAST")

package day5

import java.util.concurrent.atomic.*

// This implementation never stores `null` values.
class AtomicArrayWithCAS2<E : Any>(size: Int, initialValue: E) {
    private val array = AtomicReferenceArray<Any?>(size)

    init {
        // Fill array with the initial value.
        for (i in 0 until size) {
            array[i] = initialValue
        }
    }

    fun get(index: Int): E? {
        while (true) {
            when (val current = array[index]) {
                is Descriptor -> current.apply()
                else -> return current as E?
            }
        }
    }

    fun cas2(
        index1: Int, expected1: E?, update1: E?,
        index2: Int, expected2: E?, update2: E?
    ): Boolean {
        require(index1 != index2) { "The indices should be different" }

        val descriptor = if (index1 < index2) {
            CAS2Descriptor(index1, expected1, update1, index2, expected2, update2)
        } else {
            CAS2Descriptor(index2, expected2, update2, index1, expected1, update1)
        }

        descriptor.apply()

        return descriptor.status.get() == Status.SUCCESS
    }

    private enum class Status { UNDECIDED, SUCCESS, FAILED }

    private interface Descriptor {
        fun apply()
    }

    private inner class CAS2Descriptor(
        val index1: Int, val expected1: E?, val update1: E?,
        val index2: Int, val expected2: E?, val update2: E?
    ) : Descriptor {
        val status = AtomicReference(Status.UNDECIDED)

        override fun apply() {
            if (status.get() == Status.UNDECIDED) {
                if (tryLockIndex1()) {
                    val dcss = DCSSDescriptor(index2, expected2, this)
                    dcss.apply()
                    val result = if (dcss.state.get() == Status.SUCCESS) Status.SUCCESS else Status.FAILED
                    status.compareAndSet(Status.UNDECIDED, result)
                } else {
                    status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                }
            }

            val isSuccess = status.get() == Status.SUCCESS
            updateCell(index1, if (isSuccess) update1 else expected1)
            updateCell(index2, if (isSuccess) update2 else expected2)
        }

        private fun tryLockIndex1(): Boolean {
            while (true) {
                val current = array[index1]
                if (current === this) return true
                if (current is Descriptor) {
                    current.apply()
                    continue
                }
                if (current != expected1) return false
                if (array.compareAndSet(index1, current, this)) return true
            }
        }

        private fun updateCell(index: Int, value: Any?) {
            val current = array[index]
            if (current === this) {
                array.compareAndSet(index, this, value)
            }
        }
    }

    private inner class DCSSDescriptor(
        val index: Int, val expectedValue: E?, val descriptor: CAS2Descriptor
    ) : Descriptor {
        val state = AtomicReference(Status.UNDECIDED)

        override fun apply() {
            if (state.get() == Status.UNDECIDED) {
                if (install()) {
                    val parentState = descriptor.status.get()
                    val result = if (parentState == Status.UNDECIDED) Status.SUCCESS else Status.FAILED
                    state.compareAndSet(Status.UNDECIDED, result)
                } else {
                    state.compareAndSet(Status.UNDECIDED, Status.FAILED)
                }
            }

            val isSuccess = state.get() == Status.SUCCESS
            val replacement = if (isSuccess) descriptor else expectedValue
            val current = array[index]
            if (current === this) {
                array.compareAndSet(index, this, replacement)
            }
        }

        private fun install(): Boolean {
            while (true) {
                val current = array[index]
                if (current === this) return true
                if (current === descriptor) return true
                if (current is Descriptor) {
                    current.apply()
                    continue
                }
                if (current != expectedValue) return false
                if (array.compareAndSet(index, current, this)) return true
            }
        }
    }
}