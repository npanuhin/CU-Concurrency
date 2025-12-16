package day3

import day2.*
import java.util.concurrent.atomic.*

class FAABasedQueue<E> : Queue<E> {
    private val enqIdx = AtomicLong(0)
    private val deqIdx = AtomicLong(0)

    private val first = Segment(0)

    private val head = AtomicReference(first)
    private val tail = AtomicReference(first)

    override fun enqueue(element: E) {
        while (true) {
            val i = enqIdx.getAndIncrement()
            val segId = i / SEGMENT_SIZE
            val pos = (i % SEGMENT_SIZE).toInt()

            val curTail = tail.get()
            val seg = findSegment(curTail, segId)
            moveTailForward(seg)

            if (seg.cells.compareAndSet(pos, null, element)) return
            if (seg.cells.get(pos) === BROKEN) continue
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        while (true) {
            if (!shouldTryToDequeue()) return null

            val i = deqIdx.getAndIncrement()
            val segId = i / SEGMENT_SIZE
            val pos = (i % SEGMENT_SIZE).toInt()

            val curHead = head.get()
            val seg = findSegment(curHead, segId)
            moveHeadForward(seg)

            if (seg.cells.compareAndSet(pos, null, BROKEN)) continue

            val v = seg.cells.get(pos)
            if (v === BROKEN) continue
            return v as E
        }
    }

    private fun shouldTryToDequeue(): Boolean {
        while (true) {
            val d1 = deqIdx.get()
            val e = enqIdx.get()
            val d2 = deqIdx.get()
            if (d1 != d2) continue
            return d1 < e
        }
    }

    private fun findSegment(start: Segment, id: Long): Segment {
        var cur = if (start.id <= id) start else first

        while (cur.id < id) {
            val next = cur.next.get()
            if (next != null) {
                cur = next
                continue
            }

            val newSeg = Segment(cur.id + 1)
            if (cur.next.compareAndSet(null, newSeg)) {
                cur = newSeg
                continue
            }

            val installed = cur.next.get() ?: continue
            cur = installed
        }

        return cur
    }

    private fun moveTailForward(target: Segment) {
        while (true) {
            val curTail = tail.get()
            if (curTail.id >= target.id) return
            if (tail.compareAndSet(curTail, target)) return
        }
    }

    private fun moveHeadForward(target: Segment) {
        while (true) {
            val curHead = head.get()
            if (curHead.id >= target.id) return
            if (head.compareAndSet(curHead, target)) return
        }
    }

    companion object {
        private val BROKEN = Any()
    }
}

private class Segment(val id: Long) {
    val next = AtomicReference<Segment?>(null)
    val cells = AtomicReferenceArray<Any?>(SEGMENT_SIZE)
}

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2