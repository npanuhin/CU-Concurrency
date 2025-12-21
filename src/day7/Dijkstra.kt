package day7

import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.*
import kotlin.math.max
import kotlin.math.min

private val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> Integer.compare(o1!!.distance, o2!!.distance) }

class PriorityMultiQueue<T>(
    private val queuesNum: Int,
    private val comparator: Comparator<T>,
) {
    private val locks = Array(queuesNum) { ReentrantLock() }
    private val queues = Array(queuesNum) { PriorityQueue(comparator) }

    fun poll(): T? {
        while (true) {
            val index1 = ThreadLocalRandom.current().nextInt(queuesNum)
            val index2 = ThreadLocalRandom.current().nextInt(queuesNum)
            if (index1 == index2) continue

            val first = min(index1, index2)
            val second = max(index1, index2)

            if (!locks[first].tryLock()) {
                continue
            }
            if (!locks[second].tryLock()) {
                locks[first].unlock()
                continue
            }

            val queue1 = queues[index1]
            val queue2 = queues[index2]

            val top1 = queue1.peek()
            val top2 = queue2.peek()

            return when {
                top1 == null && top2 == null -> null
                top1 == null -> queue2.poll()
                top2 == null -> queue1.poll()

                comparator.compare(top1, top2) < 0
                    -> queue1.poll()

                else -> queue2.poll()

            }.apply {
                locks[second].unlock()
                locks[first].unlock()
            }
        }
    }

    fun add(task: T) {
        while (true) {
            val index = ThreadLocalRandom.current().nextInt(queuesNum)
            if (locks[index].tryLock()) {
                queues[index].add(task)
                locks[index].unlock()
                return
            }
        }
    }
}

// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    start.distance = 0
    // Create a priority (by distance) queue and add the start node into it
    val q = PriorityMultiQueue(workers, NODE_DISTANCE_COMPARATOR) // TODO replace me with a multi-queue based PQ!
    q.add(start)
    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker

    val active = AtomicInteger(1)

    repeat(workers) {
        thread {
            while (true) {
                val curNode = q.poll()
                    ?: if (active.get() == 0) {
                        break
                    } else {
                        continue
                    }

                for (edge in curNode.outgoingEdges) {
                    while (true) {
                        val curDist = edge.to.distance
                        val newDist = curNode.distance + edge.weight
                        if (newDist >= curDist) break
                        if (edge.to.casDistance(curDist, newDist)) {
                            active.incrementAndGet()
                            q.add(edge.to)
                            break
                        }
                    }
                }
                active.decrementAndGet()
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}