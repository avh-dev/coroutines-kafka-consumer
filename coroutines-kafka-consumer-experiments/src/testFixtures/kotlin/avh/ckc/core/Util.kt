package avh.ckc.core

import kotlin.random.Random

fun shuffleLocal(
    n: Int,
    maxDiff: Int,
    rnd: Random = Random.Default
): LongArray {
    require(n >= 0)
    require(maxDiff >= 0)
    val result = LongArray(n) { it.toLong() }

    for (i in 0..n - 65 step 64) {
        shuffleRange(result, i, i + 64, rnd)
    }
    for (i in 32..n - 65 step 64) {
        shuffleRange(result, i, i + 64, rnd)
    }
    return result
}

private fun shuffleRange(arr: LongArray, from: Int, to: Int, rnd: Random) {
    require(from >= 0 && to < arr.size && from <= to)

    for (i in to downTo from + 1) {
        val j = from + rnd.nextInt(i - from + 1)
        val tmp = arr[i]
        arr[i] = arr[j]
        arr[j] = tmp
    }
}
