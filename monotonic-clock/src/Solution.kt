/**
 * В теле класса решения разрешено использовать только переменные делегированные в класс RegularInt.
 * Нельзя volatile, нельзя другие типы, нельзя блокировки, нельзя лазить в глобальные переменные.
 *
 * @author : Gunkin Anton
 */
class Solution : MonotonicClock {
    private var c11 by RegularInt(0)
    private var c12 by RegularInt(0)
    private var c13 by RegularInt(0)

    private var c21 by RegularInt(0)
    private var c22 by RegularInt(0)
    private var c23 by RegularInt(0)

    override fun write(time: Time) {
        // -->
        c21 = time.d1
        c22 = time.d2
        c23 = time.d3

        // <--
        c13 = time.d3
        c12 = time.d2
        c11 = time.d1
    }

    override fun read(): Time {
        // -->
        val t1: List<Int> = listOf(c11, c12, c13)
        // <--
        val rl3: Int = c23
        val rl2: Int = c22
        val rl1: Int = c21
        val t2: List<Int> = listOf(rl1, rl2, rl3)

        return when ((0..2).takeWhile { i -> t1[i] == t2[i] }.count()) {
            3 -> Time(t1[0], t1[1], t1[2])
            2 -> Time(t2[0], t2[1], t2[2])
            1 -> Time(t2[0], t2[1], 0)
            else -> Time(t2[0], 0, 0)
        }
    }
}