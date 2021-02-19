import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.max
import kotlin.math.min

/**
 * Bank implementation.
 *
 * @author : Gunkin Anton
 */
class BankImpl(n: Int) : Bank {
    private val accounts: Array<Account> = Array(n) { Account() }

    override val numberOfAccounts: Int
        get() = accounts.size

    override fun getAmount(index: Int): Long {
        val account = accounts[index]
        account.lock.withLock {
            return account.amount
        }
    }

    override val totalAmount: Long
        get() {
            accounts.forEach { it.lock.lock() }
            val res = accounts.sumOf { it.amount }
            accounts.forEach { it.lock.unlock() }

            return res
        }

    override fun deposit(index: Int, amount: Long): Long {
        require(amount > 0) { "Invalid amount: $amount" }
        val account = accounts[index]

        account.lock.withLock {
            check(!(amount > Bank.MAX_AMOUNT || account.amount + amount > Bank.MAX_AMOUNT)) { "Overflow" }
            account.amount += amount
            return account.amount
        }
    }

    override fun withdraw(index: Int, amount: Long): Long {
        require(amount > 0) { "Invalid amount: $amount" }
        val account = accounts[index]

        account.lock.withLock {
            check(account.amount - amount >= 0) { "Underflow" }
            account.amount -= amount
            return account.amount
        }
    }

    override fun transfer(fromIndex: Int, toIndex: Int, amount: Long) {
        require(amount > 0) { "Invalid amount: $amount" }
        require(fromIndex != toIndex) { "fromIndex == toIndex" }

        val from = accounts[fromIndex]
        val to = accounts[toIndex]
        val firstLock = accounts[min(fromIndex, toIndex)].lock
        val secondLock = accounts[max(fromIndex, toIndex)].lock
        firstLock.withLock {
            secondLock.withLock {
                check(amount <= from.amount) { "Underflow" }
                check(!(amount > Bank.MAX_AMOUNT || to.amount + amount > Bank.MAX_AMOUNT)) { "Overflow" }
                from.amount -= amount
                to.amount += amount
            }
        }
    }

    /**
     * Private account data structure.
     */
    class Account {
        val lock: ReentrantLock = ReentrantLock()
        var amount: Long = 0
    }
}