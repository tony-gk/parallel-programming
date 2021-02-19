package stack;

import kotlinx.atomicfu.AtomicArray;
import kotlinx.atomicfu.AtomicRef;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class StackImpl implements Stack {
    private static class Node {
        final AtomicRef<Node> next;
        final int x;

        Node(int x, Node next) {
            this.next = new AtomicRef<>(next);
            this.x = x;
        }
    }

    // head pointer
    private final int SPIN_WAIT = 1000;
    private final int arraySize = Runtime.getRuntime().availableProcessors();
    private final AtomicArray<Integer> eliminationArray = new AtomicArray<>(arraySize);
    private final AtomicRef<Node> head = new AtomicRef<>(null);

    @Override
    public void push(int x) {
        int r = ThreadLocalRandom.current().nextInt(arraySize);
        for (int i = 0; i < arraySize / 2; i++) {
            int pos = (r + i) % arraySize;
            AtomicRef<Integer> ref = eliminationArray.get(pos);
            Integer boxedX = x;
            if (ref.compareAndSet(null, boxedX)) {
                for (int l = 0; l < SPIN_WAIT; l++) {
                    if (ref.getValue() == null) {
                        return;
                    }
                }

                if (ref.compareAndSet(boxedX, null)) {
                    break;
                } else {
                    return;
                }
            }
        }

        while (true) {
            Node curHead = head.getValue();
            Node newHead = new Node(x, curHead);
            if (head.compareAndSet(curHead, newHead)) {
                return;
            }
        }
    }

    @Override
    public int pop() {
        int r = ThreadLocalRandom.current().nextInt(arraySize);
        for (int i = 0; i < arraySize / 2; i++) {
            int pos = (r + i) % arraySize;
            AtomicRef<Integer> valRef = eliminationArray.get(pos);
            Integer val = valRef.getValue();
            if (val != null && valRef.compareAndSet(val, null)) {
                return val;
            }
        }

        while (true) {
            Node curHead = head.getValue();
            if (curHead == null) return Integer.MIN_VALUE;
            if (head.compareAndSet(curHead, curHead.next.getValue())) {
                return curHead.x;
            }
        }
    }
}
