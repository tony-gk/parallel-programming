package msqueue;

import kotlinx.atomicfu.AtomicRef;

public class MSQueue implements Queue {
    private final AtomicRef<Node> head;
    private final AtomicRef<Node> tail;

    public MSQueue() {
        Node dummy = new Node(0);
        this.head = new AtomicRef<>(dummy);
        this.tail = new AtomicRef<>(dummy);
    }

    @Override
    public void enqueue(int x) {
        Node newTail = new Node(x);
        while (true) {
            Node curTail = tail.getValue();
            if (curTail.next.compareAndSet(null, newTail)) {
                tail.compareAndSet(curTail, newTail);
                return;
            } else {
                tail.compareAndSet(curTail, curTail.next.getValue());
            }
        }
    }

    @Override
    public int dequeue() {
        while (true) {
            Node curHead = head.getValue();
            Node curTail = tail.getValue();
            Node next = curHead.next.getValue();
            if (curHead == curTail) {
                if (next == null)
                    return Integer.MIN_VALUE;
                tail.compareAndSet(curTail, curTail.next.getValue());
            } else if (head.compareAndSet(curHead, next)) {
                return next.x;
            }
        }
    }

    @Override
    public int peek() {
        Node next = head.getValue().next.getValue();
        if (next == null)
            return Integer.MIN_VALUE;
        return next.x;
    }

    private static class Node {
        final int x;
        AtomicRef<Node> next;

        Node(int x) {
            this.x = x;
            this.next = new AtomicRef<>(null);
        }
    }
}