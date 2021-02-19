package linked_list_set;

import kotlinx.atomicfu.AtomicRef;

public class SetImpl implements Set {

    private static class Removed extends Node {
        final Node node;

        Removed(Node node) {
            super(node.key, node.nextRef);
            this.node = node;
        }
    }

    private static class Node {
        final int key;
        final AtomicRef<Node> nextRef;

        Node(int key, Node nextRef) {
            this.key = key;
            this.nextRef = new AtomicRef<>(nextRef);
        }

        Node(int key, AtomicRef<Node> nextRef) {
            this.key = key;
            this.nextRef = nextRef;
        }
    }

    private static class Window {
        final Node cur;
        final Node next;

        public Window(Node cur, Node next) {
            this.cur = cur;
            this.next = next;
        }
    }

    private final Node head = new Node(Integer.MIN_VALUE, new Node(Integer.MAX_VALUE, (Node) null));

    private Window findWindow(int x) {
        retry:
        while (true) {
            Node cur = head;
            Node next = cur.nextRef.getValue();
            if (next instanceof Removed) {
                next = ((Removed) next).node;
            }
            while (next.key < x) {
                Node node = next.nextRef.getValue();
                if (node instanceof Removed) {
                    if (!cur.nextRef.compareAndSet(next, ((Removed) node).node)) {
                        continue retry;
                    }
                } else {
                    cur = next;
                }
                next = node;
            }

            Node node = next.nextRef.getValue();
            if (node instanceof Removed) {
                cur.nextRef.compareAndSet(next, ((Removed) node).node);
                continue;
            }
            return new Window(cur, next);
        }
    }

    @Override
    public boolean add(int x) {
        while (true) {
            Window w = findWindow(x);
            if (w.next.key == x) {
                return false;
            }
            Node node = new Node(x, w.next);
            if (w.cur.nextRef.compareAndSet(w.next, node)) {
                return true;
            }
        }
    }

    @Override
    public boolean remove(int x) {
        while (true) {
            Window w = findWindow(x);
            if (w.next.key != x) {
                return false;
            }
            Node node = w.next.nextRef.getValue();
            if (node instanceof Removed) {
                return false;
            }
            if (w.next.nextRef.compareAndSet(node, new Removed(node))) {
                return true;
            }
        }
    }

    @Override
    public boolean contains(int x) {
        Window w = findWindow(x);
        return w.next.key == x;
    }
}