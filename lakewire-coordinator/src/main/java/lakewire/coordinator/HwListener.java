package lakewire.coordinator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One dedicated connection, not taken from the pool. Postgres delivers
 * {@code sl_hw} only after the notifying transaction commits.
 */
public final class HwListener implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HwListener.class);
    private static final long INITIAL_BACKOFF_MS = 100;
    private static final long MAX_BACKOFF_MS = 5_000;

    private final CoordinatorConfig config;
    private final List<Consumer<HwUpdate>> subscribers = new CopyOnWriteArrayList<>();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Object ready = new Object();

    private volatile boolean listening;
    private volatile Connection connection;
    private Thread thread;

    public HwListener(CoordinatorConfig config) {
        this.config = config;
    }

    public void subscribe(Consumer<HwUpdate> subscriber) {
        subscribers.add(subscriber);
    }

    public synchronized void start() {
        if (thread != null) {
            return;
        }
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException failure) {
            throw new CoordinatorException("postgres driver is not on the classpath", failure);
        }
        thread = Thread.ofVirtual().name("lakewire-hw-listener").start(this::loop);
    }

    public void awaitReady(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (ready) {
            while (!listening) {
                long remainingMs = (deadline - System.nanoTime()) / 1_000_000;
                if (remainingMs <= 0) {
                    throw new CoordinatorException("hw listener did not subscribe in time");
                }
                try {
                    ready.wait(remainingMs);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new CoordinatorException("hw listener wait interrupted", interrupted);
                }
            }
        }
    }

    @Override
    public void close() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        Connection current = connection;
        if (current != null) {
            try {
                current.close();
            } catch (SQLException failure) {
                log.warn("hw listener close failed: {}", failure.toString());
            }
        }
        Thread currentThread = thread;
        if (currentThread != null) {
            currentThread.interrupt();
            try {
                currentThread.join(Duration.ofSeconds(5));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void loop() {
        long backoffMs = INITIAL_BACKOFF_MS;
        while (!stopped.get()) {
            try {
                Connection opened = DriverManager.getConnection(
                    config.jdbcUrl(), config.username(), config.password());
                connection = opened;
                opened.setAutoCommit(true);
                try (Statement statement = opened.createStatement()) {
                    statement.execute("LISTEN sl_hw");
                }
                synchronized (ready) {
                    listening = true;
                    ready.notifyAll();
                }
                backoffMs = INITIAL_BACKOFF_MS;
                PGConnection notifications = opened.unwrap(PGConnection.class);
                while (!stopped.get()) {
                    PGNotification[] batch = notifications.getNotifications(1_000);
                    if (batch == null) {
                        continue;
                    }
                    for (PGNotification notification : batch) {
                        dispatch(notification.getParameter());
                    }
                }
            } catch (SQLException failure) {
                if (!stopped.get()) {
                    log.warn("hw listener disconnected: {}", failure.toString());
                }
            } finally {
                synchronized (ready) {
                    listening = false;
                }
                Connection current = connection;
                connection = null;
                if (current != null) {
                    try {
                        current.close();
                    } catch (SQLException ignored) {
                        // The next loop opens a new connection.
                    }
                }
            }
            if (stopped.get()) {
                return;
            }
            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException interrupted) {
                if (stopped.get()) {
                    return;
                }
                Thread.currentThread().interrupt();
                return;
            }
            backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
        }
    }

    private void dispatch(String payload) {
        if (payload == null) {
            return;
        }
        String[] parts = payload.split(":", -1);
        if (parts.length != 3) {
            log.warn("ignoring sl_hw payload");
            return;
        }
        HwUpdate update;
        try {
            update = new HwUpdate(
                Long.parseLong(parts[0]),
                Integer.parseInt(parts[1]),
                Long.parseLong(parts[2])
            );
        } catch (NumberFormatException failure) {
            log.warn("ignoring sl_hw payload");
            return;
        }
        for (Consumer<HwUpdate> subscriber : subscribers) {
            try {
                subscriber.accept(update);
            } catch (RuntimeException failure) {
                log.warn("sl_hw subscriber failed: {}", failure.toString());
            }
        }
    }
}
