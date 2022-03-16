package golan.hello.redis.array;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ArrayConcurrent {
    private static final String RED_KEY = "test_array_parallelism_" + UUID.randomUUID();
    private static final int RED_POOL_SIZE =     10;
    private static final int INSERT_COUNT  = 10_000;
    private static final int RANGE_SIZE    =     10;
    private static final int EX_POOL_SIZE = 100;

    @SneakyThrows
    public static void main(String[] args) {
        final JedisPoolConfig poolConfig = getPoolConfig();
        try (JedisPool jedisPool = new JedisPool(poolConfig, "localhost", 6379, 5000)) {

            final ExecutorService executorService = Executors.newFixedThreadPool(EX_POOL_SIZE);
            int total = 0;
            while (total < INSERT_COUNT) {
                executorService.submit(new Writer(jedisPool, total, total + RANGE_SIZE));
                total += RANGE_SIZE;
            }

            executorService.shutdown();
            final boolean ok = executorService.awaitTermination(15, TimeUnit.SECONDS);
            if (!ok) {
                log.error("We waited for ExecutorService termination but we reached timeout");
            }

            try (Jedis jedis = jedisPool.getResource()) {
                final List<String> list = jedis.lrange(RED_KEY, 0, INSERT_COUNT);
                final Integer sum = list
                        .stream()
                        .map(Integer::parseInt)
                        .reduce(Integer::sum)
                        .orElse(0);
                int expected = (int) (((INSERT_COUNT-1) / 2.0) * INSERT_COUNT);
                if (sum != expected) {
                    log.error("Expected {} but was {}", expected, sum);
                }
            }
        }
    }

    private static JedisPoolConfig getPoolConfig() {
        final JedisPoolConfig pc = new JedisPoolConfig();
        pc.setMaxTotal(RED_POOL_SIZE);
        return pc;
    }

    @RequiredArgsConstructor
    private static class Writer implements Runnable {
        private final JedisPool jedisPool;
        private final int from;
        private final int to;

        @Override
        public void run() {
            log.info("Will insert [{},{}]", from, to);
            try (Jedis jedis = jedisPool.getResource()) {
                for (int i=from ; i<to ; i++) {
                    jedis.lpush(RED_KEY, ""+i);
                }
            }
        }
    }
}
