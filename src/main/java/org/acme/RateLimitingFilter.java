package org.acme;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.*;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@Provider
@ApplicationScoped
@Priority(Priorities.HEADER_DECORATOR)
public class RateLimitingFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final int MAX_REQUESTS = 10;
    private static final int WINDOW_SECONDS = 60;

    private final Cache<String, AtomicInteger> requestCounts = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofSeconds(WINDOW_SECONDS))
            .build();

    @Override
    public void filter(ContainerRequestContext ctx) throws IOException {
        if (!ctx.getUriInfo().getPath().startsWith("/api/v1")) return;

        String ip = "127.0.0.1";

        AtomicInteger count = requestCounts.get(ip, k -> new AtomicInteger(0));
        int current = count.incrementAndGet();

        ctx.setProperty("rate-limit-remaining", Math.max(0, MAX_REQUESTS - current));

        if (current > MAX_REQUESTS) {
            ctx.abortWith(Response.status(429)
                    .entity("Limite de requisições excedido. Aguarde um momento.")
                    .header("Retry-After", WINDOW_SECONDS)
                    .build());
        }
    }

    @Override
    public void filter(ContainerRequestContext req, ContainerResponseContext res) {
        Object remaining = req.getProperty("rate-limit-remaining");
        if (remaining != null) {
            res.getHeaders().add("X-RateLimit-Limit", MAX_REQUESTS);
            res.getHeaders().add("X-RateLimit-Remaining", remaining);
        }
    }
}