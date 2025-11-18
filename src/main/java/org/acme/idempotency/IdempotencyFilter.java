package org.acme.idempotency;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.*;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;

@Provider
@ApplicationScoped
@Priority(Priorities.HEADER_DECORATOR)
public class IdempotencyFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final String IDEMPOTENCY_KEY_HEADER = "X-Idempotency-Key";
    private static final String IDEMPOTENT_CONTEXT_PROPERTY = "idempotent-context";

    private final Cache<String, IdempotencyRecord> cache;

    @Context
    ResourceInfo resourceInfo;

    public IdempotencyFilter() {
        this.cache = Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(Duration.ofHours(1))
                .build();
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        Method method = resourceInfo.getResourceMethod();
        if (method == null) return;

        Idempotent annotation = method.getAnnotation(Idempotent.class);
        if (annotation == null) return;

        String idempotencyKey = requestContext.getHeaderString(IDEMPOTENCY_KEY_HEADER);
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            requestContext.abortWith(Response.status(400)
                    .entity("O cabeçalho X-Idempotency-Key é obrigatório para esta operação.")
                    .build());
            return;
        }

        String cacheKey = requestContext.getMethod() + ":" +
                requestContext.getUriInfo().getPath() + ":" +
                idempotencyKey;

        IdempotencyRecord record = cache.getIfPresent(cacheKey);

        if (record != null) {
            requestContext.abortWith(Response.status(record.status)
                    .entity(record.body)
                    .build());
            return;
        }

        requestContext.setProperty(IDEMPOTENT_CONTEXT_PROPERTY,
                new IdempotentContext(cacheKey, annotation.expireAfter()));
    }

    @Override
    public void filter(ContainerRequestContext req, ContainerResponseContext res) {
        IdempotentContext context = (IdempotentContext) req.getProperty(IDEMPOTENT_CONTEXT_PROPERTY);
        if (context != null) {
            cache.put(context.cacheKey, new IdempotencyRecord(res.getStatus(), res.getEntity()));
        }
    }

    record IdempotentContext(String cacheKey, int expireAfter) {}

    public static class IdempotencyRecord {
        public int status;
        public Object body;
        public IdempotencyRecord(int s, Object b) { this.status = s; this.body = b; }
        public IdempotencyRecord() {}
    }
}