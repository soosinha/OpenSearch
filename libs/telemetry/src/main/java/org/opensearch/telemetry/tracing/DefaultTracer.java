/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.io.Closeable;
import java.io.IOException;
import org.opensearch.telemetry.tracing.attributes.Attributes;

/**
 *
 * The default tracer implementation. It handles tracing context propagation between spans by maintaining
 * current active span in its storage
 *
 *  @opensearch.internal
 */
class DefaultTracer implements Tracer {
    static final String THREAD_NAME = "th_name";

    private final TracingTelemetry tracingTelemetry;
    private final TracerContextStorage<String, Span> tracerContextStorage;

    /**
     * Creates DefaultTracer instance
     *
     * @param tracingTelemetry tracing telemetry instance
     * @param tracerContextStorage storage used for storing current span context
     */
    public DefaultTracer(TracingTelemetry tracingTelemetry, TracerContextStorage<String, Span> tracerContextStorage) {
        this.tracingTelemetry = tracingTelemetry;
        this.tracerContextStorage = tracerContextStorage;
    }

    @Override
    public SpanScope startSpan(String spanName) {
        return startSpan(spanName, Attributes.EMPTY);
    }

    @Override
    public SpanScope startSpan(String spanName, Attributes attributes) {
        return startSpan(spanName, null, attributes);
    }

    @Override
    public SpanScope startSpan(String spanName, SpanContext parentSpan, Attributes attributes) {
        Span span = null;
        if (parentSpan != null) {
            span = createSpan(spanName, parentSpan.getSpan(), attributes);
        } else {
            span = createSpan(spanName, getCurrentSpanInternal(), attributes);
        }
        setCurrentSpanInContext(span);
        addDefaultAttributes(span);
        return new DefaultSpanScope(span, (scopeSpan) -> endSpan(scopeSpan));
    }

    @Override
    public void close() throws IOException {
        ((Closeable) tracingTelemetry).close();
    }

    private Span getCurrentSpanInternal() {
        return tracerContextStorage.get(TracerContextStorage.CURRENT_SPAN);
    }

    public SpanContext getCurrentSpan() {
        final Span currentSpan = tracerContextStorage.get(TracerContextStorage.CURRENT_SPAN);
        return (currentSpan == null) ? null : new SpanContext(currentSpan);
    }

    private void endSpan(Span span) {
        if (span != null) {
            span.endSpan();
            setCurrentSpanInContext(span.getParentSpan());
        }
    }

    private Span createSpan(String spanName, Span parentSpan, Attributes attributes) {
        return tracingTelemetry.createSpan(spanName, parentSpan, attributes);
    }

    private void setCurrentSpanInContext(Span span) {
        tracerContextStorage.put(TracerContextStorage.CURRENT_SPAN, span);
    }

    /**
     * Adds default attributes in the span
     * @param span the current active span
     */
    protected void addDefaultAttributes(Span span) {
        span.addAttribute(THREAD_NAME, Thread.currentThread().getName());
    }

}
