/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.io.Closeable;
import org.opensearch.telemetry.tracing.attributes.Attributes;

/**
 * Tracer is the interface used to create a {@link Span}
 * It automatically handles the context propagation between threads, tasks, nodes etc.
 *
 * All methods on the Tracer object are multi-thread safe.
 */
public interface Tracer extends Closeable {

    /**
     * Starts the {@link Span} with given name
     *
     * @param spanName span name
     * @return scope of the span, must be closed with explicit close or with try-with-resource
     */
    SpanScope startSpan(String spanName);

    /**
     * Starts the {@link Span} with given name and attributes. This is required in cases when some attribute based
     * decision needs to be made before starting the span. Very useful in the case of Sampling.
     * @param spanName span name.
     * @param attributes attributes to be added.
     * @return scope of the span, must be closed with explicit close or with try-with-resource
     */
    SpanScope startSpan(String spanName, Attributes attributes);

    /**
     * Starts the {@link Span} with the given name, parent and attributes.
     * @param spanName span name.
     * @param parentSpan parent span.
     * @param attributes attributes to be added.
     * @return scope of the span, must be closed with explicit close or with try-with-resource
     */
    SpanScope startSpan(String spanName, SpanContext parentSpan, Attributes attributes);

    /**
     * Returns the current span.
     * @return current wrapped span.
     */
    SpanContext getCurrentSpan();
}
