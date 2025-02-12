# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import logging
import os
import queue
import threading
import time
import typing
from os import environ
from time import time_ns


from opentelemetry._logs.severity import SeverityNumber
from opentelemetry.context import (
    _SUPPRESS_INSTRUMENTATION_KEY,
    Context,
    attach,
    detach,
    set_value,
)
from opentelemetry.sdk._logs import LogData, LogRecord, LogRecordProcessor
from opentelemetry.sdk.environment_variables import (
    OTEL_BSP_EXPORT_TIMEOUT,
    OTEL_BSP_MAX_EXPORT_BATCH_SIZE,
    OTEL_BSP_MAX_QUEUE_SIZE,
    OTEL_BSP_SCHEDULE_DELAY,
    OTEL_BSP_PARTIAL_SPANS_ENABLED,
)
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
from opentelemetry.sdk.trace.export import SpanExporter
from opentelemetry.sdk.trace.export import _FlushRequest
from opentelemetry.trace import TraceFlags

from opentelemetry.util._once import Once

_BSP_RESET_ONCE = Once()

_DEFAULT_SCHEDULE_DELAY_MILLIS = 5000
_DEFAULT_MAX_EXPORT_BATCH_SIZE = 512
_DEFAULT_EXPORT_TIMEOUT_MILLIS = 30000
_DEFAULT_MAX_QUEUE_SIZE = 2048
_ENV_VAR_INT_VALUE_ERROR_MESSAGE = (
    "Unable to parse value for %s as integer. Defaulting to %s."
)

logger = logging.getLogger(__name__)

class PartialSpanProcessor(SpanProcessor):
    """Partial span processor implementation.
    `PartialSpanProcessor` is an implementation of `SpanProcessor` that
    batches ended spans and pushes them to the configured `SpanExporter`.
    `PartialSpanProcessor` is configurable with the following environment
    variables which correspond to constructor parameters:
    - :envvar:`OTEL_BSP_SCHEDULE_DELAY`
    - :envvar:`OTEL_BSP_MAX_QUEUE_SIZE`
    - :envvar:`OTEL_BSP_MAX_EXPORT_BATCH_SIZE`
    - :envvar:`OTEL_BSP_EXPORT_TIMEOUT`
    """

    def __init__(
        self,
        span_exporter: SpanExporter,
        log_processor: LogRecordProcessor,
        max_queue_size: int = None,
        schedule_delay_millis: float = None,
        max_export_batch_size: int = None,
        export_timeout_millis: float = None,
    ):
        self.log_processor = log_processor
        self.lock = threading.Lock()

        if max_queue_size is None:
            max_queue_size = PartialSpanProcessor._default_max_queue_size()

        if schedule_delay_millis is None:
            schedule_delay_millis = (
                PartialSpanProcessor._default_schedule_delay_millis()
            )

        if max_export_batch_size is None:
            max_export_batch_size = (
                PartialSpanProcessor._default_max_export_batch_size()
            )

        if export_timeout_millis is None:
            export_timeout_millis = (
                PartialSpanProcessor._default_export_timeout_millis()
            )

        PartialSpanProcessor._validate_arguments(
            max_queue_size, schedule_delay_millis, max_export_batch_size
        )

        self.span_exporter = span_exporter
        self.queue = collections.deque([], max_queue_size)  # type: typing.Deque[Span]
        self.worker_thread = threading.Thread(
            name="OtelPartialSpanProcessor", target=self.worker, daemon=True
        )
        self.condition = threading.Condition(threading.Lock())
        self._flush_request = None  # type: typing.Optional[_FlushRequest]
        self.schedule_delay_millis = schedule_delay_millis
        self.max_export_batch_size = max_export_batch_size
        self.max_queue_size = max_queue_size
        self.export_timeout_millis = export_timeout_millis
        self.done = False
        # flag that indicates that spans are being dropped
        self._spans_dropped = False
        # precallocated list to send spans to exporter
        self.spans_list = [None] * self.max_export_batch_size  # type: typing.List[typing.Optional[Span]]
        self.worker_thread.start()
        if hasattr(os, "register_at_fork"):
            os.register_at_fork(after_in_child=self._at_fork_reinit)  # pylint: disable=protected-access
        self._pid = os.getpid()
        self.active_spans = {}
        self.ended_spans = queue.Queue()

    def on_start(
        self, span: Span, parent_context: typing.Optional[Context] = None
    ) -> None:
        span_key = (span.context.trace_id, span.context.span_id)
        with self.lock:
            self.active_spans[span_key] = span
        attributes = self.get_heartbeat_attributes()

        log_data = get_logdata(span, attributes)
        self.log_processor.emit(log_data)

    def on_end(self, span: ReadableSpan) -> None:
        span_key = (span.context.trace_id, span.context.span_id)
        self.ended_spans.put((span_key, span))

        attributes = {
            "partial.event": "stop",
            # TODO should this be removed?
            "telemetry.logs.cluster": "partial",
            "telemetry.logs.project": "span",
        }

        log_data = get_logdata(span, attributes)
        self.log_processor.emit(log_data)

        if self.done:
            logger.warning("Already shutdown, dropping span.")
            return
        if not span.context.trace_flags.sampled:
            return
        if self._pid != os.getpid():
            _BSP_RESET_ONCE.do_once(self._at_fork_reinit)

        if len(self.queue) == self.max_queue_size:
            if not self._spans_dropped:
                logger.warning("Queue is full, likely spans will be dropped.")
                self._spans_dropped = True

        self.queue.appendleft(span)

        if len(self.queue) >= self.max_export_batch_size:
            with self.condition:
                self.condition.notify()

    def _at_fork_reinit(self):
        self.condition = threading.Condition(threading.Lock())
        self.queue.clear()

        # worker_thread is local to a process, only the thread that issued fork continues
        # to exist. A new worker thread must be started in child process.
        self.worker_thread = threading.Thread(
            name="OtelPartialSpanProcessor", target=self.worker, daemon=True
        )
        self.worker_thread.start()
        self._pid = os.getpid()

    def heartbeat(self):
        # remove ended spans from active spans
        with self.lock:
            while not self.ended_spans.empty():
                span_key, span = self.ended_spans.get()
                self.active_spans.pop(span_key, None)

        attributes = self.get_heartbeat_attributes()

        with self.lock:
            for span_key, span in self.active_spans.items():
                log_data = get_logdata(span, attributes)
                self.log_processor.emit(log_data)

    def get_heartbeat_attributes(self):
        return {
            "partial.event": "heartbeat",
            "partial.frequency": str(self._default_schedule_delay_millis())
            + "ms",
            # TODO should this be removed?
            "telemetry.logs.cluster": "partial",
            "telemetry.logs.project": "span",
        }

    def worker(self):
        timeout = self.schedule_delay_millis / 1e3
        flush_request = None  # type: typing.Optional[_FlushRequest]
        while not self.done:
            with self.condition:
                if self.done:
                    # done flag may have changed, avoid waiting
                    break
                flush_request = self._get_and_unset_flush_request()
                if (
                    len(self.queue) < self.max_export_batch_size
                    and flush_request is None
                ):
                    self.condition.wait(timeout)
                    self.heartbeat()
                    flush_request = self._get_and_unset_flush_request()
                    if not self.queue:
                        # spurious notification, let's wait again, reset timeout
                        timeout = self.schedule_delay_millis / 1e3
                        self._notify_flush_request_finished(flush_request)
                        flush_request = None
                        continue
                    if self.done:
                        # missing spans will be sent when calling flush
                        break

            # subtract the duration of this export call to the next timeout
            start = time_ns()
            self._export(flush_request)
            end = time_ns()
            duration = (end - start) / 1e9
            timeout = self.schedule_delay_millis / 1e3 - duration

            self._notify_flush_request_finished(flush_request)
            flush_request = None

        # there might have been a new flush request while export was running
        # and before the done flag switched to true
        with self.condition:
            shutdown_flush_request = self._get_and_unset_flush_request()

        # be sure that all spans are sent
        self._drain_queue()
        self._notify_flush_request_finished(flush_request)
        self._notify_flush_request_finished(shutdown_flush_request)

    def _get_and_unset_flush_request(
        self,
    ) -> typing.Optional[_FlushRequest]:
        """Returns the current flush request and makes it invisible to the
        worker thread for subsequent calls.
        """
        flush_request = self._flush_request
        self._flush_request = None
        if flush_request is not None:
            flush_request.num_spans = len(self.queue)
        return flush_request

    @staticmethod
    def _notify_flush_request_finished(
        flush_request: typing.Optional[_FlushRequest],
    ):
        """Notifies the flush initiator(s) waiting on the given request/event
        that the flush operation was finished.
        """
        if flush_request is not None:
            flush_request.event.set()

    def _get_or_create_flush_request(self) -> _FlushRequest:
        """Either returns the current active flush event or creates a new one.
        The flush event will be visible and read by the worker thread before an
        export operation starts. Callers of a flush operation may wait on the
        returned event to be notified when the flush/export operation was
        finished.
        This method is not thread-safe, i.e. callers need to take care about
        synchronization/locking.
        """
        if self._flush_request is None:
            self._flush_request = _FlushRequest()
        return self._flush_request

    def _export(self, flush_request: typing.Optional[_FlushRequest]):
        """Exports spans considering the given flush_request.
        In case of a given flush_requests spans are exported in batches until
        the number of exported spans reached or exceeded the number of spans in
        the flush request.
        In no flush_request was given at most max_export_batch_size spans are
        exported.
        """
        if not flush_request:
            self._export_batch()
            return

        num_spans = flush_request.num_spans
        while self.queue:
            num_exported = self._export_batch()
            num_spans -= num_exported

            if num_spans <= 0:
                break

    def _export_batch(self) -> int:
        """Exports at most max_export_batch_size spans and returns the number of
        exported spans.
        """
        idx = 0
        # currently only a single thread acts as consumer, so queue.pop() will
        # not raise an exception
        while idx < self.max_export_batch_size and self.queue:
            self.spans_list[idx] = self.queue.pop()
            idx += 1
        token = attach(set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
        try:
            # Ignore type b/c the Optional[None]+slicing is too "clever"
            # for mypy
            self.span_exporter.export(self.spans_list[:idx])  # type: ignore
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Exception while exporting Span batch.")
        detach(token)

        # clean up list
        for index in range(idx):
            self.spans_list[index] = None
        return idx

    def _drain_queue(self):
        """Export all elements until queue is empty.
        Can only be called from the worker thread context because it invokes
        `export` that is not thread safe.
        """
        while self.queue:
            self._export_batch()

    def force_flush(self, timeout_millis: int = None) -> bool:
        if timeout_millis is None:
            timeout_millis = self.export_timeout_millis

        if self.done:
            logger.warning("Already shutdown, ignoring call to force_flush().")
            return True

        with self.condition:
            flush_request = self._get_or_create_flush_request()
            # signal the worker thread to flush and wait for it to finish
            self.condition.notify_all()

        # wait for token to be processed
        ret = flush_request.event.wait(timeout_millis / 1e3)
        if not ret:
            logger.warning("Timeout was exceeded in force_flush().")
        return ret

    def shutdown(self) -> None:
        # signal the worker thread to finish and then wait for it
        self.done = True
        with self.condition:
            self.condition.notify_all()
        self.worker_thread.join()
        self.span_exporter.shutdown()

    @staticmethod
    def _default_max_queue_size():
        try:
            return int(
                environ.get(OTEL_BSP_MAX_QUEUE_SIZE, _DEFAULT_MAX_QUEUE_SIZE)
            )
        except ValueError:
            logger.exception(
                _ENV_VAR_INT_VALUE_ERROR_MESSAGE,
                OTEL_BSP_MAX_QUEUE_SIZE,
                _DEFAULT_MAX_QUEUE_SIZE,
            )
            return _DEFAULT_MAX_QUEUE_SIZE

    @staticmethod
    def _default_schedule_delay_millis():
        try:
            return int(
                environ.get(
                    OTEL_BSP_SCHEDULE_DELAY, _DEFAULT_SCHEDULE_DELAY_MILLIS
                )
            )
        except ValueError:
            logger.exception(
                _ENV_VAR_INT_VALUE_ERROR_MESSAGE,
                OTEL_BSP_SCHEDULE_DELAY,
                _DEFAULT_SCHEDULE_DELAY_MILLIS,
            )
            return _DEFAULT_SCHEDULE_DELAY_MILLIS

    @staticmethod
    def _default_max_export_batch_size():
        try:
            return int(
                environ.get(
                    OTEL_BSP_MAX_EXPORT_BATCH_SIZE,
                    _DEFAULT_MAX_EXPORT_BATCH_SIZE,
                )
            )
        except ValueError:
            logger.exception(
                _ENV_VAR_INT_VALUE_ERROR_MESSAGE,
                OTEL_BSP_MAX_EXPORT_BATCH_SIZE,
                _DEFAULT_MAX_EXPORT_BATCH_SIZE,
            )
            return _DEFAULT_MAX_EXPORT_BATCH_SIZE

    @staticmethod
    def _default_export_timeout_millis():
        try:
            return int(
                environ.get(
                    OTEL_BSP_EXPORT_TIMEOUT, _DEFAULT_EXPORT_TIMEOUT_MILLIS
                )
            )
        except ValueError:
            logger.exception(
                _ENV_VAR_INT_VALUE_ERROR_MESSAGE,
                OTEL_BSP_EXPORT_TIMEOUT,
                _DEFAULT_EXPORT_TIMEOUT_MILLIS,
            )
            return _DEFAULT_EXPORT_TIMEOUT_MILLIS

    @staticmethod
    def _validate_arguments(
        max_queue_size, schedule_delay_millis, max_export_batch_size
    ):
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be a positive integer.")

        if schedule_delay_millis <= 0:
            raise ValueError("schedule_delay_millis must be positive.")

        if max_export_batch_size <= 0:
            raise ValueError(
                "max_export_batch_size must be a positive integer."
            )

        if max_export_batch_size > max_queue_size:
            raise ValueError(
                "max_export_batch_size must be less than or equal to max_queue_size."
            )


def get_logdata(span, attributes):
    span_context = Span.get_span_context(span)
    log_record = LogRecord(
        timestamp=time.time_ns(),
        observed_timestamp=time.time_ns(),
        trace_id=span_context.trace_id,
        span_id=span_context.span_id,
        trace_flags=TraceFlags().get_default(),
        severity_text="INFO",
        severity_number=SeverityNumber.INFO,
        body=span.to_json(),
        attributes=attributes,
    )
    log_data = LogData(log_record=log_record, instrumentation_scope=None)
    return log_data
