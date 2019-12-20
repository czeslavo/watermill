package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
)

const SpanContextKey = "opencensus_span_context"

func OpenCensusTracing(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) (producedMessages []*message.Message, err error) {
		var span *trace.Span
		ctx := msg.Context()

		parentSpanContext, ok := GetSpanContext(msg)
		if ok {
			ctx, span = trace.StartSpanWithRemoteParent(ctx, message.HandlerNameFromCtx(ctx), parentSpanContext)

			span.AddLink(trace.Link{
				TraceID:    parentSpanContext.TraceID,
				SpanID:     parentSpanContext.SpanID,
				Type:       trace.LinkTypeParent,
				Attributes: nil,
			})
		} else {
			ctx, span = trace.StartSpan(ctx, message.HandlerNameFromCtx(ctx))
		}

		defer func() {
			for _, producedMessage := range producedMessages {
				SetSpanContext(span.SpanContext(), producedMessage)
			}
		}()

		defer func() {
			if err == nil {
				span.SetStatus(trace.Status{
					Code:    trace.StatusCodeOK,
					Message: "OK",
				})
			} else {
				span.SetStatus(trace.Status{
					Code:    trace.StatusCodeUnknown,
					Message: err.Error(),
				})
			}
			span.End()
		}()

		msg.SetContext(ctx)
		return h(msg)
	}
}

func SetSpanContext(sc trace.SpanContext, msg *message.Message) {
	binarySc := string(propagation.Binary(sc))
	msg.Metadata.Set(SpanContextKey, binarySc)
}

func GetSpanContext(message *message.Message) (sc trace.SpanContext, ok bool) {
	binarySc := []byte(message.Metadata.Get(SpanContextKey))
	return propagation.FromBinary(binarySc)
}
