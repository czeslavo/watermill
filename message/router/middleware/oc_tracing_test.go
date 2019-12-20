package middleware_test

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/trace"
)

func TestOpenCensusTracingMiddleware(t *testing.T) {
	tracingMiddleware := middleware.OpenCensusTracing(handlerFuncAlwaysOK)

	msg := message.NewMessage(watermill.NewULID(), nil)
	ctx, parentSpan := trace.StartSpan(context.Background(), "parent")
	msg.SetContext(ctx)

	producedMsgs, err := tracingMiddleware(msg)
	require.NoError(t, err)
	require.NotEmpty(t, producedMsgs)

	for _, producedMsg := range producedMsgs {
		spanContext, ok := middleware.GetSpanContext(producedMsg)
		require.True(t, ok)
		require.Equal(t, parentSpan.SpanContext().TraceID, spanContext.TraceID, "should have same trace id as the parent's one")
		require.NotEqual(t, parentSpan.SpanContext().SpanID, spanContext.SpanID, "should have span ID different than parent")
	}
}
