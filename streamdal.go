package amqp091

import (
	"context"
	"errors"
	"os"

	streamdal "github.com/streamdal/streamdal/sdks/go"
)

const (
	StreamdalEnvAddress     = "STREAMDAL_ADDRESS"
	StreamdalEnvAuthToken   = "STREAMDAL_AUTH_TOKEN"
	StreamdalEnvServiceName = "STREAMDAL_SERVICE_NAME"

	StreamdalDefaultComponentName = "rabbitmq"
	StreamdalDefaultOperationName = "consume"
)

// StreamdalRuntimeConfig is an optional configuration structure that can be
// passed to channel.PublishWithDeferredConfirmWithContext() to influence
// streamdal shim behavior.
type StreamdalRuntimeConfig struct {
	// StrictErrors will cause the shim to return an error if Streamdal.Process()
	// runs into an unrecoverable error. Default: swallow error and return original value.
	StrictErrors bool

	// Audience is used to specify a custom audience when the shim calls on
	// streamdal.Process(); if nil, a default ComponentName and OperationName
	// will be used. Only non-blank values will be used to override audience defaults.
	Audience *streamdal.Audience
}

func streamdalSetup() (*streamdal.Streamdal, error) {
	address := os.Getenv(StreamdalEnvAddress)
	if address == "" {
		return nil, errors.New(StreamdalEnvAddress + " env var is not set")
	}

	authToken := os.Getenv(StreamdalEnvAuthToken)
	if authToken == "" {
		return nil, errors.New(StreamdalEnvAuthToken + " env var is not set")
	}

	serviceName := os.Getenv(StreamdalEnvServiceName)
	if serviceName == "" {
		return nil, errors.New(StreamdalEnvServiceName + " env var is not set")
	}

	sc, err := streamdal.New(&streamdal.Config{
		ServerURL:   address,
		ServerToken: authToken,
		ServiceName: serviceName,
		ClientType:  streamdal.ClientTypeShim,
	})

	if err != nil {
		return nil, errors.New("unable to create streamdal client: " + err.Error())
	}

	return sc, nil
}

func streamdalProcessConsume(ctx context.Context, sc *streamdal.Streamdal, msg *Delivery, sdCfg ...StreamdalRuntimeConfig) (*Delivery, error) {
	// Nothing to do if streamdal client is nil
	if sc == nil {
		return msg, nil
	}

	// Inject runtime config if provided
	var src *StreamdalRuntimeConfig
	if len(sdCfg) > 0 {
		src = &sdCfg[0]
	}

	// Generate an audience from the provided parameters
	aud := streamdalGenerateAudience(streamdal.OperationTypeConsumer, msg.Exchange, msg.RoutingKey, src)

	// Process msg payload via Streamdal
	resp := sc.Process(ctx, &streamdal.ProcessRequest{
		ComponentName: aud.ComponentName,
		OperationType: streamdal.OperationTypeConsumer,
		OperationName: aud.OperationName,
		Data:          msg.Body,
	})

	switch resp.Status {
	case streamdal.ExecStatusTrue, streamdal.ExecStatusFalse:
		// Process() did not error - replace msg.Body
		msg.Body = resp.Data
	case streamdal.ExecStatusError:
		// Process() errored - return message as-is; if strict errors are NOT
		// set, return error instead of message
		Logger.Printf("streamdal.Process() error: %s", ptrStr(resp.StatusMessage))

		if src != nil && src.StrictErrors {
			return nil, errors.New("streamdal.Process() error: " + ptrStr(resp.StatusMessage))
		}
	}

	return msg, nil
}

func streamdalProcessProduce(ctx context.Context, sc *streamdal.Streamdal, exchangeName, routingKey string, msg *Publishing, sdCfg ...StreamdalRuntimeConfig) (*Publishing, error) {
	// Nothing to do if streamdal client is nil
	if sc == nil {
		return msg, nil
	}

	// Inject runtime config if provided
	var src *StreamdalRuntimeConfig
	if len(sdCfg) > 0 {
		src = &sdCfg[0]
	}

	// Generate an audience from the provided parameters
	aud := streamdalGenerateAudience(streamdal.OperationTypeProducer, exchangeName, routingKey, src)

	// Process msg payload via Streamdal
	resp := sc.Process(ctx, &streamdal.ProcessRequest{
		ComponentName: aud.ComponentName,
		OperationType: streamdal.OperationTypeProducer,
		OperationName: aud.OperationName,
		Data:          msg.Body,
	})

	switch resp.Status {
	case streamdal.ExecStatusTrue, streamdal.ExecStatusFalse:
		// Process() did not error - replace msg.Body
		msg.Body = resp.Data
	case streamdal.ExecStatusError:
		// Process() errored - return message as-is; if strict errors are NOT
		// set, return error instead of message
		Logger.Printf("streamdal.Process() error: %s", ptrStr(resp.StatusMessage))

		if src != nil && src.StrictErrors {
			return nil, errors.New("streamdal.Process() error: " + ptrStr(resp.StatusMessage))
		}
	}

	return msg, nil
}

// Helper func for generating an "audience" that can be passed to streamdal's .Process() method.
//
// Topic is only used if the provided runtime config is nil or the underlying
// audience does not have an OperationName set.
func streamdalGenerateAudience(ot streamdal.OperationType, exchangeName, routingKey string, src *StreamdalRuntimeConfig) *streamdal.Audience {
	var (
		componentName = StreamdalDefaultComponentName
		operationName = StreamdalDefaultOperationName
	)

	if exchangeName != "" {
		operationName = exchangeName
	}

	if routingKey != "" {
		operationName += "-" + routingKey
	}

	if src != nil && src.Audience != nil {
		if src.Audience.OperationName != "" {
			operationName = src.Audience.OperationName
		}

		if src.Audience.ComponentName != "" {
			componentName = src.Audience.ComponentName
		}
	}

	return &streamdal.Audience{
		OperationType: ot,
		OperationName: operationName,
		ComponentName: componentName,
	}
}

// Helper func for logging errors encountered during streamdal.Process()
func streamdalLogError(loggers []Logging, msg string) {
	for _, l := range loggers {
		if l == nil {
			continue
		}

		l.Printf(msg)
	}
}

// Helper func to deref string ptrs
func ptrStr(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}
