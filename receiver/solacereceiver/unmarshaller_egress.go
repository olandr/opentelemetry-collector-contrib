// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package solacereceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/solacereceiver"

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/solacereceiver/internal/metadata"
	egress_v1 "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/solacereceiver/internal/model/egress/v1"
)

type brokerTraceEgressUnmarshallerV1 struct {
	logger           *zap.Logger
	telemetryBuilder *metadata.TelemetryBuilder
	metricAttrs      attribute.Set // other Otel attributes (to add to the metrics)
}

// unmarshal implements tracesUnmarshaller.unmarshal
func (u *brokerTraceEgressUnmarshallerV1) unmarshal(message *inboundMessage) (ptrace.Traces, error) {
	spanData, err := u.unmarshalToSpanData(message)
	if err != nil {
		return ptrace.Traces{}, err
	}
	traces := ptrace.NewTraces()
	u.populateTraces(spanData, traces)
	return traces, nil
}

// unmarshalToSpanData will consume an solaceMessage and unmarshal it into a SpanData.
// Returns an error if one occurred.
func (*brokerTraceEgressUnmarshallerV1) unmarshalToSpanData(message *inboundMessage) (*egress_v1.SpanData, error) {
	data := message.GetData()
	if len(data) == 0 {
		return nil, errEmptyPayload
	}
	var spanData egress_v1.SpanData
	if err := proto.Unmarshal(data, &spanData); err != nil {
		return nil, err
	}
	return &spanData, nil
}

// populateTraces will create a new Span from the given traces and map the given SpanData to the span.
// This will set all required fields such as name version, trace and span ID, parent span ID (if applicable),
// timestamps, errors and states.
func (u *brokerTraceEgressUnmarshallerV1) populateTraces(spanData *egress_v1.SpanData, traces ptrace.Traces) {
	// Append new resource span and map any attributes
	resourceSpan := traces.ResourceSpans().AppendEmpty()
	u.mapResourceSpanAttributes(spanData, resourceSpan.Resource().Attributes())
	instrLibrarySpans := resourceSpan.ScopeSpans().AppendEmpty()
	for _, egressSpanData := range spanData.EgressSpans {
		u.mapEgressSpan(egressSpanData, instrLibrarySpans.Spans())
	}
}

func (*brokerTraceEgressUnmarshallerV1) mapResourceSpanAttributes(spanData *egress_v1.SpanData, attrMap pcommon.Map) {
	setResourceSpanAttributes(attrMap, spanData.RouterName, spanData.SolosVersion, spanData.MessageVpnName)
}

func (u *brokerTraceEgressUnmarshallerV1) mapEgressSpan(spanData *egress_v1.SpanData_EgressSpan, clientSpans ptrace.SpanSlice) {
	// at least a support Egress span is found
	if spanData.GetTypeData() != nil {
		clientSpan := clientSpans.AppendEmpty()
		u.mapEgressSpanCommon(spanData, clientSpan)

		// We only map for KNOWN span types. Current known list:[ SendSpan, DeleteSpan ]
		// we drop any other unknown/unsupported span types
		switch casted := spanData.TypeData.(type) {
		// map Egress Send span attributes
		case *egress_v1.SpanData_EgressSpan_SendSpan:
			u.mapSendSpan(spanData.GetSendSpan(), clientSpan)
		// map Egress Delete span attributes
		case *egress_v1.SpanData_EgressSpan_DeleteSpan:
			u.mapDeleteSpan(spanData.GetDeleteSpan(), clientSpan)
		default:
			// unknown span type, drop the span
			u.logger.Warn(fmt.Sprintf("Received egress span with unknown span type %T, is the collector out of date?", casted))
			u.telemetryBuilder.SolacereceiverDroppedEgressSpans.Add(context.Background(), 1, metric.WithAttributeSet(u.metricAttrs))
		}

		// map any transaction events found
		if transactionEvent := spanData.GetTransactionEvent(); transactionEvent != nil {
			u.mapTransactionEvent(transactionEvent, clientSpan.Events().AppendEmpty())
		}
	} else {
		// malformed/incomplete egress span received, drop the span
		u.logger.Warn("Received egress span with no span type, could be malformed egress span?")
		u.telemetryBuilder.SolacereceiverDroppedEgressSpans.Add(context.Background(), 1, metric.WithAttributeSet(u.metricAttrs))
	}
}

func (*brokerTraceEgressUnmarshallerV1) mapEgressSpanCommon(spanData *egress_v1.SpanData_EgressSpan, clientSpan ptrace.Span) {
	var traceID [16]byte
	copy(traceID[:16], spanData.TraceId)
	clientSpan.SetTraceID(traceID)
	var spanID [8]byte
	copy(spanID[:8], spanData.SpanId)
	clientSpan.SetSpanID(spanID)
	// conditional parent-span-id
	if len(spanData.ParentSpanId) == 8 {
		var parentSpanID [8]byte
		copy(parentSpanID[:8], spanData.ParentSpanId)
		clientSpan.SetParentSpanID(parentSpanID)
	}
	// timestamps
	clientSpan.SetStartTimestamp(pcommon.Timestamp(spanData.GetStartTimeUnixNano()))
	clientSpan.SetEndTimestamp(pcommon.Timestamp(spanData.GetEndTimeUnixNano()))
	// status
	if spanData.ErrorDescription != nil {
		clientSpan.Status().SetCode(ptrace.StatusCodeError)
		clientSpan.Status().SetMessage(*spanData.ErrorDescription)
	}
}

func (u *brokerTraceEgressUnmarshallerV1) mapSendSpan(sendSpan *egress_v1.SpanData_SendSpan, span ptrace.Span) {
	const (
		sourceNameKey = "messaging.source.name"
		sourceKindKey = "messaging.source.kind"
		replayedKey   = "messaging.solace.message_replayed"
		outcomeKey    = "messaging.solace.send.outcome"
	)
	const (
		sendSpanOperationName = "send"
		sendSpanOperationType = "publish"
		sendNameSuffix        = " send"
		unknownSendName       = "(unknown)"
		anonymousSendName     = "(anonymous)"
	)
	// hard coded to producer span
	span.SetKind(ptrace.SpanKindProducer)

	attributes := span.Attributes()
	attributes.PutStr(systemAttrKey, systemAttrValue)
	attributes.PutStr(operationNameAttrKey, sendSpanOperationName)
	attributes.PutStr(operationTypeAttrKey, sendSpanOperationType)
	attributes.PutStr(protocolAttrKey, sendSpan.Protocol)
	if sendSpan.ProtocolVersion != nil {
		attributes.PutStr(protocolVersionAttrKey, *sendSpan.ProtocolVersion)
	}
	// we don't fatal out when we don't have a valid kind, instead just log and increment stats
	var name string
	switch casted := sendSpan.Source.(type) {
	case *egress_v1.SpanData_SendSpan_TopicEndpointName:
		if isAnonymousTopicEndpoint(casted.TopicEndpointName) {
			name = anonymousSendName
		} else {
			name = casted.TopicEndpointName
		}
		attributes.PutStr(sourceNameKey, casted.TopicEndpointName)
		attributes.PutStr(sourceKindKey, topicEndpointKind)
	case *egress_v1.SpanData_SendSpan_QueueName:
		if isAnonymousQueue(casted.QueueName) {
			name = anonymousSendName
		} else {
			name = casted.QueueName
		}
		attributes.PutStr(sourceNameKey, casted.QueueName)
		attributes.PutStr(sourceKindKey, queueKind)
	default:
		u.logger.Warn(fmt.Sprintf("Unknown source type %T", casted))
		u.telemetryBuilder.SolacereceiverRecoverableUnmarshallingErrors.Add(context.Background(), 1, metric.WithAttributeSet(u.metricAttrs))
		name = unknownSendName
	}
	span.SetName(name + sendNameSuffix)

	attributes.PutStr(clientUsernameAttrKey, sendSpan.ConsumerClientUsername)
	attributes.PutStr(clientNameAttrKey, sendSpan.ConsumerClientName)
	attributes.PutBool(replayedKey, sendSpan.ReplayedMsg)

	// include the partition number, if available
	if sendSpan.PartitionNumber != nil {
		attributes.PutInt(partitionNumberKey, int64(*sendSpan.PartitionNumber))
	}

	var outcome string
	switch sendSpan.Outcome {
	case egress_v1.SpanData_SendSpan_ACCEPTED:
		outcome = "accepted"
	case egress_v1.SpanData_SendSpan_REJECTED:
		outcome = "rejected"
	case egress_v1.SpanData_SendSpan_RELEASED:
		outcome = "released"
	case egress_v1.SpanData_SendSpan_DELIVERY_FAILED:
		outcome = "delivery failed"
	case egress_v1.SpanData_SendSpan_FLOW_UNBOUND:
		outcome = "flow unbound"
	case egress_v1.SpanData_SendSpan_TRANSACTION_COMMIT:
		outcome = "transaction commit"
	case egress_v1.SpanData_SendSpan_TRANSACTION_COMMIT_FAILED:
		outcome = "transaction commit failed"
	case egress_v1.SpanData_SendSpan_TRANSACTION_ROLLBACK:
		outcome = "transaction rollback"
	}
	attributes.PutStr(outcomeKey, outcome)
}

func (u *brokerTraceEgressUnmarshallerV1) mapDeleteSpan(deleteSpan *egress_v1.SpanData_DeleteSpan, span ptrace.Span) {
	const (
		destinationNameKey       = "messaging.destination.name"
		deleteOperationReasonKey = "messaging.solace.operation.reason"
	)
	const (
		spanOperationName     = "delete"
		spanOperationType     = "delete"
		deleteNameSuffix      = " delete"
		unknownEndpointName   = "(unknown)"
		anonymousEndpointName = "(anonymous)"
	)
	// Delete Info reasons
	const (
		ttlExpired              = "ttl_expired"
		rejectedNack            = "rejected_nack"
		maxRedeliveriesExceeded = "max_redeliveries_exceeded"
		hopCountExceeded        = "hop_count_exceeded"
		ingressSelector         = "ingress_selector"
		adminAction             = "admin_action"
	)
	// hard coded to internal span
	span.SetKind(ptrace.SpanKindInternal)

	attributes := span.Attributes()
	attributes.PutStr(systemAttrKey, systemAttrValue)
	attributes.PutStr(operationNameAttrKey, spanOperationName)
	attributes.PutStr(operationTypeAttrKey, spanOperationType)

	// include the partition number, if available
	if deleteSpan.PartitionNumber != nil {
		attributes.PutInt(partitionNumberKey, int64(*deleteSpan.PartitionNumber))
	}

	// Don't fatal out when we don't have a valid Endpoint name, instead just log and increment stats
	var endpointName string
	switch casted := deleteSpan.EndpointName.(type) {
	case *egress_v1.SpanData_DeleteSpan_TopicEndpointName:
		if isAnonymousTopicEndpoint(casted.TopicEndpointName) {
			endpointName = anonymousEndpointName
		} else {
			endpointName = casted.TopicEndpointName
		}
		attributes.PutStr(destinationNameKey, casted.TopicEndpointName)
		attributes.PutStr(destinationTypeAttrKey, topicEndpointKind)
	case *egress_v1.SpanData_DeleteSpan_QueueName:
		if isAnonymousQueue(casted.QueueName) {
			endpointName = anonymousEndpointName
		} else {
			endpointName = casted.QueueName
		}
		attributes.PutStr(destinationNameKey, casted.QueueName)
		attributes.PutStr(destinationTypeAttrKey, queueKind)
	default:
		u.logger.Warn(fmt.Sprintf("Unknown endpoint type %T", casted))
		u.telemetryBuilder.SolacereceiverRecoverableUnmarshallingErrors.Add(context.Background(), 1, metric.WithAttributeSet(u.metricAttrs))
		endpointName = unknownEndpointName
	}
	span.SetName(endpointName + deleteNameSuffix)

	// do not fatal out when we don't have a valid delete reason name
	// instead just log and increment stats
	switch casted := deleteSpan.TypeInfo.(type) {
	// caused by expired ttl on message
	case *egress_v1.SpanData_DeleteSpan_TtlExpiredInfo:
		attributes.PutStr(deleteOperationReasonKey, ttlExpired)
	// caused by consumer N(ack)ing with Rejected outcome
	case *egress_v1.SpanData_DeleteSpan_RejectedOutcomeInfo:
		attributes.PutStr(deleteOperationReasonKey, rejectedNack)
	// caused by max redelivery reached/exceeded
	case *egress_v1.SpanData_DeleteSpan_MaxRedeliveriesInfo:
		attributes.PutStr(deleteOperationReasonKey, maxRedeliveriesExceeded)
	// caused by exceeded hop count
	case *egress_v1.SpanData_DeleteSpan_HopCountExceededInfo:
		attributes.PutStr(deleteOperationReasonKey, hopCountExceeded)
	// caused by destination unable to match any ingress selector rule
	case *egress_v1.SpanData_DeleteSpan_IngressSelectorInfo:
		attributes.PutStr(deleteOperationReasonKey, ingressSelector)
	// caused by admin action
	case *egress_v1.SpanData_DeleteSpan_AdminActionInfo:
		attributes.PutStr(deleteOperationReasonKey, adminAction)
		u.mapDeleteSpanAdminActionInfo(casted.AdminActionInfo, attributes)
	default:
		u.logger.Warn(fmt.Sprintf("Unknown delete reason info type %T", casted))
		u.telemetryBuilder.SolacereceiverRecoverableUnmarshallingErrors.Add(context.Background(), 1, metric.WithAttributeSet(u.metricAttrs))
	}
}

// mapDeleteSpanAdminActionInfo will map the delete admin action information
func (u *brokerTraceEgressUnmarshallerV1) mapDeleteSpanAdminActionInfo(adminActionInfo *egress_v1.SpanData_AdminActionInfo, attrMap pcommon.Map) {
	const (
		adminInterfaceKey        = "messaging.solace.admin.interface"
		adminCliTerminalNameKey  = "messaging.solace.admin.cli.terminal.name"
		adminCliSessionNumberKey = "messaging.solace.admin.cli.session_number"
		adminSempVersionKey      = "messaging.solace.admin.semp.version"
		endUserIDKey             = "enduser.id"
		clientAddressKey         = "client.address"
	)
	// Supported Admin Interface names
	const (
		semp        = "semp"
		cliSSH      = "cli_ssh"
		cliTerminal = "cli_terminal"
	)

	// the authenticated userId that performed the delete action
	attrMap.PutStr(endUserIDKey, adminActionInfo.Username)

	// Do not fatal out when there isn't a valid delete admin action session type, instead just log and increment stats
	switch casted := adminActionInfo.SessionInfo.(type) {
	// from Cli
	case *egress_v1.SpanData_AdminActionInfo_CliSessionInfo:
		// get cli local session information
		localCliSession := casted.CliSessionInfo.GetLocalSession()
		if localCliSession != nil {
			// set the admin interface name as "cli_terminal"
			attrMap.PutStr(adminInterfaceKey, cliTerminal)
			attrMap.PutStr(adminCliTerminalNameKey, localCliSession.TerminalName)
		}
		// session number for the cli connection that made the delete request
		attrMap.PutInt(adminCliSessionNumberKey, int64(casted.CliSessionInfo.SessionNumber))
		// get cli remote session information
		remoteCliSession := casted.CliSessionInfo.GetRemoteSession()
		if remoteCliSession != nil {
			// set the admin interface name as "cli_ssl"
			attrMap.PutStr(adminInterfaceKey, cliSSH)
			// the peer IP address
			cliPeerIPLen := len(remoteCliSession.PeerIp)
			if cliPeerIPLen == 4 || cliPeerIPLen == 16 {
				attrMap.PutStr(clientAddressKey, net.IP(remoteCliSession.PeerIp).String())
			} else {
				u.logger.Debug("Cli Peer IP not included", zap.Int("length", cliPeerIPLen))
			}
		}
	// from SEMP
	case *egress_v1.SpanData_AdminActionInfo_SempSessionInfo:
		// set the admin interface name as "semp"
		attrMap.PutStr(adminInterfaceKey, semp)
		attrMap.PutInt(adminSempVersionKey, int64(casted.SempSessionInfo.SempVersion))
		sempPeerIPLen := len(casted.SempSessionInfo.PeerIp)
		if sempPeerIPLen == 4 || sempPeerIPLen == 16 {
			attrMap.PutStr(clientAddressKey, net.IP(casted.SempSessionInfo.PeerIp).String())
		} else {
			u.logger.Debug("SEMP Peer IP not included", zap.Int("length", sempPeerIPLen))
		}
	default:
		u.logger.Warn(fmt.Sprintf("Unknown admin action info type %T", casted))
		u.telemetryBuilder.SolacereceiverRecoverableUnmarshallingErrors.Add(context.Background(), 1, metric.WithAttributeSet(u.metricAttrs))
	}
}

// maps a transaction event. We cannot reuse the code in receive unmarshaller since
// the protobuf model is different and the return type for things like type and initiator would not work in an interface
func (u *brokerTraceEgressUnmarshallerV1) mapTransactionEvent(transactionEvent *egress_v1.SpanData_TransactionEvent, clientEvent ptrace.SpanEvent) {
	// map the transaction type to a name
	var name string
	switch transactionEvent.GetType() {
	case egress_v1.SpanData_TransactionEvent_COMMIT:
		name = "commit"
	case egress_v1.SpanData_TransactionEvent_ROLLBACK:
		name = "rollback"
	case egress_v1.SpanData_TransactionEvent_END:
		name = "end"
	case egress_v1.SpanData_TransactionEvent_PREPARE:
		name = "prepare"
	case egress_v1.SpanData_TransactionEvent_SESSION_TIMEOUT:
		name = "session_timeout"
	case egress_v1.SpanData_TransactionEvent_ROLLBACK_ONLY:
		name = "rollback_only"
	default:
		// Set the name to the unknown transaction event type to ensure forward compat.
		name = fmt.Sprintf("Unknown Transaction Event (%s)", transactionEvent.GetType().String())
		u.logger.Warn(fmt.Sprintf("Received span with unknown transaction event %s", transactionEvent.GetType()))
		u.telemetryBuilder.SolacereceiverRecoverableUnmarshallingErrors.Add(context.Background(), 1, metric.WithAttributeSet(u.metricAttrs))
	}
	clientEvent.SetName(name)
	clientEvent.SetTimestamp(pcommon.Timestamp(transactionEvent.TimeUnixNano))
	// map initiator enums to expected initiator strings
	var initiator string
	switch transactionEvent.GetInitiator() {
	case egress_v1.SpanData_TransactionEvent_CLIENT:
		initiator = "client"
	case egress_v1.SpanData_TransactionEvent_ADMIN:
		initiator = "administrator"
	case egress_v1.SpanData_TransactionEvent_BROKER:
		initiator = "broker"
	default:
		initiator = fmt.Sprintf("Unknown Transaction Initiator (%s)", transactionEvent.GetInitiator().String())
		u.logger.Warn(fmt.Sprintf("Received span with unknown transaction initiator %s", transactionEvent.GetInitiator()))
		u.telemetryBuilder.SolacereceiverRecoverableUnmarshallingErrors.Add(context.Background(), 1, metric.WithAttributeSet(u.metricAttrs))
	}
	clientEvent.Attributes().PutStr(transactionInitiatorEventKey, initiator)
	// conditionally set the error description if one occurred, otherwise omit
	if transactionEvent.ErrorDescription != nil {
		clientEvent.Attributes().PutStr(transactionErrorMessageEventKey, transactionEvent.GetErrorDescription())
	}
	// map the transaction type/id
	transactionID := transactionEvent.GetTransactionId()
	switch casted := transactionID.(type) {
	case *egress_v1.SpanData_TransactionEvent_LocalId:
		clientEvent.Attributes().PutInt(transactionIDEventKey, int64(casted.LocalId.TransactionId))
		clientEvent.Attributes().PutStr(transactedSessionNameEventKey, casted.LocalId.SessionName)
		clientEvent.Attributes().PutInt(transactedSessionIDEventKey, int64(casted.LocalId.SessionId))
	case *egress_v1.SpanData_TransactionEvent_Xid_:
		// format xxxxxxxx-yyyyyyyy-zzzzzzzz where x is FormatID (hex rep of int32), y is BranchQualifier and z is GlobalID, hex encoded.
		xidString := fmt.Sprintf("%08x", casted.Xid.FormatId) + "-" +
			hex.EncodeToString(casted.Xid.BranchQualifier) + "-" + hex.EncodeToString(casted.Xid.GlobalId)
		clientEvent.Attributes().PutStr(transactionXIDEventKey, xidString)
	default:
		u.logger.Warn(fmt.Sprintf("Unknown transaction ID type %T", transactionID))
		u.telemetryBuilder.SolacereceiverRecoverableUnmarshallingErrors.Add(context.Background(), 1, metric.WithAttributeSet(u.metricAttrs))
	}
}

func isAnonymousQueue(name string) bool {
	// all anonymous queues start with the prefix #P2P/QTMP
	const anonymousQueuePrefix = "#P2P/QTMP"
	return strings.HasPrefix(name, anonymousQueuePrefix)
}

func isAnonymousTopicEndpoint(name string) bool {
	// all anonymous topic endpoints are made up of hex strings of length 32
	if len(name) != 32 {
		return false
	}
	for _, c := range []byte(name) { // []byte casting is more efficient in this loop
		// check if we are outside 0-9 AND outside a-f
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
