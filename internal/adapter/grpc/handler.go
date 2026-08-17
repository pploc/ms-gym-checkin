package grpc

import (
	"context"

	"github.com/pploc/common-go/auth"
	commonerrors "github.com/pploc/common-go/errors"
	"github.com/pploc/ms-gym-checkin/internal/domain"
	"github.com/pploc/ms-gym-checkin/internal/usecase"
	checkinv1 "github.com/pploc/proto-go/checkin/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Handler struct {
	checkinv1.UnimplementedCheckInServiceServer
	service *usecase.Service
}

func NewHandler(service *usecase.Service) *Handler { return &Handler{service: service} }
func (h *Handler) ProcessScan(ctx context.Context, request *checkinv1.ProcessScanRequest) (*checkinv1.ProcessScanResponse, error) {
	claims, err := claims(ctx)
	if err != nil {
		return nil, err
	}
	record, err := h.service.Scan(ctx, claims.UserID, request.GetGymId(), request.GetQrPayload(), request.GetIdempotencyKey())
	if err != nil {
		return nil, err
	}
	return &checkinv1.ProcessScanResponse{Success: true, Message: "Check-in recorded", Record: recordResponse(record)}, nil
}
func (h *Handler) GetMyCheckInHistory(ctx context.Context, request *checkinv1.GetMyCheckInHistoryRequest) (*checkinv1.GetMyCheckInHistoryResponse, error) {
	claims, err := claims(ctx)
	if err != nil {
		return nil, err
	}
	records, total, err := h.service.MyHistory(ctx, claims.UserID, int(request.GetPage()), int(request.GetLimit()))
	if err != nil {
		return nil, err
	}
	response := &checkinv1.GetMyCheckInHistoryResponse{Total: int32(total)}
	for _, record := range records {
		response.Records = append(response.Records, recordResponse(record))
	}
	return response, nil
}
func (h *Handler) GetMemberCheckInHistory(ctx context.Context, request *checkinv1.GetMemberCheckInHistoryRequest) (*checkinv1.GetMemberCheckInHistoryResponse, error) {
	records, total, err := h.service.MemberHistory(ctx, request.GetMemberId(), int(request.GetPage()), int(request.GetLimit()))
	if err != nil {
		return nil, err
	}
	response := &checkinv1.GetMemberCheckInHistoryResponse{Total: int32(total)}
	for _, record := range records {
		response.Records = append(response.Records, recordResponse(record))
	}
	return response, nil
}
func (h *Handler) GetDailyCount(ctx context.Context, request *checkinv1.GetDailyCountRequest) (*checkinv1.GetDailyCountResponse, error) {
	count, err := h.service.DailyCount(ctx, request.GetGymId(), request.GetDate())
	if err != nil {
		return nil, err
	}
	return &checkinv1.GetDailyCountResponse{GymId: request.GetGymId(), Date: request.GetDate(), Count: int32(count)}, nil
}
func (h *Handler) GetDisplayQrPayload(ctx context.Context, request *checkinv1.GetDisplayQrPayloadRequest) (*checkinv1.GetDisplayQrPayloadResponse, error) {
	payload, err := h.service.Display(ctx, request.GetGymId())
	if err != nil {
		return nil, err
	}
	return &checkinv1.GetDisplayQrPayloadResponse{GymId: payload.GymID, Current: &checkinv1.SignedQrPayload{QrPayload: payload.Current, ActiveAt: timestamppb.New(payload.CurrentActiveAt), ExpiresAt: timestamppb.New(payload.CurrentExpiresAt)}, Next: &checkinv1.SignedQrPayload{QrPayload: payload.Next, ActiveAt: timestamppb.New(payload.NextActiveAt), ExpiresAt: timestamppb.New(payload.NextExpiresAt)}, SlotDurationSeconds: payload.SlotDurationSeconds}, nil
}
func (h *Handler) RotateGymQrRootKey(ctx context.Context, request *checkinv1.RotateGymQrRootKeyRequest) (*checkinv1.RotateGymQrRootKeyResponse, error) {
	rotation, err := h.service.Rotate(ctx, request.GetGymId(), request.GetEmergency())
	if err != nil {
		return nil, err
	}
	return &checkinv1.RotateGymQrRootKeyResponse{GymId: rotation.GymID, KeyVersion: rotation.KeyVersion, ActivatedAt: timestamppb.New(rotation.ActivatedAt)}, nil
}
func claims(ctx context.Context) (auth.Claims, error) {
	claims, ok := auth.FromContext(ctx)
	if !ok {
		return auth.Claims{}, commonerrors.New(commonerrors.CategoryUnauthorized, "AUTHENTICATION_REQUIRED", "authentication is required")
	}
	return claims, nil
}
func recordResponse(record domain.CheckInRecord) *checkinv1.CheckInRecord {
	return &checkinv1.CheckInRecord{Id: record.ID, MemberId: record.MemberID, GymId: record.GymID, CheckedInAt: timestamppb.New(record.CheckedInAt)}
}
