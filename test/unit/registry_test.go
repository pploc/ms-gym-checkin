package unit

import (
	"testing"

	"github.com/pploc/common-go/auth"
	"github.com/pploc/common-go/grpc/middleware"
	checkinv1 "github.com/pploc/proto-go/checkin/v1"

	grpcadapter "github.com/pploc/ms-gym-checkin/internal/adapter/grpc"
)

func TestGivenCheckInContract_WhenBuildingMethodRules_ThenEveryPublicMethodHasExactRole(t *testing.T) {
	// Given
	rules := grpcadapter.MethodRules()
	want := map[string]auth.Role{
		checkinv1.CheckInService_ProcessScan_FullMethodName:             auth.RoleCustomer,
		checkinv1.CheckInService_GetMyCheckInHistory_FullMethodName:     auth.RoleCustomer,
		checkinv1.CheckInService_GetMemberCheckInHistory_FullMethodName: auth.RoleSuperAdmin,
		checkinv1.CheckInService_GetDailyCount_FullMethodName:           auth.RoleSuperAdmin,
		checkinv1.CheckInService_GetDisplayQrPayload_FullMethodName:     auth.RoleSuperAdmin,
		checkinv1.CheckInService_RotateGymQrRootKey_FullMethodName:      auth.RoleSuperAdmin,
	}

	// When
	got := make(map[string]middleware.MethodRule, len(rules))
	for _, rule := range rules {
		got[rule.Method] = rule
	}

	// Then
	if len(got) != len(want) || len(grpcadapter.PublicMethods()) != 0 {
		t.Fatal("unexpected Check-in method registry")
	}
	for method, role := range want {
		rule, ok := got[method]
		if !ok || rule.Kind != middleware.MethodRoleRestricted || len(rule.Roles) != 1 || rule.Roles[0] != role {
			t.Fatal("Check-in method role is incorrect")
		}
	}
}
