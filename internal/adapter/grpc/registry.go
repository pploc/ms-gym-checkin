package grpc

import (
	"github.com/pploc/common-go/auth"
	"github.com/pploc/common-go/grpc/middleware"
	checkinv1 "github.com/pploc/proto-go/checkin/v1"
)

func MethodRules() []middleware.MethodRule {
	return []middleware.MethodRule{
		{Method: checkinv1.CheckInService_ProcessScan_FullMethodName, Kind: middleware.MethodRoleRestricted, Roles: []auth.Role{auth.RoleCustomer}},
		{Method: checkinv1.CheckInService_GetMyCheckInHistory_FullMethodName, Kind: middleware.MethodRoleRestricted, Roles: []auth.Role{auth.RoleCustomer}},
		{Method: checkinv1.CheckInService_GetMemberCheckInHistory_FullMethodName, Kind: middleware.MethodRoleRestricted, Roles: []auth.Role{auth.RoleSuperAdmin}},
		{Method: checkinv1.CheckInService_GetDailyCount_FullMethodName, Kind: middleware.MethodRoleRestricted, Roles: []auth.Role{auth.RoleSuperAdmin}},
		{Method: checkinv1.CheckInService_GetDisplayQrPayload_FullMethodName, Kind: middleware.MethodRoleRestricted, Roles: []auth.Role{auth.RoleSuperAdmin}},
		{Method: checkinv1.CheckInService_RotateGymQrRootKey_FullMethodName, Kind: middleware.MethodRoleRestricted, Roles: []auth.Role{auth.RoleSuperAdmin}},
	}
}

func PublicMethods() []string { return nil }
