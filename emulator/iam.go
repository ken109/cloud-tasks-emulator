package emulator

import (
	"context"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// IAM policy support mirrors the Cloud Pub/Sub emulator: policies are stored
// in memory per resource and never actually enforced, which is enough for
// client libraries that round-trip a policy during tests.

func (s *Server) GetIamPolicy(_ context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	resource := req.GetResource()
	if _, _, _, ok := parseQueueName(resource); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource %q", resource)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.queueLocked(resource); err != nil {
		return nil, err
	}
	if p, ok := s.policies[resource]; ok {
		return proto.Clone(p).(*iampb.Policy), nil
	}
	return &iampb.Policy{}, nil
}

func (s *Server) SetIamPolicy(_ context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	resource := req.GetResource()
	if _, _, _, ok := parseQueueName(resource); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource %q", resource)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.queueLocked(resource); err != nil {
		return nil, err
	}
	policy := req.GetPolicy()
	if policy == nil {
		policy = &iampb.Policy{}
	} else {
		policy = proto.Clone(policy).(*iampb.Policy)
	}
	s.policies[resource] = policy
	return proto.Clone(policy).(*iampb.Policy), nil
}

func (s *Server) TestIamPermissions(_ context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	resource := req.GetResource()
	if _, _, _, ok := parseQueueName(resource); !ok {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource %q", resource)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.queueLocked(resource); err != nil {
		return nil, err
	}
	// Like the Pub/Sub emulator, grant every requested permission.
	return &iampb.TestIamPermissionsResponse{Permissions: req.GetPermissions()}, nil
}
