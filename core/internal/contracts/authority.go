package contracts

// AdmissionOrigin is private server provenance. It is carried beside, never
// inside, user supplied Call or Event wire data.
type AdmissionOrigin struct {
	Root          Ref    `json:"root"`
	Actor         Ref    `json:"actor"`
	Kind          string `json:"kind"`
	ParentCallID  string `json:"parent_call_id,omitempty"`
	ParentEventID string `json:"parent_event_id,omitempty"`
}

const (
	OriginDirect       = "direct"
	OriginSubscription = "subscription"
	OriginConversation = "conversation"
	OriginSource       = "source"
	OriginResult       = "result"
)

type Authority interface {
	Authorize(origin AdmissionOrigin, action string, target Ref, operation string) error
	ShutdownCapability(owner any) HostShutdownCapability
}

// HostShutdownCapability is issued by the authority composition and kept only
// by its invocation service. It can authorize local cleanup of its own Calls.
type HostShutdownCapability interface {
	AuthorizeAbort(owner any, target Ref, operation string) error
}

func ValidAdmissionOrigin(origin AdmissionOrigin) bool {
	if !origin.Root.Valid() || !origin.Actor.Valid() ||
		(origin.Root.Namespace != "operators" && origin.Root.Namespace != "nodes") ||
		(origin.Actor.Namespace != "operators" && origin.Actor.Namespace != "nodes") ||
		origin.Root.Namespace == "operators" && origin.Root.ID != "local" ||
		origin.Actor.Namespace == "operators" && origin.Actor.ID != "local" {
		return false
	}
	switch origin.Kind {
	case OriginDirect:
		return origin.Root == (Ref{Namespace: "operators", ID: "local"}) && origin.Actor == origin.Root && origin.ParentCallID == "" && origin.ParentEventID == ""
	case OriginConversation:
		return origin.Root == (Ref{Namespace: "operators", ID: "local"}) && origin.Actor == origin.Root && origin.ParentCallID == "" && ValidID(origin.ParentEventID)
	case OriginSource:
		return origin.Root.Namespace == "nodes" && origin.Actor == origin.Root && origin.ParentCallID == "" && origin.ParentEventID == ""
	case OriginSubscription:
		return origin.ParentCallID == "" && ValidID(origin.ParentEventID)
	case OriginResult:
		return origin.Actor.Namespace == "nodes" && ValidID(origin.ParentCallID) && origin.ParentEventID == ""
	default:
		return false
	}
}
