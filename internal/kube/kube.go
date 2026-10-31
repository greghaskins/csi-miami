package kube

import (
	"context"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// ErrNotFound is wrapped by the error GetPV returns when the API server
// answers that no PV has the name. It is the only error that means the PV is
// known to be absent; see GetPV for what counts as the API server answering.
var ErrNotFound = errors.New("kube: persistent volume not found")

// DefaultDeadline bounds one API request when NewClient is given no positive
// deadline.
const DefaultDeadline = 10 * time.Second

// Request rate of the client. client-go's defaults (5 and 10) would queue the
// deletes of a namespace full of volumes behind each other.
const (
	clientQPS   = 20
	clientBurst = 40
)

// PV is the part of a PersistentVolume the controller reads, and nothing
// else. A PV without a CSI source has an empty Driver.
type PV struct {
	Name             string
	Driver           string            // spec.csi.driver
	VolumeHandle     string            // spec.csi.volumeHandle
	VolumeAttributes map[string]string // spec.csi.volumeAttributes
	ReclaimPolicy    string            // spec.persistentVolumeReclaimPolicy: Delete, Retain or Recycle
}

// PVReader reads PersistentVolumes by name.
type PVReader interface {
	// GetPV returns the PV with the given name. If there is none, the error
	// wraps ErrNotFound. Any other error (an unreachable API server, a denied
	// request, an expired deadline, an answer that is not the API server's)
	// means the answer is unknown.
	GetPV(ctx context.Context, name string) (*PV, error)
}

var pvResource = schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}

// Client is a PVReader that asks the API server directly.
type Client struct {
	pvs      dynamic.NamespaceableResourceInterface
	deadline time.Duration
}

var _ PVReader = (*Client)(nil)

// RestConfig returns the connection settings for the API server: the
// kubeconfig file named by kubeconfig when there is one (for development, and
// it wins over everything else), else the pod's service account. It is an
// error to name no kubeconfig outside a cluster.
func RestConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("kube: reading the kubeconfig: %w", err)
		}
		return cfg, nil
	}
	cfg, err := rest.InClusterConfig()
	if err != nil {
		if errors.Is(err, rest.ErrNotInCluster) {
			return nil, errors.New("kube: not running in a cluster and no kubeconfig was given")
		}
		return nil, fmt.Errorf("kube: in-cluster configuration: %w", err)
	}
	return cfg, nil
}

// NewClient returns a Client for the API server at cfg. Every request runs
// under deadline; zero or negative means DefaultDeadline. cfg is not
// modified.
//
// The client is client-go's dynamic client, so that authentication, TLS, rate
// limiting and status decoding are client-go's, and only the handful of PV
// fields the controller needs are read, into PV, without linking the typed API
// packages into the binary.
func NewClient(cfg *rest.Config, deadline time.Duration) (*Client, error) {
	if cfg == nil {
		return nil, errors.New("kube: a rest config is required")
	}
	c := rest.CopyConfig(cfg)
	if c.QPS == 0 {
		c.QPS = clientQPS
	}
	if c.Burst == 0 {
		c.Burst = clientBurst
	}
	if c.UserAgent == "" {
		c.UserAgent = "csi-miami-controller"
	}
	dyn, err := dynamic.NewForConfig(c)
	if err != nil {
		return nil, fmt.Errorf("kube: %w", err)
	}
	if deadline <= 0 {
		deadline = DefaultDeadline
	}
	return &Client{pvs: dyn.Resource(pvResource), deadline: deadline}, nil
}

// GetPV implements PVReader with one GET under the client's deadline.
//
// A 404 means "no such PV" only when it is the API server's own answer about
// this PV: a Status object that says NotFound for the resource persistentvolumes
// and the name asked for. A 404 from anything else on the way (a load balancer
// or proxy answering an unknown path with a page) is not that answer, and
// neither is the API server's own 404 for a path that is not the PV's (a
// kubeconfig whose server has a path prefix the API server does not serve),
// which carries no details about a PV. Treating either as "no such PV" would
// turn every delete into a success that deletes nothing, so each is an ordinary
// error.
func (c *Client) GetPV(ctx context.Context, name string) (*PV, error) {
	if name == "" {
		return nil, errors.New("kube: a PV name is required")
	}
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()

	obj, err := c.pvs.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) && isPVNotFound(err, name) {
			return nil, fmt.Errorf("%w: %q", ErrNotFound, name)
		}
		return nil, &ReadError{PV: name, Reason: reasonOf(err), err: err}
	}
	return decodePV(obj)
}

// ReadError is the error GetPV returns when it cannot tell whether the PV
// exists. Its message names the PV and says in a few words why, and carries
// nothing else: client-go's own message includes the URL, and so the address
// of the API server, which does not belong in a log or a gRPC status. The
// full error is available through Unwrap.
type ReadError struct {
	PV     string
	Reason string
	err    error
}

func (e *ReadError) Error() string { return fmt.Sprintf("kube: reading PV %q: %s", e.PV, e.Reason) }
func (e *ReadError) Unwrap() error { return e.err }

// reasonOf describes err without the address of the server or the request.
func reasonOf(err error) string {
	var st apierrors.APIStatus
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "the request timed out"
	case errors.Is(err, context.Canceled):
		return "the request was cancelled"
	case errors.As(err, &st):
		status := st.Status()
		switch {
		case unexpectedResponse(status):
			return fmt.Sprintf("the answer (HTTP %d) was not the API server's", status.Code)
		case apierrors.IsNotFound(err):
			return fmt.Sprintf("the API server answered HTTP %d, but not about this PV (is the server address right?)", status.Code)
		case status.Reason != "":
			return fmt.Sprintf("the API server answered %s (HTTP %d)", status.Reason, status.Code)
		}
		return fmt.Sprintf("the API server answered HTTP %d", status.Code)
	default:
		return "the API server could not be reached"
	}
}

// unexpectedResponse reports whether status is client-go's stand-in for a
// response whose body was not a Status.
func unexpectedResponse(status metav1.Status) bool {
	if status.Details == nil {
		return false
	}
	for _, cause := range status.Details.Causes {
		if cause.Type == metav1.CauseTypeUnexpectedServerResponse {
			return true
		}
	}
	return false
}

// isPVNotFound reports whether err, a NotFound, is a Status the API server
// itself wrote about the PV called name: details naming the resource
// persistentvolumes and that name, and not client-go's stand-in for a response
// that was not a Status.
func isPVNotFound(err error, name string) bool {
	var st apierrors.APIStatus
	if !errors.As(err, &st) {
		return false
	}
	status := st.Status()
	d := status.Details
	return d != nil && d.Kind == "persistentvolumes" && d.Name == name && !unexpectedResponse(status)
}

// decodePV reads the fields of PV out of the object. A field of the wrong
// type is an error, never a zero value.
func decodePV(obj *unstructured.Unstructured) (*PV, error) {
	pv := &PV{Name: obj.GetName()}
	var err error
	if pv.ReclaimPolicy, _, err = unstructured.NestedString(obj.Object, "spec", "persistentVolumeReclaimPolicy"); err != nil {
		return nil, fmt.Errorf("kube: PV %q: %w", pv.Name, err)
	}
	if pv.Driver, _, err = unstructured.NestedString(obj.Object, "spec", "csi", "driver"); err != nil {
		return nil, fmt.Errorf("kube: PV %q: %w", pv.Name, err)
	}
	if pv.VolumeHandle, _, err = unstructured.NestedString(obj.Object, "spec", "csi", "volumeHandle"); err != nil {
		return nil, fmt.Errorf("kube: PV %q: %w", pv.Name, err)
	}
	if pv.VolumeAttributes, _, err = unstructured.NestedStringMap(obj.Object, "spec", "csi", "volumeAttributes"); err != nil {
		return nil, fmt.Errorf("kube: PV %q: %w", pv.Name, err)
	}
	return pv, nil
}
