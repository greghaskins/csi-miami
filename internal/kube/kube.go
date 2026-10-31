package kube

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// ErrNotFound is wrapped by the error GetPV returns when the API server
// answers that no PV has the name. It is the only error that means the PV is
// known to be absent.
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

// PVReader reads PersistentVolumes by name.
type PVReader interface {
	// GetPV returns the PV with the given name. If there is none, the error
	// wraps ErrNotFound. Any other error (an unreachable API server, a denied
	// request, an expired deadline) means the answer is unknown.
	GetPV(ctx context.Context, name string) (*corev1.PersistentVolume, error)
}

// Client is a PVReader that asks the API server directly.
type Client struct {
	pvs      corev1client.PersistentVolumeInterface
	deadline time.Duration
}

var _ PVReader = (*Client)(nil)

// RestConfig returns the connection settings for the API server: the pod's
// service account when running in a cluster, else the kubeconfig file named
// by kubeconfig (for development). It is an error to be outside a cluster
// with no kubeconfig.
func RestConfig(kubeconfig string) (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}
	if !errors.Is(err, rest.ErrNotInCluster) {
		return nil, fmt.Errorf("kube: in-cluster configuration: %w", err)
	}
	if kubeconfig == "" {
		return nil, errors.New("kube: not running in a cluster and no kubeconfig was given")
	}
	cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("kube: reading the kubeconfig: %w", err)
	}
	return cfg, nil
}

// NewClient returns a Client for the API server at cfg. Every request runs
// under deadline; zero or negative means DefaultDeadline. cfg is not
// modified.
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
	core, err := corev1client.NewForConfig(c)
	if err != nil {
		return nil, fmt.Errorf("kube: %w", err)
	}
	if deadline <= 0 {
		deadline = DefaultDeadline
	}
	return &Client{pvs: core.PersistentVolumes(), deadline: deadline}, nil
}

// GetPV implements PVReader with one GET under the client's deadline.
func (c *Client) GetPV(ctx context.Context, name string) (*corev1.PersistentVolume, error) {
	if name == "" {
		return nil, errors.New("kube: a PV name is required")
	}
	ctx, cancel := context.WithTimeout(ctx, c.deadline)
	defer cancel()

	pv, err := c.pvs.Get(ctx, name, metav1.GetOptions{})
	switch {
	case err == nil:
		return pv, nil
	case apierrors.IsNotFound(err):
		return nil, fmt.Errorf("%w: %q", ErrNotFound, name)
	default:
		return nil, fmt.Errorf("kube: reading PV %q: %w", name, err)
	}
}
