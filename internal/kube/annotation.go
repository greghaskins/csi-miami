package kube

import "context"

// PVAnnotation returns the value of one annotation of the PV called pv, "" when
// it has none. It is what the node's generation timers read for
// reconcile.AnnotationPublish, and needs only get on persistentvolumes, which
// the node's role already has. An error (the PV is gone, the API server cannot
// be reached) is not an empty value: the caller must not read it as the
// annotation having been removed.
func (c *Client) PVAnnotation(ctx context.Context, pv, key string) (string, error) {
	p, err := c.GetPV(ctx, pv)
	if err != nil {
		return "", err
	}
	return p.Annotations[key], nil
}
