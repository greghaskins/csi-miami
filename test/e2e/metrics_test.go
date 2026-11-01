//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/greghaskins/csi-miami/internal/metrics"
)

// Exit criterion 6: /metrics of the node plugin, through the API server's pod
// proxy, has every family the plan lists (the names come from
// internal/metrics); miami_intents{kind="directory"} equals the number of
// volumes staged on the node at that moment (counted from the node's intent records); the transport gauge reports
// the transport mounted; and the request
// counters show the methods the test drove. The controller's endpoint shows
// CreateVolume and DeleteVolume.
func TestMetrics(t *testing.T) {
	e := loadEnv(t)
	for _, variant := range []string{transportLocal, transportNFS3} {
		runVariant(t, e, variant, func(t *testing.T) { metricsCriterion(t, e) })
	}
}

func metricsCriterion(t *testing.T, e *env) {
	node := e.workers(t)[0]
	n := e.newNamespace(t)

	claims := []string{"m1", "m2"}
	vols := map[string]volumeInfo{}
	pods := map[string]types.UID{}
	for _, c := range claims {
		n.createPVC(t, c)
		n.createPod(t, podSpec{Name: c, Claim: c, Node: node, Run: stay("echo " + c + " > /data/f")})
	}
	for _, c := range claims {
		pods[c] = n.waitPodReady(t, c, "").UID
		vols[c] = n.volumeOf(t, c)
		e.mustVolumeMounts(t, node, vols[c], pods[c])
	}
	removeLocal := func() {
		if e.Transport == transportLocal {
			for _, v := range vols {
				removeNodeDir(node, v.itemDir())
			}
		}
	}
	t.Cleanup(removeLocal)

	ctx, cancel := e.withDeadline()
	plugin, err := e.nodePluginPod(ctx, node)
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	scrapeNode := func(ctx context.Context) (metricSet, error) { return e.scrape(ctx, plugin.Name, nodePort) }

	// Every family, the intents gauge against the node's mount table, the
	// transport gauge, and the counters of the methods driven.
	e.waitFor(t, fmt.Sprintf("%s on %s to show every family, %d intents (as many as staged volumes), the transport mounted, and the stage and publish counters", plugin.Name, node, len(claims)), func(ctx context.Context) (string, bool, error) {
		ms, err := scrapeNode(ctx)
		if err != nil {
			return "", false, err
		}
		staged, err := nodeIntentCount(ctx, node)
		if err != nil {
			return "", false, err
		}
		intents, have := ms.value(metrics.NameIntents, map[string]string{"kind": "directory"})
		missing := ms.missingFamilies(metrics.NodeFamilies)
		transport := len(vols) > 0
		var gauges []string
		for _, v := range vols {
			g, ok := ms.value(metrics.NameTransports, map[string]string{"kind": e.Transport, "id": v.Context.Transport.ID()})
			gauges = append(gauges, fmt.Sprintf("%v/%v", g, ok))
			transport = transport && ok && g == 1
		}
		stageOK := ms.requests(metrics.NameCSIRequests, "NodeStageVolume", "OK")
		pubOK := ms.requests(metrics.NameCSIRequests, "NodePublishVolume", "OK")
		seen := fmt.Sprintf("%s; missing families %v; intents %v (present %v) with %d records on the node; transport gauges %v; NodeStageVolume OK %v, NodePublishVolume OK %v",
			ms, missing, intents, have, staged, gauges, stageOK, pubOK)
		done := len(missing) == 0 && have && int(intents) == staged && staged == len(claims) && transport &&
			stageOK >= float64(len(claims)) && pubOK >= float64(len(claims))
		return seen, done, nil
	})
	t.Logf("the node plugin on %s serves every family: %v", node, metrics.NodeFamilies)

	// Delete the pods: unpublish and unstage are counted and the gauge falls.
	for _, c := range claims {
		n.deletePod(t, c)
	}
	e.waitFor(t, fmt.Sprintf("%s to show 0 intents (as many as staged volumes) and the unpublish and unstage counters", plugin.Name), func(ctx context.Context) (string, bool, error) {
		ms, err := scrapeNode(ctx)
		if err != nil {
			return "", false, err
		}
		staged, err := nodeIntentCount(ctx, node)
		if err != nil {
			return "", false, err
		}
		intents, have := ms.value(metrics.NameIntents, map[string]string{"kind": "directory"})
		unpub := ms.requests(metrics.NameCSIRequests, "NodeUnpublishVolume", "OK")
		unstage := ms.requests(metrics.NameCSIRequests, "NodeUnstageVolume", "OK")
		seen := fmt.Sprintf("intents %v (present %v) with %d records on the node; NodeUnpublishVolume OK %v, NodeUnstageVolume OK %v", intents, have, staged, unpub, unstage)
		done := have && int(intents) == staged && staged == 0 &&
			unpub >= float64(len(claims)) && unstage >= float64(len(claims))
		return seen, done, nil
	})

	// The controller: CreateVolume now, DeleteVolume once the claims go.
	for _, c := range claims {
		n.deletePVC(t, c, vols[c].PV)
	}
	e.waitFor(t, "the controller to show CreateVolume and DeleteVolume counters and only the CSI request families", func(ctx context.Context) (string, bool, error) {
		ctrl, err := e.controllerPod(ctx)
		if err != nil {
			return "", false, err
		}
		ms, err := e.scrape(ctx, ctrl.Name, controllerPort)
		if err != nil {
			return "", false, err
		}
		create := ms.requests(metrics.NameCSIRequests, "CreateVolume", "OK")
		del := ms.requests(metrics.NameCSIRequests, "DeleteVolume", "OK")
		missing := ms.missingFamilies(metrics.ControllerFamilies)
		_, hasIntents := ms.value(metrics.NameIntents, map[string]string{"kind": "directory"})
		seen := fmt.Sprintf("%s on %s; missing families %v; node-only intents series present %v; CreateVolume OK %v, DeleteVolume OK %v", ms, ctrl.Name, missing, hasIntents, create, del)
		return seen, len(missing) == 0 && !hasIntents && create >= float64(len(claims)) && del >= float64(len(claims)), nil
	})
}
