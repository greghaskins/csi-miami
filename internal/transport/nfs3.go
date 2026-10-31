package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/observe"
)

// nfs3FSType is the file system type of the mount: the kernel client, with no
// helper binary.
const nfs3FSType = "nfs"

// nfs3Flags are the generic mount flags of the profile. noatime is a flag of
// the mount, not an NFS data option.
const nfs3Flags = unix.MS_NOATIME

// nfs3Profile is the one option set nfs3 mounts with. It is code, not
// configuration, and changing it is a code change with a version bump.
//
//	vers=3,proto=tcp      the protocol the target servers speak; tcp so that a
//	                      socket reset is a real fence
//	softerr,timeo=100,retrans=3
//	                      bounded failure: about 100s major timeout, and
//	                      ETIMEDOUT instead of EIO (needs Linux 5.3)
//	nolock                locks are node-local; no NLM, no lockd state to hang
//	lookupcache=positive  never cache a negative lookup
//	actimeo=30            attribute cache of 30s; close-to-open still holds
//
// There is no hard, and no fallback to soft. addr= is added by nfs3Data.
const nfs3Profile = "vers=3,proto=tcp,softerr,timeo=100,retrans=3,nolock,lookupcache=positive,actimeo=30"

// nfs3Data renders the data string of mount(2): the profile and the address
// of the server, resolved by the driver so that no helper is needed.
func nfs3Data(addr string) string { return nfs3Profile + ",addr=" + addr }

// nfs3SuperOptions are the tokens that /proc/self/mountinfo must show among a
// mount's super options for it to be this driver's mount. The kernel reports
// the profile in its own words, not the ones it was given (nolock appears as
// local_lock=all, actimeo=30 as the four ac* options, those left at their
// default left out), so these were taken from a real mount, not from the
// profile. addr is not among them: the name may resolve differently later,
// and the source string is what names the server.
var nfs3SuperOptions = []string{
	"rw",
	"vers=3",
	"proto=tcp",
	"softerr",
	"timeo=100",
	"retrans=3",
	"local_lock=all",
	"acregmin=30",
	"acregmax=30",
	"acdirmax=30",
}

// nfs3MountOptions are the tokens of the mount's own options (not the super
// block's) that the profile sets.
var nfs3MountOptions = []string{"noatime"}

// nfs3 is an NFSv3 export mounted with mount(2) at Dir.
type nfs3 struct {
	base
	source string // <server>:<export>, as this driver passes it
}

func newNFS3(b base) *nfs3 {
	return &nfs3{base: b, source: b.spec.Server + ":" + b.spec.Export}
}

// missing lists what the mount lacks of this driver's mount of the export,
// by token. It is empty when m is that mount. A mount of another file system
// type or source is reported as that alone: its options say nothing useful.
func (n *nfs3) missing(m observe.Mount) []string {
	var out []string
	if m.FSType != nfs3FSType {
		out = append(out, "fstype "+nfs3FSType)
	}
	if m.Source != n.source {
		out = append(out, "source "+n.source)
	}
	if len(out) != 0 {
		return out
	}
	for _, want := range nfs3SuperOptions {
		if !hasToken(m.SuperOptions, want) {
			out = append(out, want)
		}
	}
	for _, want := range nfs3MountOptions {
		if !hasToken(m.Options, want) {
			out = append(out, want)
		}
	}
	return out
}

func hasToken(tokens []string, want string) bool {
	for _, t := range tokens {
		if t == want {
			return true
		}
	}
	return false
}

func (n *nfs3) Mounted(ctx context.Context) (bool, error) {
	t, err := n.m.Mounts(ctx)
	if err != nil {
		return false, err
	}
	top, state := n.inspect(t)
	return state == dirMounted && len(n.missing(top)) == 0, nil
}

func (n *nfs3) Mount(ctx context.Context) error {
	t, err := n.m.Mounts(ctx)
	if err != nil {
		return err
	}
	top, state := n.inspect(t)
	switch state {
	case dirHidden:
		return n.conflict(top, state, "")
	case dirMounted:
		// The Mounter alone would call a mount of the same source a no-op
		// whatever its options, so the profile is checked here.
		missing := n.missing(top)
		if len(missing) == 0 {
			return nil
		}
		return n.conflict(top, state, "it lacks "+strings.Join(missing, ", "))
	}

	addr, err := n.addr(ctx)
	if err != nil {
		return err
	}
	if err := n.makeMountPoint(); err != nil {
		return err
	}
	err = n.m.Mount(ctx, n.source, n.dir, nfs3FSType, nfs3Flags, nfs3Data(addr))
	if errors.Is(err, unix.EINVAL) {
		return fmt.Errorf("nfs3 mount of %s: the kernel rejected the mount options (EINVAL); the profile's softerr option needs Linux 5.3 or newer, and there is no fallback to soft or hard: %w", n.source, err)
	}
	return err
}

// addr returns the address for the addr= option: an IP literal as it is, a
// name resolved by the host resolver under its own deadline, the first
// address it returns.
func (n *nfs3) addr(ctx context.Context) (string, error) {
	server := n.spec.Server
	if a, ok := parseIPv6Literal(server); ok {
		return a.String(), nil
	}
	if a, err := netip.ParseAddr(server); err == nil {
		return a.String(), nil
	}
	what := fmt.Sprintf("resolving nfs3 server %q", server)
	addrs, err := within(ctx, n.opts.ResolveTimeout, what, func(ctx context.Context) ([]net.IPAddr, error) {
		return n.opts.Resolver.LookupIPAddr(ctx, server)
	})
	if err != nil {
		return "", fmt.Errorf("%s: %w", what, err)
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("%s: the name has no address", what)
	}
	a, ok := netip.AddrFromSlice(addrs[0].IP)
	if !ok {
		return "", fmt.Errorf("%s: the resolver returned an invalid address", what)
	}
	a = a.Unmap()
	if addrs[0].Zone != "" && a.Is6() {
		a = a.WithZone(addrs[0].Zone)
	}
	return a.String(), nil
}
