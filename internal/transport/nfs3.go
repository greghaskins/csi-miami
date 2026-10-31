package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
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

// Adoption is "this profile and nothing that changes its meaning", not a
// byte-exact comparison of the options: rsize and wsize are negotiated with
// the server, newer kernels add tokens (softreval, fatal_neterrors), and the
// MOUNT request contributes mountaddr, mountvers, mountport and mountproto.
// Requiring those would reject this driver's own mount on another kernel or
// server. So the profile's tokens are required, the tokens that would change
// its semantics are forbidden, and everything else is ignored.

// nfs3SuperOptions are the tokens that /proc/self/mountinfo must show among a
// mount's super options for it to be this driver's mount. The kernel reports
// the profile in its own words, not the ones it was given, so these were taken
// from a real mount (testdata/mountinfo_nfs3.txt, from the e2e run against a
// kernel server), not from the profile:
//
//	rw,vers=3,rsize=1048576,wsize=1048576,namlen=255,
//	acregmin=30,acregmax=30,acdirmax=30,softerr,softreval,nolock,
//	fatal_neterrors=none,proto=tcp,timeo=100,retrans=3,sec=sys,
//	mountaddr=<ip>,mountvers=3,mountproto=tcp,lookupcache=pos,
//	local_lock=all,addr=<ip>
//
// actimeo=30 shows as three of the four ac* options (acdirmin is 30 by
// default, and a default is not shown); lookupcache=positive shows as
// lookupcache=pos. softreval and fatal_neterrors come from newer kernels than
// softerr does, rsize and wsize are negotiated with the server, and sec and
// the mount* options describe the MOUNT request, so none of those is
// required. addr is not either: a name may resolve differently later, and the
// source string is what names the server.
var nfs3SuperOptions = []string{
	"rw",
	"vers=3",
	"proto=tcp",
	"softerr",
	"timeo=100",
	"retrans=3",
	"nolock",
	"local_lock=all",
	"lookupcache=pos",
	"acregmin=30",
	"acregmax=30",
	"acdirmax=30",
}

// nfs3Forbidden are bare super option tokens that change what the profile
// means, so a mount that has one is not this driver's mount even when every
// required token is present: no close-to-open consistency (nocto), a
// superblock not shared with the same export mounted elsewhere
// (nosharecache), client ports above 1023 (noresvport), and the failure
// modes the profile exists to avoid (soft, hard) or a read-only super block.
var nfs3Forbidden = []string{"nocto", "nosharecache", "noresvport", "soft", "hard", "ro"}

// nfs3Pinned are the valued super options, by name, that must have exactly
// the value the profile gives when they are shown. Absence is fine for
// acdirmin: the default is not shown, and actimeo=30 lands on it.
var nfs3Pinned = map[string]string{
	"acdirmin=":    "acdirmin=30",
	"sec=":         "sec=sys",
	"lookupcache=": "lookupcache=pos",
	"local_lock=":  "local_lock=all",
}

// nfs3MountOptions are the only tokens the mount's own options (not the super
// block's) may have, and both must be present: the mount is read-write and
// noatime, and nosuid, nodev, noexec, ro or any other flag means someone
// else's mount.
var nfs3MountOptions = []string{"rw", "noatime"}

// nfs3 is an NFSv3 export mounted with mount(2) at Dir.
type nfs3 struct {
	base
	source string // <server>:<export>, as this driver passes it
}

func newNFS3(b base) *nfs3 {
	return &nfs3{base: b, source: b.spec.Server + ":" + b.spec.Export}
}

// differs lists how m differs from this driver's mount of the export; it is
// empty when m is that mount. A mount of another file system type or source
// is reported as that alone: its options say nothing useful.
func (n *nfs3) differs(m observe.Mount) []string {
	if m.FSType != nfs3FSType {
		return []string{"it is not an " + nfs3FSType + " mount"}
	}
	if m.Source != n.source {
		return []string{"its source is not " + n.source + "; a server must be named one way across StorageClasses, so a host name and its address do not match each other"}
	}
	var out []string
	for _, want := range nfs3SuperOptions {
		if !slices.Contains(m.SuperOptions, want) {
			out = append(out, "it lacks "+want)
		}
	}
	for _, tok := range m.SuperOptions {
		if slices.Contains(nfs3Forbidden, tok) {
			out = append(out, "it has "+tok)
			continue
		}
		for prefix, allowed := range nfs3Pinned {
			if strings.HasPrefix(tok, prefix) && tok != allowed {
				out = append(out, "it has "+tok)
			}
		}
	}
	for _, want := range nfs3MountOptions {
		if !slices.Contains(m.Options, want) {
			out = append(out, "it lacks the mount option "+want)
		}
	}
	for _, tok := range m.Options {
		if !slices.Contains(nfs3MountOptions, tok) {
			out = append(out, "it has the mount option "+tok)
		}
	}
	return out
}

// judgeTable classifies Dir in t.
func (n *nfs3) judgeTable(t observe.Table) verdict {
	v, _ := n.judge(t, func(m observe.Mount) (string, error) {
		return strings.Join(n.differs(m), ", "), nil
	})
	return v
}

func (n *nfs3) State(ctx context.Context) (State, error) {
	t, err := n.m.Mounts(ctx)
	if err != nil {
		return StateEmpty, err
	}
	return n.judgeTable(t).state, nil
}

func (n *nfs3) Mounted(ctx context.Context) (bool, error) {
	s, err := n.State(ctx)
	return s == StateOurs, err
}

func (n *nfs3) Mount(ctx context.Context) error {
	t, err := n.m.Mounts(ctx)
	if err != nil {
		return err
	}
	// The Mounter alone would call a mount of the same source a no-op
	// whatever its options, so the profile is checked here.
	v := n.judgeTable(t)
	switch v.state {
	case StateOurs:
		return nil
	case StateForeign, StateHidden:
		return n.conflict(v)
	}

	addr, err := n.addr(ctx)
	if err != nil {
		return err
	}
	if err := n.makeMountPoint(ctx); err != nil {
		return err
	}
	err = n.m.Mount(ctx, n.source, n.dir, nfs3FSType, nfs3Flags, nfs3Data(addr))
	if errors.Is(err, unix.EINVAL) {
		return fmt.Errorf("nfs3 mount of %s: the kernel rejected the mount options (EINVAL); the profile's softerr option needs Linux 5.3 or newer, and there is no fallback to soft or hard: %w", n.source, err)
	}
	return err
}

// addr returns the address for the addr= option. The driver is IPv4 only: an
// IPv4 literal is used as it is, and a name is resolved by the host resolver
// under its own deadline and the first IPv4 address of the answer is used
// (an IPv4-mapped IPv6 address counts as IPv4). An answer with no IPv4
// address is an error. The server name is the key of the lookup in flight:
// while one that timed out has not returned, the next fails with
// mount.ErrBusy instead of starting another.
func (n *nfs3) addr(ctx context.Context) (string, error) {
	server := n.spec.Server
	if a, err := netip.ParseAddr(server); err == nil {
		return a.String(), nil
	}
	what := fmt.Sprintf("resolving nfs3 server %q", server)
	addrs, err := within(ctx, n.opts.ResolveTimeout, what, n.busy, "resolve\x00"+server, func(ctx context.Context) ([]net.IPAddr, error) {
		a, err := n.opts.Resolver.LookupIPAddr(ctx, server)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", what, err)
		}
		return a, nil
	})
	if err != nil {
		return "", err
	}
	for _, ia := range addrs {
		if a, ok := netip.AddrFromSlice(ia.IP); ok && a.Unmap().Is4() {
			return a.Unmap().String(), nil
		}
	}
	return "", fmt.Errorf("%s: the name has no IPv4 address", what)
}
