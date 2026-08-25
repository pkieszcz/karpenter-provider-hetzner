package instancetype

import (
	"context"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hetznercloud/hcloud-go/v2/hcloud"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"

	apiv1 "github.com/paperclipinc/karpenter-provider-hetzner/pkg/apis/v1"
	"github.com/paperclipinc/karpenter-provider-hetzner/pkg/metrics"
)

const cacheTTL = 6 * time.Hour

// ServerTypeClient is the narrow interface for the hcloud server types API.
type ServerTypeClient interface {
	All(ctx context.Context) ([]*hcloud.ServerType, error)
}

// Provider resolves Hetzner server types to Karpenter InstanceTypes.
type Provider struct {
	client ServerTypeClient

	// vmMemoryOverheadPercent estimates the gap between a server type's
	// advertised memory and what the guest sees, for types no node has yet
	// reported. Once one has, discovered holds the measured value instead.
	vmMemoryOverheadPercent float64

	mu          sync.RWMutex
	cachedTypes []*cloudprovider.InstanceType
	cacheExpiry time.Time

	unavailable *unavailableCache
	discovered  *discoveredCapacityCache
}

// NewProvider creates a new instance type provider.
func NewProvider(client ServerTypeClient, vmMemoryOverheadPercent float64) *Provider {
	return &Provider{
		client:                  client,
		vmMemoryOverheadPercent: vmMemoryOverheadPercent,
		unavailable: newUnavailableCache(
			// 5m: long enough to route around a saturated location, short enough to
			// retry it soon. Matches observed recovery -- a resource_unavailable on
			// (cx43, hel1) cleared within minutes, an identical create succeeding ~70s
			// later. A long quarantine would keep falling through to a ~4x-priced type
			// well after capacity returned, so if this is ever tuned, prefer a short base
			// with backoff on repeat failures over a longer flat TTL.
			// TODO: make configurable via operator config if needed.
			5 * time.Minute,
		),
		discovered: newDiscoveredCapacityCache(),
	}
}

// List returns all available InstanceTypes for a node class, filtered to those
// with offerings in its locations. A nil node class lists every type with no
// location filter and no declared kubelet reservations.
//
// The 6h cache holds only what depends on the hcloud catalogue. Anything that
// depends on the node class -- its kubelet reservations, and the capacity
// measured for its image -- is applied per call, so a node class edit takes
// effect immediately instead of at the next catalogue refresh.
func (p *Provider) List(ctx context.Context, nodeClass *apiv1.HCloudNodeClass) ([]*cloudprovider.InstanceType, error) {
	var locations []string
	if nodeClass != nil {
		locations = nodeClass.Spec.Locations
	}

	p.mu.RLock()
	if p.cachedTypes != nil && time.Now().Before(p.cacheExpiry) {
		cached := p.cachedTypes
		p.mu.RUnlock()
		metrics.RecordCacheHit()
		return p.resolve(filterByLocations(cached, locations), nodeClass), nil
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock.
	if p.cachedTypes != nil && time.Now().Before(p.cacheExpiry) {
		metrics.RecordCacheHit()
		return p.resolve(filterByLocations(p.cachedTypes, locations), nodeClass), nil
	}

	// Cache miss: fetch fresh data from the hcloud API.
	metrics.RecordCacheMiss()

	serverTypes, err := p.client.All(ctx)
	if err != nil {
		return nil, err
	}

	types := make([]*cloudprovider.InstanceType, 0, len(serverTypes))
	for _, st := range serverTypes {
		types = append(types, toInstanceType(st, p.vmMemoryOverheadPercent))
	}

	p.cachedTypes = types
	p.cacheExpiry = time.Now().Add(cacheTTL)

	return p.resolve(filterByLocations(types, locations), nodeClass), nil
}

// MarkUnavailable records that a (serverType, location) offering failed with a
// capacity error so it is reported unavailable for a TTL. The mark takes effect
// on the next call to List, which Karpenter invokes at the start of each
// provisioning cycle (not within the cycle that failed).
func (p *Provider) MarkUnavailable(serverType, location string) {
	p.unavailable.markUnavailable(serverType, location)
}

// resolve returns copies of the given instance types with everything that must
// not be baked into the 6h catalogue cache applied fresh:
//
//   - each offering's Available flag, from the unavailable cache, so the
//     catalogue never staleness-traps availability;
//   - memory capacity measured from a registered node of that type and image,
//     when one has been seen, in place of the estimate;
//   - the overhead implied by this node class's declared kubelet reservations.
//
// The returned InstanceType and Offering structs are fresh value-copies, and
// Capacity is rebuilt rather than shared because the discovered value differs
// per node class. Requirements stays shared read-only with the cached entry, so
// callers must not mutate it.
func (p *Provider) resolve(types []*cloudprovider.InstanceType, nodeClass *apiv1.HCloudNodeClass) []*cloudprovider.InstanceType {
	out := make([]*cloudprovider.InstanceType, len(types))
	for i, it := range types {
		offerings := make(cloudprovider.Offerings, len(it.Offerings))
		for j, o := range it.Offerings {
			zone := o.Requirements.Get(corev1.LabelTopologyZone).Any()
			cp := *o
			// AND, never overwrite: o.Available already encodes catalogue facts (unpriced
			// or withdrawn by hcloud) that the capacity cache knows nothing about.
			cp.Available = o.Available && !p.unavailable.isUnavailable(it.Name, zone)
			offerings[j] = &cp
		}

		capacity := make(corev1.ResourceList, len(it.Capacity))
		for k, v := range it.Capacity {
			capacity[k] = v
		}
		if measured, ok := p.discovered.get(it.Name, nodeClass); ok {
			capacity[corev1.ResourceMemory] = measured
		}

		// Construct a fresh InstanceType (rather than copying *it) to avoid
		// copying the embedded sync.Once (govet copylocks), which also matters
		// because Allocatable() memoises on first call.
		out[i] = &cloudprovider.InstanceType{
			Name:         it.Name,
			Offerings:    offerings,
			Requirements: it.Requirements,
			Capacity:     capacity,
			Overhead:     overheadFor(nodeClass, capacity),
		}
	}
	return out
}

// toInstanceType maps a Hetzner ServerType to a Karpenter InstanceType. Only
// node-class-independent facts belong here, since the result is cached across
// every node class; Overhead is left nil and filled in by resolve.
func toInstanceType(st *hcloud.ServerType, vmMemoryOverheadPercent float64) *cloudprovider.InstanceType {
	arch := "amd64"
	if st.Architecture == hcloud.ArchitectureARM {
		arch = "arm64"
	}

	cpuType := string(st.CPUType) // "shared" or "dedicated"

	// Build offerings: one per pricing location. Every priced location stays in the
	// catalogue even when it cannot be launched -- karpenter core documents that Offerings
	// must list all allowed offerings "even if they're temporarily unavailable", and
	// treats a running node whose offering has disappeared as drifted, replacing healthy
	// nodes. Availability, not membership, is what keeps an offering out of selection.
	//
	// Availability is NOT gated on ServerType.Locations[].Available. That flag is wrong in
	// BOTH directions, so no reading of it is safe:
	//
	//   false negatives -- observed reading false for (cx53, nbg1) 50 minutes after the
	//   API accepted a cx53 create there, for (cx53, fsn1) while a cx53 was created in
	//   fsn1, and across all three eu-central locations while eight cx53 nodes ran.
	//
	//   false positives -- hcloud datacenter describe hel1-dc2 listed cx43 in both
	//   server_types.available and available_for_migration, and the create was still
	//   rejected with resource_unavailable; an identical request minutes later succeeded.
	//
	// It carries no signal about whether a server can be created, so gating on it
	// proactively is unsound whichever way it is interpreted. Excluding those offerings
	// drops them out of ranking entirely, so a 32Gi pod falls through cx53 (EUR 29.49) to
	// cpx62 (EUR 129.99) -- a 4.4x regression from the change meant to prevent exactly
	// that. Withdrawn pairs are handled reactively by the unavailable cache, which marks
	// a pair only after a real create failure and so cannot be fooled in either
	// direction.
	offerings := make(cloudprovider.Offerings, 0, len(st.Pricings))
	for _, p := range st.Pricings {
		if p.Location == nil {
			continue
		}
		// An unpriced offering cannot be ranked, so it is unavailable rather than priced
		// at 0 -- a 0 sorts as the best deal in the cluster and would win every selection.
		price, priced := hourlyNetPrice(p)
		offerings = append(offerings, &cloudprovider.Offering{
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeOnDemand),
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, p.Location.Name),
			),
			Price:     price,
			Available: priced,
		})
	}

	// Memory: ServerType.Memory is float32 in GB. Hetzner's figure is what the
	// VM is allocated, not what the guest kernel ends up seeing, so hold back an
	// estimate of the difference until a real node tells us the true number.
	memBytes := memoryWithVMOverhead(int64(float64(st.Memory)*1024*1024*1024), vmMemoryOverheadPercent)
	// Disk: ServerType.Disk is int in GB.
	diskBytes := int64(st.Disk) * 1024 * 1024 * 1024

	return &cloudprovider.InstanceType{
		Name:      st.Name,
		Offerings: offerings,
		Requirements: scheduling.NewRequirements(
			scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, st.Name),
			scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, arch),
			scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeOnDemand),
			scheduling.NewRequirement(apiv1.LabelCPUType, corev1.NodeSelectorOpIn, cpuType),
			scheduling.NewRequirement(apiv1.LabelServerFamily, corev1.NodeSelectorOpIn, serverFamily(st.Name)),
		),
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU:              *resource.NewMilliQuantity(int64(st.Cores)*1000, resource.DecimalSI),
			corev1.ResourceMemory:           *resource.NewQuantity(memBytes, resource.BinarySI),
			corev1.ResourceEphemeralStorage: *resource.NewQuantity(diskBytes, resource.BinarySI),
			corev1.ResourcePods:             *resource.NewQuantity(110, resource.DecimalSI),
		},
	}
}

// serverFamily extracts the server type family prefix (e.g. "cax", "cx", "cpx", "ccx").
func serverFamily(name string) string {
	for _, prefix := range []string{"cax", "cpx", "ccx", "cx"} {
		if strings.HasPrefix(name, prefix) {
			return prefix
		}
	}
	// Fall back to any leading alpha characters.
	i := 0
	for i < len(name) && (name[i] < '0' || name[i] > '9') {
		i++
	}
	if i > 0 {
		return name[:i]
	}
	return name
}

// Pricing here is the server-type base net price and intentionally excludes the
// primary-IPv4 surcharge: the catalog is NodeClass-agnostic. Cost-sensitive
// clusters drop the IPv4 charge with HCloudNodeClass.spec.enablePublicIPv4=false.
//
// hourlyNetPrice returns the net hourly price for a server-type pricing entry,
// preferring the explicit hourly figure and falling back to monthly/730. ok is false
// when neither figure yields a usable price; callers must drop the offering rather
// than substitute a default, because a zero price ranks as the best deal in the
// cluster and would win every selection.
func hourlyNetPrice(p hcloud.ServerTypeLocationPricing) (price float64, ok bool) {
	// ParseFloat accepts "inf"/"NaN", and an infinite price would sort last forever
	// rather than being recognised as unusable, so require a finite positive figure.
	if v, err := strconv.ParseFloat(strings.TrimSpace(p.Hourly.Net), 64); err == nil && v > 0 && !math.IsInf(v, 0) {
		return v, true
	}
	if v, err := strconv.ParseFloat(strings.TrimSpace(p.Monthly.Net), 64); err == nil && v > 0 && !math.IsInf(v, 0) {
		return v / 730, true
	}
	return 0, false
}

// filterByLocations returns only the instance types that have at least one offering in the requested locations.
// If locations is empty, all instance types are returned unchanged.
func filterByLocations(types []*cloudprovider.InstanceType, locations []string) []*cloudprovider.InstanceType {
	if len(locations) == 0 {
		return types
	}
	locSet := make(map[string]struct{}, len(locations))
	for _, l := range locations {
		locSet[l] = struct{}{}
	}

	result := make([]*cloudprovider.InstanceType, 0, len(types))
	for _, it := range types {
		filtered := make(cloudprovider.Offerings, 0, len(it.Offerings))
		for _, o := range it.Offerings {
			zone := o.Requirements.Get(corev1.LabelTopologyZone).Any()
			if _, ok := locSet[zone]; ok {
				filtered = append(filtered, o)
			}
		}
		if len(filtered) > 0 {
			result = append(result, &cloudprovider.InstanceType{
				Name:         it.Name,
				Offerings:    filtered,
				Requirements: it.Requirements,
				Capacity:     it.Capacity,
				Overhead:     it.Overhead,
			})
		}
	}
	return result
}
