package main

import (
	"strconv"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"sort"

	blake2b "github.com/minio/blake2b-simd"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	chash "github.com/golang/groupcache/consistenthash"
	"github.com/pilosa/pilosa/ctl"
	"github.com/pkg/errors"
	"github.com/twmb/murmur3"
)

func main() {
	// playHash()
	// playShardDist()
	// printShardDistMaxAve()
	// playPartitionDistribution()
	// playMglev()
	// playHashRing()
	playBounded()
}

func playPartitionDistribution() {
	p1 := getPartitionDistribution("index", 1024, 256)
	p2 := getPartitionDistribution("ada", 512, 256)
	p3 := getPartitionDistribution("index", 800, 256)
	p4 := getPartitionDistribution("johnny", 100, 256)
	p5 := getPartitionDistribution("astrophysicists", 22, 256)
	p6 := getPartitionDistribution("index", 127, 256)
	p7 := getPartitionDistribution("chicken", 4, 256)
	p8 := getPartitionDistribution("toffee", 6, 256)
	p9 := getPartitionDistribution("unicorns", 650, 256)
	p10 := getPartitionDistribution("dragonstone", 4234, 256)
	p11 := getPartitionDistribution("ice", 26, 256)
	p12 := getPartitionDistribution("baby", 28, 256)

	combined := combineMaps(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12)

	fmt.Println(combined)
	var sum int
	for _, v := range combined {
		sum += v
	}
	fmt.Println(sum)
}

func playMglevDist() {

}

func playMglev() {
	const numNodes = 10
	const partitionN = 241

	nodeIDs := make([]string, numNodes)
	for i := range nodeIDs {
		nodeIDs[i] = fmt.Sprintf("node%d", i)
	}
	m := newMglevHasher(nodeIDs, partitionN)

	for numShards := 12; numShards <= 1000; numShards++ {
		mp := make(map[string]int)
		p := make(map[int]int)
		for i := 0; i <= numShards; i++ {
			partitionID := partition("index", uint64(i), partitionN)
			nodeID := m.partitionNodes(partitionID, partitionN, nodeIDs)
			p[partitionID]++
			mp[nodeID]++
		}
		// fmt.Println(p)
		// fmt.Println(mp)
		sizes := make([]int, numNodes)
		for i, nodeID := range nodeIDs {
			numShards := mp[nodeID]
			sizes[i] = numShards
		}
		_, max, ave, _ := analyzeSizeDist(sizes, numNodes, numShards)
		fmt.Println(float64(max) / ave)
	}
}
func playHashRing() {
	const numNodes = 10
	// const partitionN = 359
	const replica = 10000

	nodeIDs := make([]string, numNodes)
	for i := range nodeIDs {
		nodeIDs[i] = fmt.Sprintf("node%d", i)
	}
	hr := chash.New(replica, nil)
	hr.Add(nodeIDs...)

	for numShards := 12; numShards <= 1000; numShards++ {
		mp := make(map[string]int)
		// p := make(map[int]int)
		for i := 0; i <= numShards; i++ {
			// partitionID := partition("index", uint64(i), partitionN)
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], uint64(i))
			
			h := fnv.New64a()
			h.Write(buf[:])
			ID := string(h.Sum([]byte("index")))
			
			nodeID := hr.Get(ID)
			// p[partitionID]++
			mp[nodeID]++
		}
		// fmt.Println(p)
		// fmt.Println(mp)
		sizes := make([]int, numNodes)
		for i, nodeID := range nodeIDs {
			numShards := mp[nodeID]
			sizes[i] = numShards
		}
		_, max, ave, _ := analyzeSizeDist(sizes, numNodes, numShards)
		fmt.Println(float64(max) / ave)
	}
}

func getPartitionDistribution(index string, numShards, partitionN uint64) map[int]int {
	partitions := make(map[int]int)
	for i := uint64(0); i <= numShards; i++ {
		par := partition(index, i, partitionN)
		partitions[par]++
	}
	return partitions
}

// partition returns the partition that a shard belongs to.
func partition(index string, shard, partitionN uint64) int {
	// var buf [8]byte
	// binary.BigEndian.PutUint64(buf[:], shard)

	// // Hash the bytes and mod by partition count.
	// h := fnv.New64a()
	// _, _ = h.Write([]byte(index))
	// // _, _ = h.Write(buf[:])
	// return int((h.Sum64() + shard) % partitionN)
	return int((murmur3.StringSum64(index) + shard) % partitionN)
}

// partitionNodes returns a list of nodes that own a partition. unprotected.
func (m *mglevhasher) partitionNodes(partitionID, partitionN int, nodeIDs []string) string {
	// func (c *cluster) partitionNodes(partitionID uint64) []*Node {
	// Default replica count to between one and the number of nodes.
	// The replica count can be zero if there are no nodes.

	// Determine primary owner node.
	var h Hasher2 = m
	id := h.Hash(partitionID)

	return id
}

func playShardDist() {
	p1, _ := getSizes(getShardDistribution("index", 6, 1024))
	p2, _ := getSizes(getShardDistribution("ada", 6, 512))
	p3, _ := getSizes(getShardDistribution("index", 6, 800))
	p4, _ := getSizes(getShardDistribution("johnny", 6, 100))
	p5, _ := getSizes(getShardDistribution("astrophysicists", 6, 22))
	p6, _ := getSizes(getShardDistribution("index", 6, 127))
	p7, _ := getSizes(getShardDistribution("chicken", 6, 4))
	p8, _ := getSizes(getShardDistribution("toffee", 6, 6))
	p9, _ := getSizes(getShardDistribution("unicorns", 6, 650))
	p10, _ := getSizes(getShardDistribution("dragonstone", 6, 4234))
	p11, _ := getSizes(getShardDistribution("ice", 6, 26))
	p12, _ := getSizes(getShardDistribution("baby", 6, 28))

	combined := combineSize(p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12)
	fmt.Println(combined)
}

func printShardDistMaxAve() {
	for i := 12; i <= 10000; i++ {
		_, max, ave, _ := analyzeShardDistribution("index", 10, i)
		fmt.Println(float64(max) / ave)
	}
}

func playShardDistributionsDiff() {
	// for i := 10; i <= 10000; i++ {
	// 	_, max, ave, _ := analyzeShardDistribution("index", 10, i)
	// 	fmt.Println(float64(max)/ave)
	// }
	index := "index"
	nds1 := getShardDistribution(index, 6, 1000)
	nds2 := getShardDistribution(index, 5, 1000)
	nds2 = append([][]uint64{[]uint64{}}, nds2...)

	min, max, ave := analyzeSizes(nds1, nds2, 1)
	fmt.Println(min, max, ave)
}

func analyzeShardDistribution(index string, nodes, shards int) (int, int, float64, float64) {
	nds := getShardDistribution(index, nodes, shards)
	sizes, err := getSizes(nds)
	if err != nil {
		panic(err)
	}
	return analyzeSizeDist(sizes, nodes, shards)
}

func analyzeSizeDist(sizes []int, nodes, shards int) (int, int, float64, float64) {
	min := 1 << 32
	max := -1
	var sd float64

	ave := float64(shards+1) / float64(nodes)
	for _, size := range sizes {
		if size < min {
			min = size
		}
		if size > max {
			max = size
		}
		sd += math.Pow((float64(size) - ave), 2)
	}
	sd = math.Sqrt(sd / float64(nodes))

	ave = math.Ceil(ave)

	return min, max, ave, sd
}

func analyzeSizes(nds1, nds2 Nodes, start int) (int, int, float64) {
	var size int
	if len(nds1) <= len(nds2) {
		size = len(nds1)
	} else {
		size = len(nds2)
	}
	min := 1 << 32
	max := -1
	var ave float64

	for i := start; i < size; i++ {
		diffSlice := differenceSlice(nds1[i], nds2[i])
		diff := len(diffSlice)

		if diff < min {
			min = diff
		}
		if diff > max {
			max = diff
		}
		ave += float64(diff)
	}
	ave /= float64(size - start)
	ave = math.Ceil(ave)
	return min, max, ave
}

func getSizes(nds Nodes) ([]int, error) {
	if nds == nil {
		return nil, errors.New("nil nodes")
	}
	sizes := make([]int, 0)
	for _, nd := range nds {
		sizes = append(sizes, len(nd))
	}
	return sizes, nil
}

func getShardDistribution(index string, nodes, shards int) Nodes {
	var nds Nodes
	cmd := ctl.NewShardDistributionCommand(os.Stdin, &nds, os.Stderr)
	cmd.Index = index
	cmd.NumNodes = nodes
	cmd.MaxShard = shards

	cmd.Run(context.Background())
	return nds
}

// Nodes contains the nodes with their shards.
type Nodes [][]uint64

// Write implements the io.Writer interface.
func (nds *Nodes) Write(b []byte) (n int, err error) {
	if err := json.Unmarshal(b, nds); err != nil {
		return 0, errors.Wrap(err, "error unmarhsaling nodes")
	}
	return len(b), nil
}

func combineMaps(m1 map[int]int, maps ...map[int]int) map[int]int {
	for _, m := range maps {
		for key, val := range m {
			m1[key] += val
		}
	}
	return m1
}

func combineSize(s1 []int, slices ...[]int) []int {
	size := len(s1)
	for _, s := range slices {
		for i := 0; i < size; i++ {
			s1[i] += s[i]
		}
	}
	return s1
}

// differenceSlice returns a slice containing the values
// present in the first slice but not in the second.
func differenceSlice(s1, s2 []uint64) []uint64 {
	// throw s2 in a map, check if each value
	// in s1 is also in that map
	hash := make(map[uint64]bool)
	for _, val := range s2 {
		hash[val] = true
	}
	diff := make([]uint64, 0)
	for _, val := range s1 {
		if _, found := hash[val]; !found {
			diff = append(diff, val)
		}
	}
	// make sure duplicates in s1 are not added
	return diff
}

// xorSlice returns an array containing the values
// present in exactly one of the two slices.
func xorSlice(s1, s2 []uint64) []uint64 {
	// throw both slices in maps
	hash1 := make(map[uint64]bool)
	for _, val := range s1 {
		hash1[val] = true
	}
	hash2 := make(map[uint64]bool)
	for _, val := range s2 {
		hash2[val] = true
	}

	xor := make([]uint64, 0)
	// add all values in hash1 not in hash2
	for key := range hash1 {
		if _, found := hash2[key]; !found {
			xor = append(xor, key)
		}
	}
	// add all values in hash2 not in hash1
	for key := range hash2 {
		if _, found := hash1[key]; !found {
			xor = append(xor, key)
		}
	}

	if len(xor) == 0 {
		return nil
	}
	return xor
}

func playHash() {
	sizes := make(map[int]int)
	for i := uint64(0); i < 7545; i++ {
		h := hash(i, 6)
		sizes[h]++
	}
	c := make([]int, 0)
	for _, v := range sizes {
		c = append(c, v)
	}
	fmt.Println(c)
}

// Hash returns the integer hash for the given key.
func hash(key uint64, n int) int {
	b, j := int64(-1), int64(0)
	for j < int64(n) {
		b = j
		key = key*uint64(2862933555777941757) + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int(b)
}

// Hasher2 interface
type Hasher2 interface {
	Hash(key int) string
}

type mglevhasher struct {
	// The permutation table of partitions and nodes.
	permutation map[string][]int

	lookup []string

	nodeIDs []string
}

func newMglevHasher(nodeIDs []string, M int) *mglevhasher {
	ids := make([]string, len(nodeIDs))
	copy(ids, nodeIDs)
	m := &mglevhasher{
		nodeIDs: ids,
	}
	m.generatePermutations(M)
	m.populate(M)
	return m
}

func (m *mglevhasher) generatePermutations(M int) {
	m.permutation = make(map[string][]int)
	for _, ID := range m.nodeIDs {
		offset := int(murmur3.StringSum64(ID) % uint64(M))
		h := fnv.New64a()
		_, _ = h.Write([]byte(ID))
		skip := int(h.Sum64()%uint64(M-1)) + 1

		m.permutation[ID] = make([]int, M)
		for i := 0; i < M; i++ {
			m.permutation[ID][i] = (offset + i*skip) % M
		}
	}
}

func (m *mglevhasher) populate(M int) {
	N := len(m.nodeIDs)
	m.lookup = make([]string, M)
	next := make([]int, N)
	n := 0
	for {
		for i, ID := range m.nodeIDs {
			c := m.permutation[ID][next[i]]
			for m.lookup[c] != "" {
				next[i]++
				c = m.permutation[ID][next[i]]
			}
			m.lookup[c] = ID
			next[i]++
			n++
			if n == M {
				return
			}
		}
	}
}

// Hash returns the node for the given key.
func (m *mglevhasher) Hash(key int) string {
	return m.lookup[key]
}

func playBounded() {
	numNodes := 10
	

	c := New()

	nodeIDs := make([]string, numNodes)
	for i := range nodeIDs {
		nodeIDs[i] = fmt.Sprintf("node%d", i)
		c.Add(nodeIDs[i])
	}

	for numShards := 12; numShards <= 1000; numShards++ {
		mp := make(map[string]int)
		for i := 0; i <= numShards; i++ {
			host, err := c.GetLeast(strconv.Itoa(i))
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			c.Inc(host)
			mp[host]++
		}
		// fmt.Println(p)
		// fmt.Println(mp)
		sizes := make([]int, numNodes)
		for i, nodeID := range nodeIDs {
			numShards := mp[nodeID]
			sizes[i] = numShards
		}
		_, max, ave, _ := analyzeSizeDist(sizes, numNodes, numShards)
		fmt.Println(float64(max) / ave)
	}
}

// type member string

// func (m member) String() string {
// 	return string(m)
// }

// type hasher struct {}

// func (h hasher) Sum64(data []byte) uint64 {
// 	f := fnv.New64a()
// 	f.Write(data)
// 	return f.Sum64()
// }

// func playBounded() {
// 	numNodes := 10
	
// 	cfg := consistent.Config{
// 		PartitionCount:    241,
// 		ReplicationFactor: 200,
// 		Load:              1.1,
// 		Hasher:            hasher{},
// 	}
// 	c := consistent.New(nil, cfg)

// 	nodeIDs := make([]string, numNodes)
// 	for i := range nodeIDs {
// 		nodeIDs[i] = fmt.Sprintf("node%d", i)
// 		c.Add(member(nodeIDs[i]))
// 	}

// 	for numShards := 12; numShards <= 1000; numShards++ {
// 		mp := make(map[string]int)
// 		for i := 0; i <= numShards; i++ {
// 			var key [8]byte
// 			binary.BigEndian.PutUint64(key[:], uint64(i))
// 			nodeID := c.LocateKey(key[:]).String()
// 			mp[nodeID]++
// 		}
// 		// fmt.Println(p)
// 		// fmt.Println(mp)
// 		sizes := make([]int, numNodes)
// 		for i, nodeID := range nodeIDs {
// 			numShards := mp[nodeID]
// 			sizes[i] = numShards
// 		}
// 		_, max, ave, _ := analyzeSizeDist(sizes, numNodes, numShards)
// 		fmt.Println(float64(max) / ave)
// 		fmt.Println(c.LoadDistribution())
// 	}
// }

const load = 1.0

func dummy() {
	xxhash.New()
	murmur3.New64()
	fnv.New64()
	binary.Size(0)
	var _ chash.Map
	strconv.Atoi("ss")
	var _ consistent.Consistent
}



const replicationFactor = 10

var ErrNoHosts = errors.New("no hosts added")

type Host struct {
	Name string
	Load int64
}

type Consistent struct {
	hosts     map[uint64]string
	sortedSet []uint64
	loadMap   map[string]*Host
	totalLoad int64

	sync.RWMutex
}

func New() *Consistent {
	return &Consistent{
		hosts:     map[uint64]string{},
		sortedSet: []uint64{},
		loadMap:   map[string]*Host{},
	}
}

func (c *Consistent) Add(host string) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.loadMap[host]; ok {
		return
	}

	c.loadMap[host] = &Host{Name: host, Load: 0}
	for i := 0; i < replicationFactor; i++ {
		h := c.hash(fmt.Sprintf("%s%d", host, i))
		c.hosts[h] = host
		c.sortedSet = append(c.sortedSet, h)

	}
	// sort hashes ascendingly
	sort.Slice(c.sortedSet, func(i int, j int) bool {
		if c.sortedSet[i] < c.sortedSet[j] {
			return true
		}
		return false
	})
}

// Returns the host that owns `key`.
//
// As described in https://en.wikipedia.org/wiki/Consistent_hashing
//
// It returns ErrNoHosts if the ring has no hosts in it.
func (c *Consistent) Get(key string) (string, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.hosts) == 0 {
		return "", ErrNoHosts
	}

	h := c.hash(key)
	idx := c.search(h)
	return c.hosts[c.sortedSet[idx]], nil
}

// It uses Consistent Hashing With Bounded loads
//
// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
//
// to pick the least loaded host that can serve the key
//
// It returns ErrNoHosts if the ring has no hosts in it.
//
func (c *Consistent) GetLeast(key string) (string, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.hosts) == 0 {
		return "", ErrNoHosts
	}

	h := c.hash(key)
	idx := c.search(h)

	i := idx
	for {
		host := c.hosts[c.sortedSet[i]]
		if c.loadOK(host) {
			return host, nil
		}
		i++
		if i >= len(c.hosts) {
			i = 0
		}
	}
}

func (c *Consistent) search(key uint64) int {
	idx := sort.Search(len(c.sortedSet), func(i int) bool {
		return c.sortedSet[i] >= key
	})

	if idx >= len(c.sortedSet) {
		idx = 0
	}
	return idx
}

// Sets the load of `host` to the given `load`
func (c *Consistent) UpdateLoad(host string, load int64) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.loadMap[host]; !ok {
		return
	}
	c.totalLoad -= c.loadMap[host].Load
	c.loadMap[host].Load = load
	c.totalLoad += load
}

// Increments the load of host by 1
//
// should only be used with if you obtained a host with GetLeast
func (c *Consistent) Inc(host string) {
	c.Lock()
	defer c.Unlock()

	atomic.AddInt64(&c.loadMap[host].Load, 1)
	atomic.AddInt64(&c.totalLoad, 1)
}

// Decrements the load of host by 1
//
// should only be used with if you obtained a host with GetLeast
func (c *Consistent) Done(host string) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.loadMap[host]; !ok {
		return
	}
	atomic.AddInt64(&c.loadMap[host].Load, -1)
	atomic.AddInt64(&c.totalLoad, -1)
}

// Deletes host from the ring
func (c *Consistent) Remove(host string) bool {
	c.Lock()
	defer c.Unlock()

	for i := 0; i < replicationFactor; i++ {
		h := c.hash(fmt.Sprintf("%s%d", host, i))
		delete(c.hosts, h)
		c.delSlice(h)
	}
	delete(c.loadMap, host)
	return true
}

// Return the list of hosts in the ring
func (c *Consistent) Hosts() (hosts []string) {
	c.RLock()
	defer c.RUnlock()
	for k, _ := range c.loadMap {
		hosts = append(hosts, k)
	}
	return hosts
}

// Returns the loads of all the hosts
func (c *Consistent) GetLoads() map[string]int64 {
	loads := map[string]int64{}

	for k, v := range c.loadMap {
		loads[k] = v.Load
	}
	return loads
}

// Returns the maximum load of the single host
// which is:
// (total_load/number_of_hosts)*1.25
// total_load = is the total number of active requests served by hosts
// for more info:
// https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
func (c *Consistent) MaxLoad() int64 {
	if c.totalLoad == 0 {
		c.totalLoad = 1
	}
	var avgLoadPerNode float64
	avgLoadPerNode = float64(c.totalLoad / int64(len(c.loadMap)))
	if avgLoadPerNode == 0 {
		avgLoadPerNode = 1
	}
	avgLoadPerNode = math.Ceil(avgLoadPerNode * load)
	return int64(avgLoadPerNode)
}

func (c *Consistent) loadOK(host string) bool {
	// a safety check if someone performed c.Done more than needed
	if c.totalLoad < 0 {
		c.totalLoad = 0
	}

	var avgLoadPerNode float64
	avgLoadPerNode = float64((c.totalLoad + 1) / int64(len(c.loadMap)))
	if avgLoadPerNode == 0 {
		avgLoadPerNode = 1
	}
	avgLoadPerNode = math.Ceil(avgLoadPerNode * load)

	bhost, ok := c.loadMap[host]
	if !ok {
		panic(fmt.Sprintf("given host(%s) not in loadsMap", bhost.Name))
	}

	if float64(bhost.Load)+1 <= avgLoadPerNode {
		return true
	}

	return false
}

func (c *Consistent) delSlice(val uint64) {
	idx := -1
	l := 0
	r := len(c.sortedSet) - 1
	for l <= r {
		m := (l + r) / 2
		if c.sortedSet[m] == val {
			idx = m
			break
		} else if c.sortedSet[m] < val {
			l = m + 1
		} else if c.sortedSet[m] > val {
			r = m - 1
		}
	}
	if idx != -1 {
		c.sortedSet = append(c.sortedSet[:idx], c.sortedSet[idx+1:]...)
	}
}

func (c *Consistent) hash(key string) uint64 {
	out := blake2b.Sum512([]byte(key))
	return binary.LittleEndian.Uint64(out[:])
}