package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/bismuthsalamander/hashi/log"
	"github.com/bismuthsalamander/stopwatch"
)

const (
	HORIZONTAL = 0
	VERTICAL   = 1
)

// TODO: do we need to index rivers by direction?
type Island struct {
	Num        int
	Bridges    int
	Available  int
	R          int
	C          int
	Rivers     []*River
	LiveRivers []*River
	Cluster    *Cluster
}

type River struct {
	Islands   []*Island
	Crossings []*River
	Bridges   int
	ToGive    int
	Max       int
}

type Board struct {
	Grid       [][]*Island
	Rows       int
	Cols       int
	AllIslands []*Island
	AllRivers  []*River
	Clusters   []*Cluster
}

type Cluster struct {
	Map map[*Island]struct{}
}

func EmptyCluster() *Cluster {
	c := Cluster{
		Map: make(map[*Island]struct{}),
	}
	return &c
}

func SoloCluster(i *Island) *Cluster {
	c := EmptyCluster()
	c.Add(i)
	return c
}

func (c *Cluster) Add(i *Island) {
	c.Map[i] = struct{}{}
}

func (c *Cluster) AddAll(other *Cluster) {
	for k := range other.Map {
		c.Add(k)
	}
}

func (c *Cluster) Contains(i *Island) bool {
	_, ok := c.Map[i]
	return ok
}

func (c *Cluster) Remove(i *Island) {
	delete(c.Map, i)
}

func (c *Cluster) Size() int {
	return len(c.Map)
}

func (c *Cluster) IncompleteIslands() []*Island {
	ret := []*Island{}
	for i := range c.Map {
		if !i.IsComplete() {
			ret = append(ret, i)
		}
	}
	return ret
}

func (b *Board) IsSolved() (bool, error) {
	//1. do all rivers have <= 2 bridges?
	for _, r := range b.AllRivers {
		if r.Bridges > r.Max {
			return false, fmt.Errorf("river %s has %d bridges; max is %d", r, r.Bridges, r.Max)
		}
	}

	//2. does each island have the correct number of bridges?
	for _, island := range b.AllIslands {
		if island.Bridges != island.Num {
			return false, fmt.Errorf("island %s has %d bridges; target is %d", island, island.Bridges, island.Num)
		}
	}

	//3. are all islands in a single cluster?
	if len(b.Clusters) != 1 {
		return false, fmt.Errorf("islands are divided into %d clusters; should have 1", len(b.Clusters))
	}
	if b.Clusters[0].Size() != len(b.AllIslands) {
		return false, fmt.Errorf("cluster has %d islands; should have all %d", b.Clusters[0].Size(), len(b.AllIslands))
	}

	//4. are there clashing bridges (i.e., two crossing rivers each with >= 1 bridge)?
	for _, r := range b.AllRivers {
		if r.Bridges == 0 {
			continue
		}
		for _, cross := range r.Crossings {
			if cross.Bridges > 0 {
				return false, fmt.Errorf("bridges %s and %s cross, but both have bridges (%d and %d)", r, cross, r.Bridges, cross.Bridges)
			}
		}
	}
	return true, nil
}

func (b *Board) HasMistakes() (bool, error) {
	//1. do any rivers have too many bridges?
	for _, r := range b.AllRivers {
		for _, i := range r.Islands {
			if r.Bridges > i.Num {
				return true, fmt.Errorf("river %s has %d bridges; island %s needs %d", r, r.Bridges, i, i.Num)
			}
		}
	}

	//2. do any islands have too few bridges available?
	for _, i := range b.AllIslands {
		if i.Available < i.NumNeeded() {
			return true, fmt.Errorf("island %s needs %d bridges, but only %d are available", i, i.NumNeeded(), i.Available)
		}

	}

	//3. is there an incomplete cluster with no edges?
	if len(b.Clusters) > 1 {
		for _, c := range b.Clusters {
			if len(c.Edges()) == 0 {
				return true, fmt.Errorf("cluster %s has no edges and does not contain all islands", c)
			}
		}
	}

	//4. are there clashing bridges (i.e., two crossing rivers each with >= 1 bridge)?
	for _, r := range b.AllRivers {
		if r.Bridges == 0 {
			continue
		}
		for _, cross := range r.Crossings {
			if cross.Bridges > 0 {
				return true, fmt.Errorf("bridges %s and %s cross, but both have bridges (%d and %d)", r, cross, r.Bridges, cross.Bridges)
			}
		}
	}
	return false, nil
}

func (b *Board) AddIsland(ct int, r int, c int) *Island {
	i := Island{
		Num:       ct,
		Bridges:   0,
		Available: 0,
		R:         r,
		C:         c,
		Rivers:    []*River{},
		Cluster:   nil,
	}
	cluster := SoloCluster(&i)
	i.Cluster = cluster
	b.Grid[r][c] = &i
	b.Clusters = append(b.Clusters, cluster)
	b.AllIslands = append(b.AllIslands, &i)
	return &i
}

func (b *Board) RemoveCluster(deadcluster *Cluster) {
	for i, c := range b.Clusters {
		if c == deadcluster {
			b.Clusters[i] = b.Clusters[len(b.Clusters)-1]
			b.Clusters = b.Clusters[:len(b.Clusters)-1]
			return
		}
	}
}

func (b *Board) joinClusters(ca *Cluster, cb *Cluster) bool {
	if ca == cb {
		return false
	}
	ca.AddAll(cb)
	for i := range cb.Map {
		i.Cluster = ca
	}
	b.RemoveCluster(cb)
	return true
}

func (b *Board) AddBridge(r *River) error {
	if r.ToGive < 1 || r.Bridges >= r.Max {
		return fmt.Errorf("river %s has no more bridges to give", r)
	}
	r.Bridges++
	r.ToGive--
	r.Islands[0].Update()
	r.Islands[1].Update()
	b.joinClusters(r.Islands[0].Cluster, r.Islands[1].Cluster)
	for _, crossingRiver := range r.Crossings {
		crossingRiver.SetToGive(0)
	}
	return nil
}

func (r *River) CapToGive(mx int) bool {
	if r.ToGive > mx {
		r.SetToGive(mx)
		return true
	}
	return false
}

func (r *River) SetToGive(ct int) {
	r.ToGive = ct
	r.Islands[0].Update()
	r.Islands[1].Update()
}

func (r *River) Crosses(other *River) bool {
	for _, test := range r.Crossings {
		if test == other {
			return true
		}
	}
	return false
}

func (i *Island) NumNeeded() int {
	return i.Num - i.Bridges
}

func (i *Island) IsComplete() bool {
	return i.Num == i.Bridges
}

func (i *Island) Update() {
	riversToUpdate := []*River{}
	newBridges := 0
	newAvailable := 0
	newLiveRivers := []*River{}
	//Update bridges, then river ToGives, then Available
	for _, r := range i.Rivers {
		newBridges += r.Bridges
	}
	i.Bridges = newBridges
	for _, r := range i.Rivers {
		newToGive := min(i.NumNeeded(), r.ToGive)
		if newToGive != r.ToGive {
			riversToUpdate = append(riversToUpdate, r)
			r.ToGive = newToGive
		}
	}
	for _, r := range i.Rivers {
		newAvailable += r.ToGive
		if r.ToGive > 0 {
			newLiveRivers = append(newLiveRivers, r)
		}
	}
	i.LiveRivers = newLiveRivers
	i.Available = newAvailable
	for _, r := range riversToUpdate {
		r.Neighbor(i).Update()
	}
}

func (b *Board) AddBridgeBetween(ia *Island, ib *Island) error {
	r := ia.RiverWith(ib)
	if r == nil {
		return fmt.Errorf("islands %s and %s are not adjacent", ia, ib)
	}
	return b.AddBridge(r)
}

func min3(a int, b int, c int) int {
	return min(a, min(b, c))
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// generate a river between those two islands, properly initializing ToGive
func (b *Board) CreateRiver(ia *Island, ib *Island) *River {
	r := River{
		Islands:   []*Island{ia, ib},
		Crossings: []*River{},
		Bridges:   0,
		ToGive:    min3(ia.Num, ib.Num, 2),
		Max:       2,
	}
	ia.addRiver(&r)
	ib.addRiver(&r)
	//fmt.Printf("New river: num %d num %d togive %d\n", ia.Num, ib.Num, r.ToGive)
	b.AllRivers = append(b.AllRivers, &r)
	ia.Update()
	ib.Update()
	return &r
}

// get the pointer to this island's river with island other, or nil
func (i *Island) RiverWith(other *Island) *River {
	for _, r := range i.Rivers {
		if r.Connects(other) {
			return r
		}
	}
	return nil
}

func (i *Island) addRiver(r *River) {
	i.Rivers = append(i.Rivers, r)
	i.LiveRivers = append(i.LiveRivers, r)
}

// Is the parameter target one of the two islands that r connects?
func (r *River) Connects(target *Island) bool {
	for _, i := range r.Islands {
		if i == target {
			return true
		}
	}
	return false
}

func (r *River) Neighbor(me *Island) *Island {
	if r.Islands[0] == me {
		return r.Islands[1]
	}
	return r.Islands[0]
}

func (i *Island) String() string {
	return fmt.Sprintf("[%d/%d] (r%d, c%d) a%d", i.Bridges, i.Num, i.R, i.C, i.Available)
}

func (r *River) String() string {
	return fmt.Sprintf("%s <=> %s", r.Islands[0], r.Islands[1])
}

func (c *Cluster) String() string {
	out := fmt.Sprintf("Cluster of size %d: ", c.Size())
	for i := range c.Map {
		out += fmt.Sprintf("%s ", i)
	}
	out += fmt.Sprintf("Edges: %v", c.Edges())
	return out
}

func markCrossing(a *River, b *River) {
	a.Crossings = append(a.Crossings, b)
	b.Crossings = append(b.Crossings, a)
}

// the direction here is arbitrary. we happened to add horizontal rivers first
// in AddRivers, so we look for horizontal crossings here.
func (b *Board) FindHorizontalRiverCrossing(r int, c int) *River {
	var left *Island = nil
	var right *Island = nil
	for ci := c - 1; ci >= 0; ci-- {
		if b.Grid[r][ci] != nil {
			left = b.Grid[r][ci]
			break
		}
	}
	if left == nil {
		return nil
	}
	for ci := c + 1; ci < b.Cols; ci++ {
		if b.Grid[r][ci] != nil {
			right = b.Grid[r][ci]
			break
		}
	}
	if right == nil {
		return nil
	}
	return left.RiverWith(right)
}

func (b *Board) CreateRivers() {
	//horizontal
	for ri := 0; ri < b.Rows; ri++ {
		var left *Island
		for ci := 0; ci < b.Cols; ci++ {
			right := b.Grid[ri][ci]
			if right == nil {
				continue
			}

			if left != nil {
				b.CreateRiver(left, right)
			}
			left = right
		}
	}

	//add verticals and track crossings
	for ci := 0; ci < b.Cols; ci++ {
		var top *Island
		for ri := 0; ri < b.Rows; ri++ {
			bottom := b.Grid[ri][ci]
			if bottom == nil {
				continue
			}
			if top != nil {
				r := b.CreateRiver(top, bottom)
				for crossRow := top.R + 1; crossRow < bottom.R; crossRow++ {
					crossingRiver := b.FindHorizontalRiverCrossing(crossRow, ci)
					if crossingRiver != nil {
						markCrossing(crossingRiver, r)
					}
				}
			}
			top = bottom
		}
	}
}

func (b *Board) DebugOut() string {
	out := ""
	for _, i := range b.AllIslands {
		out += fmt.Sprintf("%s\n", i)
		for _, r := range i.Rivers {
			out += fmt.Sprintf("\t%s\n", r)
		}
	}
	return out
}

func BoardFromString(data string) (*Board, error) {
	lines := make([]string, 0)
	for _, txt := range strings.Split(data, "\n") {
		txt = strings.Trim(txt, "\r\n")
		if len(txt) > 0 {
			lines = append(lines, txt)
		}
	}
	b := Board{Grid: make([][]*Island, 0), Rows: len(lines), Cols: len(lines[0]), Clusters: []*Cluster{}, AllRivers: []*River{}, AllIslands: []*Island{}}
	for ri, rowstr := range lines {
		if len(rowstr) != b.Cols {
			return nil, fmt.Errorf("board has %d cols, but row %d has %d cells", b.Cols, ri, len(rowstr))
		}
		row := make([]*Island, b.Cols)
		b.Grid = append(b.Grid, row)
		for ci, ch := range rowstr {
			if ch >= '1' && ch <= '8' {
				b.AddIsland(int(ch-'0'), ri, ci)
			}
		}
	}
	b.CreateRivers()
	return &b, nil
}

func GetBoardFromFile(fn string) (*Board, error) {
	stopwatch.Start("Load board")
	defer stopwatch.Stop("Load board")
	data, err := os.ReadFile(fn)
	if err != nil {
		return nil, err
	}
	return BoardFromString(string(data))
}

func (b *Board) String() string {
	return b.String2(true)
}

func (b *Board) String2(short bool) string {
	grid := make([][]rune, b.Rows)
	for ri := 0; ri < b.Rows; ri++ {
		grid[ri] = make([]rune, b.Cols)
		for ci := 0; ci < b.Cols; ci++ {
			if b.Grid[ri][ci] != nil {
				grid[ri][ci] = rune(fmt.Sprintf("%d", b.Grid[ri][ci].Num)[0])
			} else {
				grid[ri][ci] = ' '
			}
		}
	}

	getBridgeChar := func(direction int, num int) rune {
		if direction == HORIZONTAL {
			if num == 1 {
				return '-'
			} else if num == 2 {
				return '='
			}
		} else if direction == VERTICAL {
			if num == 1 {
				return '|'
			} else if num == 2 {
				return '"'
			}
		}
		return ' '
	}
	addBridgeChars := func(h rune, v rune) rune {
		if v == '|' {
			if h == '-' {
				return '+'
			} else if h == '=' {
				return 'F'
			}
			return v
		} else if v == '"' {
			if h == '-' {
				return 'H'
			} else if h == '=' {
				return '#'
			}
			return v
		}
		//v must be a space
		return h
	}

	for _, r := range b.AllRivers {
		if r.Bridges == 0 {
			continue
		}

		if r.Islands[0].R == r.Islands[1].R {
			//Horizontal
			cleft := min(r.Islands[0].C, r.Islands[1].C)
			cright := max(r.Islands[0].C, r.Islands[1].C)
			ri := r.Islands[0].R
			for ci := cleft + 1; ci < cright; ci++ {
				log.Trace("Writing %d horizontal bridges at %d, %d\n", r.Bridges, ri, ci)
				grid[ri][ci] = addBridgeChars(getBridgeChar(HORIZONTAL, r.Bridges), grid[ri][ci])
			}
		} else {
			//Vertical
			rtop := min(r.Islands[0].R, r.Islands[1].R)
			rbot := max(r.Islands[0].R, r.Islands[1].R)
			ci := r.Islands[0].C
			for ri := rtop + 1; ri < rbot; ri++ {
				log.Trace("Writing %d vertical bridges at %d, %d\n", r.Bridges, ri, ci)
				grid[ri][ci] = addBridgeChars(grid[ri][ci], getBridgeChar(VERTICAL, r.Bridges))
			}
		}
	}
	out := ""
	for _, runerow := range grid {
		for _, r := range runerow {
			out += string(r)
		}
		out += "\n"
	}
	if short {
		return out[:len(out)-1]
	}
	out += fmt.Sprintf("Clusters (%d)\n", len(b.Clusters))
	for _, c := range b.Clusters {
		out += fmt.Sprintf("%s\n", c)
	}
	return out[:len(out)-1]
}

func (b *Board) RequiredFill() bool {
	changed := false
	for _, island := range b.AllIslands {
		b.MustProvide(island.LiveRivers, island.NumNeeded())
	}
	return changed
}

func (c *Cluster) Edges() []*Island {
	edges := []*Island{}
	for i := range c.Map {
		isLiberty := false
		for _, r := range i.LiveRivers {
			n := r.Neighbor(i)
			if !c.Contains(n) {
				isLiberty = true
				break
			}
		}
		if isLiberty {
			edges = append(edges, i)
		}
	}
	return edges
}

func (b *Board) CapToAvoidJoinedIsolation() bool {
	changed := false
	if len(b.Clusters) <= 2 {
		return changed
	}
	for _, c := range b.Clusters {
		edges := c.Edges()
		if len(edges) != 1 {
			continue
		}
		i := edges[0]
		for _, r := range i.LiveRivers {
			n := r.Neighbor(i)
			neighborEdges := n.Cluster.Edges()
			if len(neighborEdges) != 1 {
				continue
			}
			if n.NumNeeded() != i.NumNeeded() {
				continue
			}
			if r.CapToGive(n.NumNeeded() - 1) {
				changed = true
			}
		}
	}
	return changed
}

func (b *Board) CapToAvoidSelfIsolation() bool {
	changed := false
	if len(b.Clusters) <= 2 {
		return changed
	}
	for _, c := range b.Clusters {
		incomplete := c.IncompleteIslands()
		if len(incomplete) != 2 {
			continue
		}
		r := incomplete[0].RiverWith(incomplete[1])
		if r == nil {
			continue
		}
		if incomplete[0].NumNeeded() != incomplete[1].NumNeeded() || incomplete[0].NumNeeded() < r.ToGive {
			continue
		}
		if r.CapToGive(incomplete[0].NumNeeded() - 1) {
			changed = true
		}
	}
	return changed
}

func (b *Board) MustProvide(rivers []*River, ct int) bool {
	changed := false
	avail := 0
	for _, r := range rivers {
		avail += r.ToGive
	}
	excess := avail - ct
	for _, r := range rivers {
		toAdd := r.ToGive - excess
		for i := 0; i < toAdd; i++ {
			b.AddBridge(r)
			changed = true
		}
	}
	return changed
}

// TODO: should we switch to directional river pointers instead of doing all this looping?
func (b *Board) BadCorners() bool {
	changed := false
	//grab a "corner" pair of rivers
	//find all neighbors with TWO rivers intersected by that corner
	//if the neighbor no longer has enough, we have an impermissible corner
	//max permissible in that pair is max(r1.ToGive, r2.ToGive)
	//if current island needs more, then we have to get it from the other two (?) rivers
	for _, i := range b.AllIslands {
		if i.IsComplete() || len(i.LiveRivers) < 2 {
			continue
		}
		emptyRivers := []*River{}
		for _, r := range i.LiveRivers {
			if r.Bridges == 0 {
				emptyRivers = append(emptyRivers, r)
			}
		}
		if len(emptyRivers) < 2 {
			continue
		}
		for ri := 0; ri < len(emptyRivers); ri++ {
			for rj := ri + 1; rj < len(emptyRivers); rj++ {
				hitIslands := make(map[*Island]int)

				for _, crossed := range emptyRivers[ri].Crossings {
					hitIslands[crossed.Islands[0]]++
					hitIslands[crossed.Islands[1]]++
				}
				for _, crossed := range emptyRivers[rj].Crossings {
					hitIslands[crossed.Islands[0]]++
					hitIslands[crossed.Islands[1]]++
				}
				for hitIsland, hitCount := range hitIslands {
					if hitCount < 2 {
						continue
					}
					hitLeftAfterCorner := 0
					for _, r := range hitIsland.LiveRivers {
						if !emptyRivers[ri].Crosses(r) && !emptyRivers[rj].Crosses(r) {
							hitLeftAfterCorner += r.ToGive
						}
					}
					if hitLeftAfterCorner >= hitIsland.NumNeeded() {
						continue
					}

					cornerMax := max(emptyRivers[ri].ToGive, emptyRivers[rj].ToGive)
					othersMustProvide := i.NumNeeded() - cornerMax
					others := []*River{}
					for _, other := range i.LiveRivers {
						if other == emptyRivers[ri] || other == emptyRivers[rj] {
							continue
						}
						others = append(others, other)
					}

					result := b.MustProvide(others, othersMustProvide)
					if result {
						changed = true
						break
					}
				}
			}
		}
	}
	return changed
}

func (b *Board) MakeAGuess() bool {
	stopwatch.Start("MakeAGuess")
	defer stopwatch.Stop("MakeAGuess")
	for _, i := range b.AllIslands {
		if i.IsComplete() {
			continue
		}
		for _, r := range i.LiveRivers {
			n := r.Neighbor(i)
			c := b.Clone()
			cloneI := c.Grid[i.R][i.C]
			cloneN := c.Grid[n.R][n.C]
			cloneI.RiverWith(cloneN).CapToGive(0)
			c.AutoSolve(false)
			if m, _ := c.HasMistakes(); m {
				b.AddBridge(r)
				return true
			}
		}

		for _, r := range i.LiveRivers {
			n := r.Neighbor(i)
			c := b.Clone()
			cloneI := c.Grid[i.R][i.C]
			cloneN := c.Grid[n.R][n.C]
			for j := 0; j < r.ToGive; j++ {
				c.AddBridgeBetween(cloneI, cloneN)
			}
			c.AutoSolve(false)
			if m, _ := c.HasMistakes(); m {
				r.CapToGive(r.ToGive - 1)
				return true
			}
		}
	}
	return false
}

func (b *Board) Clone() *Board {
	stopwatch.Start("Clone board")
	defer stopwatch.Stop("Clone board")
	copy := Board{Grid: make([][]*Island, 0), Rows: b.Rows, Cols: b.Cols, Clusters: []*Cluster{}, AllRivers: []*River{}, AllIslands: []*Island{}}
	for i := 0; i < copy.Rows; i++ {
		copy.Grid = append(copy.Grid, make([]*Island, copy.Cols))
	}
	//clone islands and initialize clusters
	for _, oldI := range b.AllIslands {
		copy.AddIsland(oldI.Num, oldI.R, oldI.C)
	}
	//create rivers with bridge counts and merge clusters
	for _, oldR := range b.AllRivers {
		oldA := oldR.Islands[0]
		oldB := oldR.Islands[1]
		newA := copy.Grid[oldA.R][oldA.C]
		newB := copy.Grid[oldB.R][oldB.C]
		newR := copy.CreateRiver(newA, newB)
		for j := 0; j < oldR.Bridges; j++ {
			copy.AddBridge(newR)
		}
	}
	//add crossings
	for ci := 0; ci < copy.Cols; ci++ {
		var top *Island
		for ri := 0; ri < copy.Rows; ri++ {
			bottom := copy.Grid[ri][ci]
			if bottom == nil {
				continue
			}
			if top != nil {
				r := top.RiverWith(bottom)
				for crossRow := top.R + 1; crossRow < bottom.R; crossRow++ {
					crossingRiver := copy.FindHorizontalRiverCrossing(crossRow, ci)
					if crossingRiver != nil {
						markCrossing(crossingRiver, r)
					}
				}
			}
			top = bottom
		}
	}

	return &copy
}

func (b *Board) AutoSolve(allowGuess bool) {
	changed := true
	for changed {
		changed = false
		changed = b.RequiredFill() || changed
		changed = b.CapToAvoidJoinedIsolation() || changed
		changed = b.CapToAvoidSelfIsolation() || changed
		if m, _ := b.HasMistakes(); m {
			return
		}
		if !changed {
			changed = b.BadCorners() || changed
		}
		if !changed && allowGuess {
			changed = changed || b.MakeAGuess()
		}
	}
}

func printUsage() {
	fmt.Printf("usage: %s [problemfile] [options]\n", os.Args[0])
	fmt.Printf("options:\t-t: print execution time profile\n")
}

func main() {
	//log.LEVEL = log.DEBUG
	if len(os.Args) > 3 {
		printUsage()
		return
	}
	var file string = ""
	var timer bool = false
	for idx, arg := range os.Args {
		if idx == 0 {
			continue
		}
		if arg == "-t" {
			timer = true
		} else {
			if file == "" {
				file = arg
			} else {
				fmt.Printf("unrecognized argument: %s\n", arg)
				return
			}
		}
	}
	if file == "" {
		printUsage()
		return
	}
	b, err := GetBoardFromFile(os.Args[1])
	if err != nil {
		fmt.Printf("error loading file: %s\n", err)
		return
	}
	b.AutoSolve(true)
	fmt.Printf("%s\n", b)
	res, reason := b.IsSolved()
	fmt.Printf("Solved: %v", res)
	if reason != nil {
		fmt.Printf(" (%v)", reason)
	}
	fmt.Print("\n")
	if timer {
		fmt.Print(stopwatch.Results())
	}
}

//todo: precompute small clusters? 3: 2 1 1?
