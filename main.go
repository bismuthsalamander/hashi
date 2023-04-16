package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/bismuthsalamander/hashi/log"
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

func (r *River) SetToGive(ct int) {
	r.ToGive = ct
	r.Islands[0].Update()
	r.Islands[1].Update()
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
		log.Debug("Found no left on %d,%d\n", r, c)
		return nil
	}
	for ci := c + 1; ci < b.Cols; ci++ {
		if b.Grid[r][ci] != nil {
			right = b.Grid[r][ci]
			break
		}
	}
	if right == nil {
		log.Debug("Found no right on %d,%d\n", r, c)
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
				r := b.CreateRiver(left, right)
				log.Debug("Added river %s\n", r)
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
				log.Debug("Added river %s\n", r)
				for crossRow := top.R + 1; crossRow < bottom.R; crossRow++ {
					crossingRiver := b.FindHorizontalRiverCrossing(crossRow, ci)
					if crossingRiver != nil {
						log.Debug("River %s crosses river %s\n", r, crossingRiver)
						markCrossing(crossingRiver, r)
					} else {
						log.Debug("Checked cell r%d, c%d\n", crossRow, ci)
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
	log.Debug("Splitting")
	for _, txt := range strings.Split(data, "\n") {
		txt = strings.Trim(txt, "\r\n")
		if len(txt) > 0 {
			lines = append(lines, txt)
		}
	}
	log.Debug("Making board")
	b := Board{Grid: make([][]*Island, 0), Rows: len(lines), Cols: len(lines[0]), Clusters: []*Cluster{}, AllRivers: []*River{}, AllIslands: []*Island{}}
	log.Debug("Making islands")
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
	log.Debug("Adding rivers")
	b.CreateRivers()
	return &b, nil
}

func GetBoardFromFile(fn string) (*Board, error) {
	log.Debug("Reading file")
	data, err := os.ReadFile(fn)
	log.Debug("Read file")
	if err != nil {
		return nil, err
	}
	return BoardFromString(string(data))
}

func (b *Board) String() string {
	return b.String2(false)
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

	addBridges := func(output [][]rune, ri int, ci int, count int, direction int) {
		if direction == HORIZONTAL {
			if output[ri][ci] == ' ' {
				if count == 1 {
					output[ri][ci] = '-'
				} else if count == 2 {
					output[ri][ci] = '='
				}
			} else if output[ri][ci] == '|' {
				if count == 1 {
					output[ri][ci] = '+'
				} else if count == 2 {
					output[ri][ci] = 'F'
				}
			} else if output[ri][ci] == '"' {
				if count == 1 {
					output[ri][ci] = 'H'
				} else if count == 2 {
					output[ri][ci] = '#'
				}
			}
		} else if direction == VERTICAL {
			if output[ri][ci] == ' ' {
				if count == 1 {
					output[ri][ci] = '|'
				} else if count == 2 {
					output[ri][ci] = '"'
				}
			} else if output[ri][ci] == '-' {
				if count == 1 {
					output[ri][ci] = '+'
				} else if count == 2 {
					output[ri][ci] = 'H'
				}
			} else if output[ri][ci] == '=' {
				if count == 1 {
					output[ri][ci] = 'F'
				} else if count == 2 {
					output[ri][ci] = '#'
				}
			}
		}
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
				addBridges(grid, ri, ci, r.Bridges, HORIZONTAL)
			}
		} else {
			//Vertical
			rtop := min(r.Islands[0].R, r.Islands[1].R)
			rbot := max(r.Islands[0].R, r.Islands[1].R)
			ci := r.Islands[0].C
			for ri := rtop + 1; ri < rbot; ri++ {
				log.Trace("Writing %d vertical bridges at %d, %d\n", r.Bridges, ri, ci)
				addBridges(grid, ri, ci, r.Bridges, VERTICAL)
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
	return out
}

func (b *Board) ZeroBridgeExcesses() bool {
	changed := false
	for _, island := range b.AllIslands {
		if island.IsComplete() {
			continue
		}
		if island.Available == island.NumNeeded() {
			log.Debug("Island %s num %d bridges %d needed %d available %d\n", island, island.Num, island.Bridges, island.NumNeeded(), island.Available)
			for _, r := range island.LiveRivers {
				if r.ToGive > 1 {
					log.Trace("Adding a second one because ToGive is %d!\n", r.ToGive)
					b.AddBridge(r)
				}
				log.Debug("Adding one or more bridges with %s\n", r.Neighbor(island))
				b.AddBridge(r)
				log.Debug("Me: %s Neighbor: %s\n", island, r.Neighbor(island))
				changed = true
			}
		}
	}
	return changed
}

func (b *Board) OneBridgeExcesses() bool {
	changed := false
	for _, island := range b.AllIslands {
		if island.Available == island.NumNeeded()+1 {
			for _, r := range island.LiveRivers {
				if r.ToGive > 1 {
					log.Debug("Island %s needs %d, available %d; adding bridge to river %s\n", island, island.NumNeeded(), island.Available, r)
					b.AddBridge(r)
					changed = true
				}
			}
		}
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

func (b *Board) CapToAvoidIsolation() bool {
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
			if n.NumNeeded() <= r.ToGive {
				log.Debug("River %s would result in an isolation with these clusters:\n%s\n%s\nReducing ToGive to %d\n", r, n.Cluster, i.Cluster, n.NumNeeded()-1)
				r.SetToGive(n.NumNeeded() - 1)
				changed = true
			}
		}
	}
	return changed
}

func main() {
	//log.LEVEL = log.DEBUG
	b, err := GetBoardFromFile("problem36.txt")
	if err != nil {
		fmt.Printf("error loading file: %s\n", err)
		return
	}
	changed := true
	for changed {
		changed = false
		changed = b.ZeroBridgeExcesses() || changed
		changed = b.OneBridgeExcesses() || changed
		changed = b.CapToAvoidIsolation() || changed
		/*
			b.AddBridgeBetween(b.Grid[0][2], b.Grid[0][0])
			fmt.Printf("%s\n", b)
			b.AddBridgeBetween(b.Grid[2][2], b.Grid[2][4])
			fmt.Printf("%s\n", b)
			b.AddBridgeBetween(b.Grid[0][2], b.Grid[2][2])
			fmt.Printf("%s\n", b)
		*/
	}
	fmt.Printf("%s\n", b)
	res, reason := b.IsSolved()
	fmt.Printf("Solved: %v", res)
	if reason != nil {
		fmt.Printf(" (%v)", reason)
	}
	fmt.Printf("\n")
}
