package shardmaster


import "raft"
import "labrpc"
import "sync"
import "encoding/gob"
import "fmt"
import "time"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	empty	bool
	result	map[int]chan Op
	clientMu sync.Mutex

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	NewConfig Config
}

type ShardsState struct{
	surplus	map[int][]int 
	lack	map[int][]int
	surplusCount int
	lackCount int
}

func ConfigMeasure(config Config) map[int][]int{
	shardInfo := make(map[int][]int)
	for index,gid := range config.Shards {
		if _,ok := shardInfo[gid]; ok{
			shardInfo[gid] = append(shardInfo[gid], index)
		}else{
			shardInfo[gid] = make([]int, 0)
			shardInfo[gid] = append(shardInfo[gid], index)
		}
	}
	return shardInfo
}

func GetShardsState(config Config, shardsState *ShardsState){
	shardsMap1 := map[int][]int{}
	shardsMap2 := map[int][]int{}
	count1, count2 := 0, 0

	for gid, _ := range config.Groups{
		c := 0
		cShards := make([]int, 0) 

		for index, cgid := range config.Shards{
			if gid == cgid {
				c += 1
				cShards = append(cShards, index)
			}
		}

		if c == count1 || count1 == 0{
			shardsMap1[gid] = cShards
			count1 = c
		}else{
			shardsMap2[gid] = cShards
			count2 = c
		}
	}

	// if all gids have the same count, then put them in surplus
	if count1 < count2{
		shardsState.surplus = shardsMap2
		shardsState.lack = shardsMap1
		shardsState.surplusCount = count2
		shardsState.lackCount = count2 - 1
	}else{
		shardsState.surplus = shardsMap1
		shardsState.lack = shardsMap2
		shardsState.surplusCount = count1
		shardsState.lackCount = count1 - 1
	}

	//fmt.Printf("GetShardsState surplus: %v\n, lack: %v\n", shardsState.surplus, shardsState.lack) 

}

func GetGidList(newServers map[int][]string, config Config) []int{
	newGidList := make([]int, 0)
	for newGid,_ := range(newServers){
		if _,ok := config.Groups[newGid];ok{
			continue
		}
		newGidList = append(newGidList, newGid)
	}
	return newGidList
}

func CopyConfig(oriConfig Config, newConfig *Config){
	newConfig.Num = oriConfig.Num
	newConfig.Groups = map[int][]string{}

	for index, shard := range oriConfig.Shards{
		newConfig.Shards[index] = shard
	}

	for key, value := range oriConfig.Groups{
		newConfig.Groups[key] = value
	}
}

func GetNewConfig(oriConfig Config, newConfig *Config){
	newConfig.Num = oriConfig.Num + 1
	newConfig.Groups = map[int][]string{}

	for index, shard := range oriConfig.Shards{
		newConfig.Shards[index] = shard
	}

	for key, value := range oriConfig.Groups{
		newConfig.Groups[key] = value
	}
}

func (sm *ShardMaster) AppendConfigToLog(entry Op) bool {
	index, _, isLeader := sm.rf.Start(entry)
	if !isLeader {
		return false
	}

	sm.mu.Lock()
	ch,ok := sm.result[index]
	if !ok {
		ch = make(chan Op,1)
		sm.result[index] = ch
	}
	sm.mu.Unlock()
	select {
	case <-ch:
		return true
	case <-time.After(1000 * time.Millisecond):
		//fmt.Printf("Me:%d, Wait Apply Timeout\n", sm.me)
		return false
	}
}

func (sm *ShardMaster) ApplyShardsState(config *Config, shardsState ShardsState){
	for gid, shards := range shardsState.surplus{
		for _, shard := range shards{
			config.Shards[shard] = gid
		} 
	}

	for gid, shards := range shardsState.lack{
		for _, shard := range shards{
			config.Shards[shard] = gid
		} 
	}
}

func (sm *ShardMaster) Apply(args Op) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.configs = append(sm.configs, args.NewConfig)
	sm.empty = false
	sm.empty = len(args.NewConfig.Groups) == 0
	//if len(args.NewConfig.Groups) == 0{
		//fmt.Printf("Empty State!!!\n")
	//}
	//if sm.rf.IsLeader() == false{
		//fmt.Printf("Follower Apply me:%d, config num:%d, confings len:%d\n", sm.me, args.NewConfig.Num, len(sm.configs))
	//}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	
	if sm.rf.IsLeader() == false {
		reply.WrongLeader = true
		return
	}

	sm.clientMu.Lock()
	defer sm.clientMu.Unlock()
	latestConfig := sm.configs[len(sm.configs) - 1]
	//shardInfo := ConfigMeasure(latestConfig)
	//newGidList := GetGidList(args.Servers, latestConfig) // need latestConfig to dedup 
	//newGidIndex := 0
	var newConfig Config
	GetNewConfig(latestConfig, &newConfig)
	var shardsState ShardsState
	GetShardsState(newConfig, &shardsState)
	//fmt.Printf("Join create num:%d\n", newConfig.Num)
	/*
	mean := 0
	if sm.empty{
		mean = NShards / len(newGidList)
		sm.empty = false
	}else{
		mean = NShards / (len(shardInfo) + len(newGidList))
	}

	pos := 0
	end := false
	getNum := 0

	for newGidIndex < len(newGidList){
		for gid,_ := range shardInfo {
			newConfig.Shards[shardInfo[gid][pos]] = newGidList[newGidIndex]
			getNum++
			if getNum == mean{
				if newGidIndex == len(newGidList) - 1{
					end = true
				} else{
					newGidIndex++
					getNum = 0
				}
			}
		}
		pos++
		if end {
			break
		}
	}
	*/
	empty := sm.empty
	for gid,serverList := range args.Servers{

		if _,ok := newConfig.Groups[gid];ok{
			for _,newStr := range(serverList){
				dup := false
				for _,oriStr := range(newConfig.Groups[gid]){
					if oriStr == newStr {
						dup = true
						break
					}
				}
				if dup == false{
					newConfig.Groups[gid] = append(newConfig.Groups[gid], newStr)
				}
			}
			continue
		}else{
			newConfig.Groups[gid] = serverList
			//fmt.Printf("Insert group:Servers:%v , gid:%d\n", serverList, gid)
		}

		count := 0
		gidShard := make([]int, 0)
		compelete := false
		for {
				newSurplus := shardsState.surplus
				if empty{
					for index := 0; index < len(newConfig.Shards); index++{
						gidShard = append(gidShard, index)
					}
					//newSurplus[gid] = gidShard
					empty = false
					break
				}

				for oGid, oShards := range shardsState.surplus{
					//fmt.Printf("oGid:%d, oShards:%v\n", oGid, oShards)
					gidShard = append(gidShard, oShards[len(oShards) - 1])
					shardsState.lack[oGid] = shardsState.surplus[oGid][0: len(shardsState.surplus[oGid])-1]
					delete(newSurplus, oGid)
					count += 1
					//fmt.Printf("count:%d, lackCount:%d\n", count, shardsState.lackCount)
					if(empty && count == len(newConfig.Shards)) || (!empty && count == shardsState.lackCount){
						compelete = true
						empty = false
						break
					}
				}

				if len(newSurplus) == 0{
					shardsState.surplus = shardsState.lack
					shardsState.lack = map[int][]int{}
					shardsState.surplusCount -= 1
					shardsState.lackCount -= 1
				}else{
					shardsState.surplus = newSurplus
				}

				if compelete{
					break
				}
		}
		if len(shardsState.lack) == 0{
			shardsState.surplus[gid] = gidShard
			shardsState.surplusCount = len(gidShard)
			shardsState.lackCount = len(gidShard) - 1
		}else{
			shardsState.lack[gid] = gidShard
		}
	}
	sm.ApplyShardsState(&newConfig, shardsState)

	//for gid := range(newConfig.Shards){
		//fmt.Printf("%d ",gid)
	//}

	//for gid,servers := range newConfig.Groups{
		//fmt.Printf("Num:%d, gid:%d, server:%v\n", newConfig.Num, gid, servers)
	//}

	fmt.Printf("Join, config.num:%d, Shards:%v \n", newConfig.Num, newConfig.Shards)
	//sm.configs = append(sm.configs, newConfig)
	entry := Op{NewConfig:newConfig}
	if !sm.AppendConfigToLog(entry){
		fmt.Printf("Me:%d, Append to Raft Error.\n", sm.me)
	}
	reply.WrongLeader = false
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if sm.rf.IsLeader() == false {
		reply.WrongLeader = true
		return
	}

	sm.clientMu.Lock()
	defer sm.clientMu.Unlock()
	latestConfig := sm.configs[len(sm.configs) - 1]
	var newConfig Config
	GetNewConfig(latestConfig, &newConfig)
	var shardsState ShardsState
	GetShardsState(newConfig, &shardsState)
	/*
	leaveList := make([]int, 0)  // node's ID
	remainList := make([]int, 0)  // leave gid
	for index,gid := range(newConfig.Shards){
		ifLeave := false
		for _,leaveGid := range(args.GIDs){
			if gid == leaveGid{
				leaveList = append(leaveList, index)
				ifLeave = true
				break
			}
		}

		if ifLeave == false {
			remainList = append(remainList, gid)
		}
	}

	leavePos := 0
	fmt.Printf("Leave list:%v remainList:%v\n", leaveList, remainList)
	for _,leaveNode := range(leaveList){
		if len(remainList) == 0{
			newConfig.Shards[leaveNode] = 0
		}else{
			newConfig.Shards[leaveNode] = remainList[leavePos]
			leavePos++
			if leavePos == len(remainList){
				leavePos = 0
			}
		}
	}
	*/

	for _, gid := range(args.GIDs){
		gidShard := make([]int, 0)
		if _,ok := shardsState.surplus[gid]; ok{
			gidShard = shardsState.surplus[gid]
			delete(shardsState.surplus, gid)
		}else{
			gidShard = shardsState.lack[gid]
			delete(shardsState.lack, gid)
		}

		for _, allocShard := range gidShard{
			if len(shardsState.lack) == 0{
				shardsState.lack = shardsState.surplus
				shardsState.surplus = map[int][]int{}
				shardsState.lackCount += 1
				shardsState.surplusCount += 1
			}

			dGid := -1
			for oGid, oShards := range shardsState.lack{
				shardsState.surplus[oGid] = append(oShards, allocShard)
				dGid = oGid
				break
			}
			delete(shardsState.lack, dGid)
		}

		delete(newConfig.Groups, gid)
		//fmt.Printf("Leave group:Current Shard:%v , gid:%d, group:%v \n", newConfig.Shards, gid, newConfig.Groups)
	}
	sm.ApplyShardsState(&newConfig, shardsState)

	//sm.configs = append(sm.configs, newConfig)
	fmt.Printf("Leave, config.num:%d, Shards:%v \n", newConfig.Num, newConfig.Shards)
	entry := Op{NewConfig:newConfig}
	sm.AppendConfigToLog(entry)
	reply.WrongLeader = false
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if sm.rf.IsLeader() == false{
		reply.WrongLeader = true
		return
	}

	sm.clientMu.Lock()
	defer sm.clientMu.Unlock()
	latestConfig := sm.configs[len(sm.configs) - 1]
	var newConfig Config
	GetNewConfig(latestConfig, &newConfig)
	newConfig.Num++

	oldGid := newConfig.Shards[args.Shard]
	newConfig.Shards[args.Shard] = args.GID
	remain := false
	for _, gid := range newConfig.Shards{
		if gid == oldGid{
			remain = true
			break
		}
	}
	if !remain{
		delete(newConfig.Groups, oldGid)
	}

	//sm.configs = append(sm.configs, newConfig)
	entry := Op{NewConfig:newConfig}
	sm.AppendConfigToLog(entry)
	reply.WrongLeader = false
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	//fmt.Printf("Query Me:%d\n", sm.me)
	if sm.rf.IsLeader() == false {
		reply.WrongLeader = true
		return
	}

	if args.Num == -1 || args.Num >= len(sm.configs){
		reply.Config = sm.configs[len(sm.configs) - 1]
	}else{
		//fmt.Printf("Query Num:%d\n", args.Num)
		//fmt.Printf("Query group len:%d \n", len(sm.configs[args.Num].Groups))
		//for key,value := range(sm.configs[args.Num].Groups){
			//fmt.Printf("Query group gid:%d servers:%v \n", key, value)
		//}
		reply.Config = sm.configs[args.Num]
	}
	//fmt.Printf("me:%d, Length of configs:%d \n", sm.me, len(sm.configs))
	reply.WrongLeader = false
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	//fmt.Printf("StartServer  id:%d\n", me)
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.empty = true
	sm.configs[0].Num = 0
	sm.result = map[int]chan Op{}

	go func() {
		for {
				msg := <-sm.applyCh
				//fmt.Printf("Me:%d, apply msg:%v\n", sm.me, msg)
				if msg.Command == nil{
					//fmt.Printf("Me:%d, apply nil msg:%v\n", sm.me, msg)
					continue
				}
				op := msg.Command.(Op)
				//fmt.Printf("Me:%d, apply op:%v\n", sm.me, op)
				//sm.mu.Lock()
				sm.Apply(op)
				sm.mu.Lock()
				ch,ok := sm.result[msg.Index]
				if ok {
					select {
						case <-sm.result[msg.Index]:
						default:
					}
					ch <- op
				} else {
					if sm.rf.IsLeader(){
						sm.result[msg.Index] = make(chan Op, 1)
						//fmt.Printf("Going to send message, message.index:%d, me:%d\n", msg.Index, kv.me)
						sm.result[msg.Index] <- op
						//fmt.Printf("Send message done, message.index:%d, me:%d\n", msg.Index, kv.me)
					}
				}
				sm.mu.Unlock()
			}
		}()

		sm.rf.ReApply(0) 

	return sm
}
