package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "shardmaster"
import "time"
import "bytes"
import "fmt"

const(
	TRYTIME = 1000
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Kind string //"Put" or "Append" or "Get" or "Transfer" or "Finish"
	Key string
	Value string
	ShardId int
	Id int64
	ReqId int
	ConfigNum int
	Config shardmaster.Config
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck			 *shardmaster.Clerk
	config       shardmaster.Config
	//futureConfig shardmaster.Config
	inTransfer	 bool
	inHang		 bool
	transfer	 [shardmaster.NShards]bool
	//transferMu   [shardmaster.NShards]sync.Mutex
	//applyCountMu [shardmaster.NShards]sync.Mutex
	//applyCount	 [shardmaster.NShards]int
	shardsHang	 [shardmaster.NShards]bool 
	//ava		 	 bool
	ack 		 map[int64]int
	result		 map[int]chan Err
	db 			 map[int]map[string]string
	buffer 		 map[int]map[int]map[string]string
	bufferFinish map[int]map[int]bool
	killCh		 chan bool
	transFinish  chan bool
}

func (kv *ShardKV) SendTransferRpc(config shardmaster.Config, shard int, op string, key string, value string, CId int64, ReqId *int){
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ConfigNum = kv.config.Num
	args.ShardId = shard
	args.Id = CId
	//fmt.Printf("SendTransferRpc CId:%d\n", CId)
	//args.ReqID = ReqId

	gid := config.Shards[shard]
	servers := config.Groups[gid]
	fmt.Printf("SendTransferRpc CId:%d, gid:%d, shard:%d, config.num:%d, op:%s\n", CId, gid, shard, config.Num, op)

	for tryTime := 0; tryTime < TRYTIME; tryTime++{
		success := false
		args.ReqID = *ReqId
		*ReqId += 1
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply PutAppendReply
			ok := srv.Call("ShardKV.PutAppend", &args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				success = true
				break
			}
			if ok && reply.Err == ErrWrongGroup {
				//fmt.Printf("Transfer return ErrWrongGroup, gid:%d, id:%d, op:%s, \n", gid, CId, op)
				if reply.ServerNum > kv.config.Num{
					success = true
				}
				break
			}
		}

		if success{
			break
		}else{
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) GetTransferShardsSet(lackConfig shardmaster.Config, surplusConfig shardmaster.Config, gid int) map[int]bool{
	shardsSet := make(map[int]bool)
	for index, cgid := range surplusConfig.Shards{
		if cgid == gid{
			shardsSet[index] = true
		}
	}
	for index, cgid := range lackConfig.Shards{
		if cgid == gid{
			delete(shardsSet, index)
		}
	}
	return shardsSet
}

func (kv *ShardKV) Transfer(shardsSet map[int]bool, config shardmaster.Config){
	//kv.inTransfer = true
	CId := nrand()
	ReqId := 0
	for shard, _ := range shardsSet{
		for key, value := range kv.db[shard]{
			//Transfer every key-value to new server
			kv.SendTransferRpc(config, shard, "Transfer", key, value, CId, &ReqId)
			//ReqId += 1
		}

		kv.SendTransferRpc(config, shard, "Finish", "", "",  CId, &ReqId)
		entry := Op{Kind:"TransferFinish", ShardId:shard, ConfigNum:kv.config.Num, Id:CId, ReqId:ReqId}
		kv.AppendEntryToLog(entry)
		//delete(kv.db, shard)
	}
	kv.inTransfer = false
	kv.mu.Lock()
	kv.transFinish <- true
	kv.mu.Unlock()
}

/*
func (kv *ShardKV) UpdateConfig(newConfigNum int){
	dstConfig = mck.Query(newConfigNum)
	if dstConfig.Num == config.Num{
		return
	}

	kv.mu.Lock()
	shardmaster.CopyConfig(dstConfig, &kv.futureConfig)
	kv.mu.Unlock()

	if rf.IsLeader(){
		if _,ok := dstConfig.Groups[kv.gid];ok{
			// this group still exist
			newSNum := kv.CountShards(dstConfig, kv.gid)
			currentSNum := kv.CountShards(kv.config, kv.gid)
			if newSNum > currentSNum{
				// need to serve more shards, wait transfer
				shardsSet := kv.GetTransferShardsSet(kv.config, dstConfig, kv.gid)
				for{
					compelete := true
					for shard, _ := range shardsSet{
						if _, ok := kv.transRes[shard]; !ok{
							compelete = false
							break
						}
					}
					if compelete{
						break
					}
				}
			}else if newSNum < currentSNum{
				// need to serve shards, transfer to new group
				shardsSet := kv.GetTransferShardsSet(dstConfig, kv.config, kv.gid)
				kv.Transfer(shardsSet, dstConfig)
			}
		}else{
			// this group has been deleted,send all shards
			shardsSet := map[int]bool{}
			for index, gid := range kv.config.Shards{
				if gid == kv.gid{
					shardsSet[index] = true
				}
			}
			kv.Transfer(shardsSet, dstConfig)
		}
	}

	kv.mu.Lock()
	shardmaster.CopyConfig(dstConfig, &kv.config)
	kv.transRes = map[int]bool{}
	kv.mu.Unlock()
}
*/

func (kv *ShardKV) UpdateConfig(newConfigNum int){
	fmt.Printf("Ready to UpdateConfig!!!, newConfigNum:%d, kv.config.num:%d, gid:%d\n", newConfigNum, kv.config.Num, kv.gid)
	dstConfig := kv.mck.Query(newConfigNum)
	if dstConfig.Num <= kv.config.Num{
		return
	}
	
	for kv.inTransfer {
		fmt.Printf("UpdateConfig in wait trasfer!!!, newConfigNum:%d, kv.config.num:%d, gid:%d\n", newConfigNum, kv.config.Num, kv.gid)
		select{
		case <- kv.transFinish:
			break
		case <- time.After(100 * time.Millisecond):
			break
		}
	}

	kv.inTransfer = false
	kv.mu.Lock()
	kv.transFinish = make(chan bool, 5)
	kv.mu.Unlock()

	for index := 0; index < shardmaster.NShards; index ++{
		if kv.config.Shards[index] != kv.gid{
			kv.mu.Lock()
			//kv.db[index] = make(map[string]string)
			delete(kv.db, index)
			kv.mu.Unlock()
		}
	}

	if dstConfig.Num > 1{
		lackShardsSet := kv.GetTransferShardsSet(kv.config, dstConfig, kv.gid)
		for shard, _ := range lackShardsSet{
			fmt.Printf("shardsHang gid:%d, shard:%d, me:%d, config.Num:%d\n", kv.gid, shard, kv.me, kv.config.Num)
			kv.shardsHang[shard] = true
			kv.inHang = true
		}

		if configBuffer, ok := kv.buffer[dstConfig.Num]; ok{
			fmt.Printf("UpdateConfig use buffer!!!!!!!!\n")
			for shard, shardBuffer := range configBuffer{
				kv.mu.Lock()
				//kv.db[shard] = shardBuffer
				kv.db[shard] = make(map[string]string)
				for key, value := range shardBuffer{
					kv.db[shard][key] = value
				} 
				kv.mu.Unlock()
				if _, ok2 := kv.bufferFinish[dstConfig.Num][shard]; ok2{
					kv.shardsHang[shard] = false
				}
			}
			kv.CheckHang()
			delete(kv.buffer, dstConfig.Num)
			delete(kv.bufferFinish, dstConfig.Num)
		}
	}

	surplusShardsSet := kv.GetTransferShardsSet(dstConfig, kv.config, kv.gid)
	for shard, _ := range surplusShardsSet{
		kv.transfer[shard] = true
	}


	kv.mu.Lock()
	shardmaster.CopyConfig(dstConfig, &kv.config)
	fmt.Printf("Update config complete, config.num:%d\n", kv.config.Num)
	kv.mu.Unlock()

	if kv.rf.IsLeader() && len(surplusShardsSet) > 0{
		fmt.Printf("Going to inTransfer2, gid:%d, config.num:%d\n", kv.gid, kv.config.Num)
		kv.inTransfer = true
		go kv.Transfer(surplusShardsSet, dstConfig)
	}
}
/*
func (kv *ShardKV) AppendEntryToLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}

	kv.mu.Lock()
	ch,ok := kv.result[index]
	if !ok {
		ch = make(chan Op,1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()
	select {
	case op := <-ch:
		return op == entry
	case <-time.After(1000 * time.Millisecond):
		//log.Printf("timeout\n")
		return false
	}
}
*/

func (kv *ShardKV) AppendEntryToLog(entry Op) int{
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return -1
	}
	return index
}

func (kv *ShardKV) AppendAndWaitApply(entry Op) (bool, Err){
	index := kv.AppendEntryToLog(entry)
	if index < 0{
		return true, OK
	}

	kv.mu.Lock()
	ch, ok := kv.result[index]
	if !ok {
		ch = make(chan Err,1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()
	select {
	case err := <-ch:
		return false, err
	case <-time.After(1000 * time.Millisecond):
		//log.Printf("timeout\n")
		return false, ErrNoKey
	}

}

func (kv *ShardKV) CheckDup(id int64,reqid int) bool {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	v,ok := kv.ack[id]
	if ok {
		return v >= reqid
	}
	return false
}

/*
// Not in Transfer: client's config must equal kv's config
// In Transfer: client 's config must equal kv's futureConfig and shard required must in new config
func (kv *ShardKV) NoValidClientReq(clientConfigNum int, shard int, kind string){
	if clientConfigNum < kv.config.Num || (kv.inTransfer && clientConfigNum < kv.futureConfig.Num){
		return true
	}

	if kv.inTransfer && kv.gid != kv.futureConfig.Shards[shard]{
		return true
	}

	if kv.shardsHang[shard] && kind != "Transfer" && kind != "Finish"{
		return true
	}

	return false
}
*/

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	/*
	if kv.NoValidClientReq(args.ConfgiNum, args.ShardId, args.Kind){
		reply.Err = ErrWrongGroup
		return
	}
	*/

	entry := Op{Kind:"Get",Key:args.Key,Id:args.Id,ReqId:args.ReqID,ShardId:args.ShardId,ConfigNum:args.ConfigNum}
	reply.WrongLeader, reply.Err = kv.AppendAndWaitApply(entry)

	if reply.Err == OK{
		kv.mu.Lock()
		reply.Value = kv.db[args.ShardId][args.Key]
		//kv.ack[args.Id] = args.ReqID
		//log.Printf("%d get:%v value:%s\n",kv.me,entry,reply.Value)
		kv.mu.Unlock()
	}
	/*
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false

		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[args.ShardId][args.Key]
		kv.ack[args.Id] = args.ReqID
		//log.Printf("%d get:%v value:%s\n",kv.me,entry,reply.Value)
		kv.mu.Unlock()
	}
	*/
}

func (kv *ShardKV) Ack(args Op){
	kv.ack[args.Id] = args.ReqId
}

func (kv *ShardKV) CheckHang(){
	fmt.Printf("CheckHang, gid:%d, shardsHang[shard]:%v\n", kv.gid, kv.shardsHang)
	allReady := true
	for shard := 0; shard < shardmaster.NShards; shard++{
		if kv.shardsHang[shard]{
			allReady = false
		}
	}
	if allReady{
		fmt.Printf("CheckHang set false gid:%d\n", kv.gid)
		kv.inHang = false
	}
}

func (kv *ShardKV) Apply(args Op) Err{
	//defer kv.Ack(args)
	var res Err
	res = OK
	switch args.Kind {
	case "Get":
		if args.ConfigNum == kv.config.Num && kv.shardsHang[args.ShardId] == false {
			res = OK
		}else{
			res =  ErrWrongGroup
		}
	case "Put":
		if args.ConfigNum == kv.config.Num && kv.shardsHang[args.ShardId] == false && kv.transfer[args.ShardId] == false{
			kv.mu.Lock()
			if mdb, ok := kv.db[args.ShardId]; ok{
				mdb[args.Key] = args.Value
			}else{
				kv.db[args.ShardId] = make(map[string]string)
				kv.db[args.ShardId][args.Key] = args.Value
			}
			kv.mu.Unlock()
			res =  OK
		}else{
			//fmt.Printf("PUT ErrWrongGroup !!!")
			res =  ErrWrongGroup
		}

	case "Append":
		//fmt.Printf("Apply Append, key:%s, value:%s\n", args.Key, args.Value)
		if args.ConfigNum == kv.config.Num && kv.shardsHang[args.ShardId] == false && kv.transfer[args.ShardId] == false{
			kv.mu.Lock()
			fmt.Printf("Apply Append OK, key:%s, value:%s, gid:%d, shard:%d, config.num:%d\n", args.Key, args.Value, kv.gid, args.ShardId, kv.config.Num)
			if mdb, ok := kv.db[args.ShardId]; ok{
				mdb[args.Key] += args.Value
			}else{
				kv.db[args.ShardId] = make(map[string]string)
				kv.db[args.ShardId][args.Key] = args.Value
			}
			fmt.Printf("After Append, value:%s\n", kv.db[args.ShardId][args.Key])
			kv.mu.Unlock()
			res =  OK
		}else{
			fmt.Printf("Append ErrWrongGroup!!\n")
			res =  ErrWrongGroup
		}

	case "Transfer":
		if kv.rf.IsLeader(){
			fmt.Printf("Apply Transfer, gid:%d, kv.config.Num:%d, args.ConfigNum:%d, shard:%d\n", kv.gid, kv.config.Num, args.ConfigNum, args.ShardId)
		}
		if args.ConfigNum == kv.config.Num{
			if kv.shardsHang[args.ShardId]{
				kv.mu.Lock()
				if mdb, ok := kv.db[args.ShardId]; ok{
					mdb[args.Key] = args.Value
				}else{
					kv.db[args.ShardId] = make(map[string]string)
					kv.db[args.ShardId][args.Key] = args.Value
				}
				kv.mu.Unlock()
				res = OK
			}else{
				res = ErrWrongGroup
			}
		}else if args.ConfigNum > kv.config.Num{
			if _, ok := kv.buffer[args.ConfigNum]; !ok{
				kv.buffer[args.ConfigNum] = make(map[int]map[string]string)
			}

			if _, ok := kv.buffer[args.ConfigNum][args.ShardId]; !ok{
				kv.buffer[args.ConfigNum][args.ShardId] = make(map[string]string)
			}

			kv.buffer[args.ConfigNum][args.ShardId][args.Key] = args.Value
			res = OK
		}else{
			res = ErrWrongGroup
		}

	case "Finish":
		if kv.rf.IsLeader(){
			fmt.Printf("Apply TransferFinish, gid:%d, kv.config.Num:%d, args.ConfigNum:%d, shard:%d\n", kv.gid, kv.config.Num, args.ConfigNum, args.ShardId)
		}
		if args.ConfigNum == kv.config.Num{
			if kv.shardsHang[args.ShardId]{
				kv.shardsHang[args.ShardId] = false
				kv.CheckHang()
				res = OK
			}else{
				res = ErrWrongGroup
			}
		}else if args.ConfigNum > kv.config.Num{
			if _, ok := kv.bufferFinish[args.ConfigNum]; !ok{
				kv.bufferFinish[args.ConfigNum] = make(map[int]bool)
			}
			kv.bufferFinish[args.ConfigNum][args.ShardId] = true
			res = OK
		}else{
			res = ErrWrongGroup
		}
	case "TransferFinish":
		if args.ConfigNum == kv.config.Num{
		kv.transfer[args.ShardId] = false
			kv.mu.Lock()
			//delete(kv.db, args.ShardId)
			kv.mu.Unlock()
		}
		res = OK
	}
	//kv.ack[args.Id] = args.ReqId
	if res == OK{
		kv.Ack(args)
	}
	return res
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{Kind:args.Op,Key:args.Key,Value:args.Value,Id:args.Id,ReqId:args.ReqID,ShardId:args.ShardId,ConfigNum:args.ConfigNum}
	if args.Op == "Put" || args.Op == "Append"{
		reply.WrongLeader, reply.Err = kv.AppendAndWaitApply(entry)
		reply.ServerNum = kv.config.Num
	}else{
		index := kv.AppendEntryToLog(entry)
		reply.WrongLeader = index < 0
		if args.ConfigNum < kv.config.Num {
			reply.Err = ErrWrongGroup
			reply.ServerNum = kv.config.Num
		}else{
			reply.Err = OK
		}
		if reply.WrongLeader == false{
			fmt.Printf("Transfer Server op:%s, gid:%d, kv.config.Num:%d, args.ConfigNum:%d\n", args.Op, kv.gid, kv.config.Num, args.ConfigNum)
		}
	}
	/*
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK

		kv.applyCountMu[args.ShardId].Lock()
		if kv.applyCount[args.ShardId] == 0{	
			kv.transferMu[args.ShardId].Lock()
		}
		kv.applyCount[args.ShardId]++
		kv.applyCountMu[args.ShardId].Unlock()
	}
	
	if reply.WrongLeader == false{
		fmt.Printf("Put ret:me:%d, req_id:%d, err:%s\n", kv.me, args.ReqID, reply.Err)
	}
	*/
}

func (kv *ShardKV) CheckTransfer(){
	if kv.rf.IsLeader() {
		transferShardsSet := map[int]bool{}
		for shard, _ := range kv.config.Shards{
			if kv.transfer[shard]{
				transferShardsSet[shard] = true
			}
		}

		if len(transferShardsSet) > 0{
			fmt.Printf("Going to inTransfer2, gid:%d, config.num:%d\n", kv.gid, kv.config.Num)
			kv.inTransfer = true
			go kv.Transfer(transferShardsSet, kv.config)
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	//kv.mu.Lock()
	//kv.stop = true
	kv.killCh <- true
	//time.Sleep(200 * time.Millisecond)
	//kv.mu.Unlock()
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	//kv.transRes = make(map[int]bool)
	kv.db = make(map[int]map[string]string)
	kv.ack = make(map[int64]int)
	kv.buffer = make(map[int]map[int]map[string]string)
	kv.bufferFinish = make(map[int]map[int]bool)
	kv.result = make(map[int]chan Err)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.killCh = make(chan bool, 1)
	kv.transFinish = make(chan bool, 5)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.mck = shardmaster.MakeClerk(kv.masters)
	//kv.config = kv.mck.Query(-1)
	kv.config.Num = 0
	kv.config.Groups = map[int][]string{}
	for {
		latestConfig := kv.mck.Query(-1)
		if kv.config.Num == latestConfig.Num{
			break
		}
		if _, ok := kv.config.Groups[gid]; ok{
			break
		}else{
			kv.config = kv.mck.Query(kv.config.Num + 1)
		}
		fmt.Printf("Start Server config.num:%d\n", kv.config.Num)
	}


	kv.inTransfer = false
	kv.inHang = false

	for shard := 0; shard < shardmaster.NShards; shard++{
		//kv.shardsHang[shard] = kv.config.Shards[shard] == kv.gid && kv.config.Num > 1
		//kv.inHang = kv.inHang && kv.shardsHang[shard]
		kv.shardsHang[shard] = false
		kv.transfer[shard] = false
	}

	fmt.Printf("Start kv server, gid:%d, me:%d, config.num:%d \n, ack:%v\n", gid, kv.me, kv.config.Num, kv.ack)

	//Start Listenning apply channel
	go func() {
		//fmt.Printf("Listenning apply channel, ack:%v\n", kv.ack)
		for {
			msg := <-kv.applyCh
			//fmt.Printf("Receive msg, Index:%d\n", msg.Index)
			if msg.UseSnapshot {
				fmt.Printf("Using Snapshot!!!!!!\n")
				var LastIncludedIndex int
				var LastIncludedTerm int

				r := bytes.NewBuffer(msg.Snapshot)
				d := gob.NewDecoder(r)

				kv.mu.Lock()
				d.Decode(&LastIncludedIndex)
				d.Decode(&LastIncludedTerm)
				kv.db = make(map[int]map[string]string)
				kv.ack = make(map[int64]int)
				d.Decode(&kv.db)
				d.Decode(&kv.ack)
				d.Decode(&kv.config.Num)
				d.Decode(&kv.config.Shards)
				d.Decode(&kv.config.Groups)
				//d.Decode(&kv.inTransfer)
				d.Decode(&kv.inHang)
				d.Decode(&kv.transfer)
				d.Decode(&kv.shardsHang)
				kv.buffer = make(map[int]map[int]map[string]string)
				kv.bufferFinish = make(map[int]map[int]bool)
				d.Decode(&kv.buffer)
				d.Decode(&kv.bufferFinish)
				fmt.Printf("Snapshot num:%d, gid:%d, inTransfer:%t, inHang:%t, isLeader:%t!!!!!!\n", kv.config.Num, kv.gid, kv.inTransfer, kv.inHang, kv.rf.IsLeader())
				kv.CheckTransfer()
				kv.mu.Unlock()
			} else {
				op := msg.Command.(Op)
				//kv.mu.Lock()
				var res Err
				if op.Kind == "UpdateConfig"{
					kv.UpdateConfig(op.Config.Num)
					res = OK
					//fmt.Printf("UpdateConfig, ack:%v\n", kv.ack)
				}else {
					//fmt.Printf("config num right, kv.config.Num:%d\n", kv.config.Num)
					if !kv.CheckDup(op.Id, op.ReqId) {
						//fmt.Printf("Client Req no Dup, Id:%d, ReqId:%d, Kind:%s\n ack:%v\n", op.Id, op.ReqId, op.Kind, kv.ack)
					//if true {
						//if kv.rf.IsLeader() == false{
							//time.Sleep(time.Second * 1)
						//}
							//if kv.rf.IsLeader(){
								res = kv.Apply(op)
							//}
					}else{
						//fmt.Printf("Client Req Dup, Id:%d, ReqId:%d, Kind:%s\n ack:%v\n", op.Id, op.ReqId, op.Kind, kv.ack)
						res = OK
					}
				}

				kv.mu.Lock()
				ch, ok := kv.result[msg.Index]
				if ok {
					select {
						case <-kv.result[msg.Index]:
						default:
					}
					ch <- res
				} else {
					if kv.rf.IsLeader(){
						kv.result[msg.Index] = make(chan Err, 1)
						//fmt.Printf("Going to send message, message.index:%d, me:%d\n", msg.Index, kv.me)
						kv.result[msg.Index] <- res
						//fmt.Printf("Send message done, message.index:%d, me:%d\n", msg.Index, kv.me)
					}
				}

				//need snapshot
				if maxraftstate != -1 && kv.rf.GetPerisistSize() > maxraftstate {
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.db)
					e.Encode(kv.ack)
					e.Encode(kv.config.Num)
					e.Encode(kv.config.Shards)
					e.Encode(kv.config.Groups)
					//e.Encode(kv.inTransfer)
					e.Encode(kv.inHang)
					e.Encode(kv.transfer)
					e.Encode(kv.shardsHang)
					e.Encode(kv.buffer)
					e.Encode(kv.bufferFinish)
					data := w.Bytes()
					go kv.rf.StartSnapshot(data,msg.Index)
				}
				kv.mu.Unlock()
			}
		}
	}()

	go func() {
		GoId := nrand()
		for{
			if kv.rf.IsLeader(){
				kv.CheckTransfer()
			}

			if kv.rf.IsLeader() && (!kv.inTransfer) && (!kv.inHang){
				//fmt.Printf("Check Config need update!!\n")
				latestConfig := kv.mck.Query(-1)
				if latestConfig.Num != kv.config.Num{
					var appendConfig shardmaster.Config
					//if kv.config.Num == 0{
						//appendConfig = latestConfig
					//}else{
						appendConfig = kv.mck.Query(kv.config.Num + 1)
					//}
					fmt.Printf("Update Config!!, appendConfig.num:%d\n", appendConfig.Num)
					entry := Op{Kind:"UpdateConfig", Config:appendConfig}
					//fmt.Printf("Update Config!!\n")
					kv.AppendEntryToLog(entry)
				}
			}

			select {
			case <-kv.killCh:
				fmt.Printf("Update Kill, gid:%d, config.num:%d\n", kv.gid, kv.config.Num)
				return
			case <-time.After(100 * time.Millisecond):
				fmt.Printf("Update thread wait kill timeout\n")
			}

			//time.Sleep(100 * time.Millisecond)
			if kv.rf.IsLeader(){
				fmt.Printf("Update thread wake!!, GoId:%d, gid:%d, config.num:%d, me:%d, kv.inTransfer:%t, kv.inHang:%t\n", GoId, kv.gid, kv.config.Num, kv.me, kv.inTransfer, kv.inHang)
			}
		}
	}()


	return kv
}
