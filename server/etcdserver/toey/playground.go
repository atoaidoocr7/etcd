package toey

import pb "go.etcd.io/etcd/api/v3/etcdserverpb"

type KVToey struct {
	key   string
	value string
}

type RangeResponseToey struct {
	KVs []KVToey
}

func ConvertRangeResponse(r *pb.RangeResponse) RangeResponseToey {
	var response RangeResponseToey
	var kvsToey []KVToey
	kvs := r.GetKvs()
	for _, kv := range kvs {
		kvsToey = append(kvsToey, KVToey{key: string(kv.Key), value: string(kv.Value)})
	}
	response.KVs = kvsToey
	return response
}
