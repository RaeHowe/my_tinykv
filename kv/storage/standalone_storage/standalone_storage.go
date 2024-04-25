package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	kvDB *badger.DB
}

type StandAloneStorageReader struct {
	//事务对象
	txn *badger.Txn
}

/*
GetCF(cf string, key []byte) ([]byte, error)
IterCF(cf string) engine_util.DBIterator
Close()
*/
func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return nil, nil
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return nil
}

func (s *StandAloneStorageReader) Close() {

}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		kvDB: engine_util.CreateDB(conf.DBPath, conf.Raft),
	}
}

func (s *StandAloneStorage) Start() error {
	/*
		todo: 一些初始化的操作
	**/
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

// Reader 读取，只读操作（方法只返回个reader就行，调用该方法的逻辑会去进行真正的读取操作）
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.kvDB.NewTransaction(false)

	var reader = &StandAloneStorageReader{
		txn: txn,
	}

	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	writeBatch := new(engine_util.WriteBatch) //rocksdb里面的writeBatch
	for _, x := range batch {                 //写入一批数据
		switch x.Data.(type) { //判断数据类型（数据里面带有了对rocksdb的操作类型）
		case storage.Put: //如果是对rocksdb的PUT操作（新增和修改）
			writeBatch.SetCF(x.Cf(), x.Key(), x.Value()) //往rocksdb的memtable 列簇中写kv数据
		case storage.Delete: //如果是对rocksdb的DELETE操作（删除）
			writeBatch.DeleteCF(x.Cf(), x.Key()) //从列簇中删key
		}
	}

	//写完memtable的cf之后写库（简化了memtable flush to sst的过程）
	return writeBatch.WriteToDB(s.kvDB)
}
