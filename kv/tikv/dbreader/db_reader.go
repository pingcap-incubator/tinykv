package dbreader

import (
	"github.com/coocood/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

type DBReader interface {
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) engine_util.DBIterator
	Close()
}

type RegionReader struct {
	txn    *badger.Txn
	region *metapb.Region
}

func NewRegionReader(txn *badger.Txn, region metapb.Region) *RegionReader {
	return &RegionReader{
		txn:    txn,
		region: &region,
	}
}

func (r *RegionReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(r.txn, cf, key)
}

func (r *RegionReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *RegionReader) Close() {
	r.txn.Discard()
}

type BadgerReader struct {
	txn *badger.Txn
}

func NewBadgerReader(txn *badger.Txn) *BadgerReader {
	return &BadgerReader{txn}
}

func (b *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(b.txn, cf, key)
}

func (b *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, b.txn)
}

func (b *BadgerReader) Close() {
	b.txn.Discard()
}
