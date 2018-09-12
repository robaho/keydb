package structkeys

import (
	"github.com/robaho/keydb"
	. "github.com/robaho/keydb/_examples/structkeys"
	"time"
)

type MyKeyTx struct {
	tx *keydb.Transaction
}

func (mktx *MyKeyTx) Get(key MyKey) (string, error) {
	_key, err := key.MarshalBinary()
	if err != nil {
		return "", err
	}

	val, err := mktx.tx.Get(_key)
	return string(val), err
}
func (mktx *MyKeyTx) Put(key MyKey, value string) error {
	_key, err := key.MarshalBinary()
	if err != nil {
		return err
	}
	return mktx.tx.Put(_key, []byte(value))
}

func main() {

	path := "test/structkeys"

	keydb.Remove(path)
	db, err := keydb.Open(path, true)
	if err != nil {
		panic(err)
	}

	tx, err := db.BeginTX("main")
	if err != nil {
		panic(err)
	}

	mktx := MyKeyTx{tx}

	a := MyKey{"ibm", time.Now()}
	b := MyKey{"aapl", time.Now()}

	mktx.Put(a, "some value for a")
	mktx.Put(b, "some value for b")

	tx.Commit()

	tx, err = db.BeginTX("main")
	if err != nil {
		panic(err)
	}

	mktx = MyKeyTx{tx}

	value, err := mktx.Get(a)
	if value != "some value for a" {
		panic("wrong a value")
	}
	value, err = mktx.Get(b)
	if value != "some value for b" {
		panic("wrong a value")
	}

	tx.Commit()
	db.Close()
}
