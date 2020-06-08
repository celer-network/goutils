// Copyright 2019-2020 Celer Network
//
// This test starts an instance of CockroachDB and terminates it at the end.
// That instance uses non-default port numbers and a /tmp storage directory.
// It also creates temporary SQLite DB files.  Each unittest is run twice,
// once using CockroachDB and once using SQLite.

package sqldb

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

const (
	driverPG = "postgres"
	driverLT = "sqlite3"
	dbSvr    = "localhost:5555"
	dbWebSvr = "localhost:5566"
	dbInfoPG = "postgresql://celer@" + dbSvr + "/celertest?sslmode=disable"
	dbDirPG  = "/tmp/sqldb_test"
)

var (
	ErrDelNotFound = errors.New("delete did not find the row")
)

// TestMain is used to setup/teardown a temporary CockroachDB instance
// and run all the unit tests in between.
func TestMain(m *testing.M) {
	flag.Parse()

	if err := setupDB(); err != nil {
		fmt.Println("cannot setup DB:", err)
		os.Exit(1)
	}

	exitCode := m.Run() // run all unittests

	teardownDB()
	os.Exit(exitCode)
}

func setupDB() error {
	err := os.RemoveAll(dbDirPG)
	if err != nil {
		return fmt.Errorf("cannot remove old DB directory: %s: %s", dbDirPG, err)
	}

	cmd := exec.Command("cockroach", "start", "--insecure",
		"--listen-addr="+dbSvr, "--http-addr="+dbWebSvr,
		"--store=path="+dbDirPG)
	if err = cmd.Start(); err != nil {
		return fmt.Errorf("cannot start DB: %s", err)
	}

	time.Sleep(time.Second)

	sqlInitCmds := []string{
		"CREATE DATABASE IF NOT EXISTS celertest",
		"CREATE USER IF NOT EXISTS celer",
		"GRANT ALL ON DATABASE celertest TO celer",
		"SET DATABASE TO celertest",
	}

	var out []byte
	for _, initCmd := range sqlInitCmds {
		cmd = exec.Command("cockroach", "sql", "--insecure",
			"--host="+dbSvr, "-e", initCmd)
		if out, err = cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("cannot init DB: %s: %v (%s)", initCmd, err, string(out))
		}
	}

	time.Sleep(time.Second)
	return nil
}

func teardownDB() {
	cmd := exec.Command("cockroach", "quit", "--insecure", "--host="+dbSvr)
	if err := cmd.Run(); err != nil {
		fmt.Printf("WARNING: cannot terminate DB: %s", err)
		return
	}

	time.Sleep(time.Second)
	os.RemoveAll(dbDirPG)
}

func resetDB(t *testing.T, db *Db) {
	sqlCmds := []string{
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (name TEXT PRIMARY KEY NOT NULL, count INT NOT NULL)",
	}

	for _, cmd := range sqlCmds {
		_, err := db.Exec(cmd)
		if err != nil {
			t.Fatalf("cannot reset DB: %s: %v", cmd, err)
		}
	}
}

// Return a temporary DB file for testing (without creating the file).
func tempDbFile() string {
	user, _ := user.Current()
	ts := time.Now().UnixNano() / 1000
	dir := fmt.Sprintf("sqldb-%s-%d.db", user.Username, ts)
	return filepath.Join(os.TempDir(), dir)
}

type TestFunc func(*testing.T, *Db)

// Create a DB (CockroachDB or SQLite) to run the test callback function.
func runWithDatabase(t *testing.T, sqlite bool, testCallback TestFunc) {
	var db *Db
	var err error

	if sqlite {
		dbFile := tempDbFile()
		db, err = NewDb(driverLT, dbFile, 0)
		if err != nil {
			t.Fatalf("cannot create SQLite DB %s: %v", dbFile, err)
		}
		defer db.Close()
		defer os.Remove(dbFile)
	} else {
		db, err = NewDb(driverPG, dbInfoPG, 4)
		if err != nil {
			t.Fatalf("cannot connect to SQL DB %s: %v", dbInfoPG, err)
		}
		defer db.Close()
	}

	resetDB(t, db)
	testCallback(t, db)
}

func insertName(s SqlStorage, name string, count int) error {
	q := "INSERT INTO foo (name, count) VALUES ($1, $2)"
	res, err := s.Exec(q, name, count)
	return ChkExec(res, err, 1, "insertName")
}

func deleteName(s SqlStorage, name string) error {
	q := "DELETE FROM foo WHERE name = $1"
	res, err := s.Exec(q, name)
	return ChkExecDiffError(res, err, ErrDelNotFound, 1)
}

func getCount(s SqlStorage, name string) (int, bool, error) {
	var count int
	q := "SELECT count FROM foo WHERE name = $1"
	err := s.QueryRow(q, name).Scan(&count)
	found, err := ChkQueryRow(err)
	return count, found, err
}

func updateCount(s SqlStorage, name string, count int) error {
	q := "UPDATE foo SET count = $1 WHERE name = $2"
	res, err := s.Exec(q, count, name)
	return ChkExec(res, err, 1, "updateCount")
}

func rowsToMap(rows *sql.Rows) (map[string]int, error) {
	names := make(map[string]int)

	if rows != nil {
		var name string
		var count int
		for rows.Next() {
			err := rows.Scan(&name, &count)
			if err != nil {
				return nil, err
			}
			names[name] = count
		}
	}

	return names, nil
}

func getAllNames(s SqlStorage) (map[string]int, error) {
	q := "SELECT name, count FROM foo"
	rows, err := s.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rowsToMap(rows)
}

func getSomeNames(s SqlStorage, names []string) (map[string]int, error) {
	if len(names) == 0 {
		return rowsToMap(nil)
	}

	var args []interface{}
	for _, name := range names {
		args = append(args, name)
	}

	q := "SELECT name, count FROM foo WHERE " + InClause("name", len(args), 1)
	rows, err := s.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rowsToMap(rows)
}

func expectAllNames(t *testing.T, s SqlStorage, exp map[string]int) {
	names, err := getAllNames(s)
	if err != nil {
		t.Errorf("getAllNames error: %v", err)
	} else if !reflect.DeepEqual(names, exp) {
		t.Errorf("getAllNames wrong: %v != %v", names, exp)
	}

	var some []string
	for name := range exp {
		some = append(some, name)
	}

	names, err = getSomeNames(s, some)
	if err != nil {
		t.Errorf("getSomeNames error: %v", err)
	} else if !reflect.DeepEqual(names, exp) {
		t.Errorf("getSomeNames wrong: %v != %v", names, exp)
	}
}

func testSimpleOps(t *testing.T, db *Db) {
	name := "foo"
	count, found, err := getCount(db, name)
	if err != nil || found {
		t.Errorf("getCount wrong name got: %d, %t, %v", count, found, err)
	}

	expNames := map[string]int{}
	expectAllNames(t, db, expNames)

	expNames = map[string]int{
		"foo":    1234,
		"bar":    5555,
		"qwerty": 98765432,
	}

	for n, c := range expNames {
		err = insertName(db, n, c)
		if err != nil {
			t.Errorf("insertName (%s) failed: %v", n, err)
		}
	}

	for n, c := range expNames {
		count, found, err = getCount(db, n)
		if err != nil || !found || count != c {
			t.Errorf("getCount (%s) error: %d, %t, %v (exp %d)", n, count, found, err, c)
		}
	}

	expectAllNames(t, db, expNames)

	newCount := 7777777
	err = updateCount(db, name, newCount)
	if err != nil {
		t.Errorf("updateCount error: %v", err)
	}

	count, found, err = getCount(db, name)
	if err != nil || !found || count != newCount {
		t.Errorf("getCount after update got: %d, %t, %v", count, found, err)
	}

	err = deleteName(db, name)
	if err != nil {
		t.Errorf("deleteName error: %v", err)
	}

	delete(expNames, name)
	expectAllNames(t, db, expNames)

	// Delete non-existing row should return the specific default error.
	err = deleteName(db, name)
	if err != ErrDelNotFound {
		t.Errorf("deleteName non-row wrong error: %v != %v", err, ErrDelNotFound)
	}

	// Update non-existing row should fail with a wrapped ErrNoRows error.
	err = updateCount(db, name, 1234567)
	if err == nil {
		t.Errorf("updateCount non-row did not fail")
	} else if !errors.Is(err, ErrNoRows) {
		t.Errorf("updateCount non-row wrong error, not wrap of ErrNoRows: %v", err)
	}
}

func TestSimpleOps_crdb(t *testing.T) {
	runWithDatabase(t, false, testSimpleOps)
}

func TestSimpleOps_sqlite(t *testing.T) {
	runWithDatabase(t, true, testSimpleOps)
}

func initFooBar(db *Db, fooCount, barCount int) error {
	if err := insertName(db, "foo", fooCount); err != nil {
		return err
	}
	return insertName(db, "bar", barCount)
}

func txUpdateFunc(tx *DbTx, args ...interface{}) error {
	fooCount := args[0].(int)
	barCount := args[1].(int)

	_, found, err := getCount(tx, "foo")
	if err != nil {
		return err
	} else if !found {
		return fmt.Errorf("txUpdateFunc getCount: foo not found")
	}

	_, found, err = getCount(tx, "bar")
	if err != nil {
		return err
	} else if !found {
		return fmt.Errorf("txUpdateFunc getCount: bar not found")
	}

	err = updateCount(tx, "foo", fooCount)
	if err != nil {
		return err
	}

	return updateCount(tx, "bar", barCount)
}

func testTransactional(t *testing.T, db *Db) {
	err := initFooBar(db, 10, 90)
	if err != nil {
		t.Errorf("cannot initialize foo & bar: %v", err)
	}

	err = db.Transactional(txUpdateFunc, 40, 60)
	if err != nil {
		t.Errorf("cannot make Tx update: %v", err)
	}

	expNames := map[string]int{
		"foo": 40,
		"bar": 60,
	}
	expectAllNames(t, db, expNames)
}

func TestTransactional_crdb(t *testing.T) {
	runWithDatabase(t, false, testTransactional)
}

func TestTransactional_sqlite(t *testing.T) {
	runWithDatabase(t, true, testTransactional)
}

func testCancelTransaction(t *testing.T, db *Db) {
	err := initFooBar(db, 10, 90)
	if err != nil {
		t.Errorf("cannot initialize foo & bar: %v", err)
	}

	tx, err := db.OpenTransaction()
	if err != nil {
		t.Errorf("cannot start transaction: %v", err)
	}

	err = txUpdateFunc(tx, 60, 40)
	if err != nil {
		t.Errorf("cannot make Tx update: %v", err)
	}
	err = insertName(tx, "baz", 555555)
	if err != nil {
		t.Errorf("cannot insert baz inside Tx: %v", err)
	}

	expNames := map[string]int{
		"foo": 60,
		"bar": 40,
		"baz": 555555,
	}

	expectAllNames(t, tx, expNames)

	err = deleteName(tx, "foo")
	if err != nil {
		t.Errorf("error deleting foo inside Tx: %v", err)
	}

	count, found, err := getCount(tx, "foo")
	if err != nil || found {
		t.Errorf("getCount error inside Tx after deleting foo: %d, %t, %v", count, found, err)
	}

	tx.Discard()

	expNames = map[string]int{
		"foo": 10,
		"bar": 90,
	}
	expectAllNames(t, db, expNames)
}

func TestCancelTransaction_crdb(t *testing.T) {
	runWithDatabase(t, false, testCancelTransaction)
}

func TestCancelTransaction_sqlite(t *testing.T) {
	runWithDatabase(t, true, testCancelTransaction)
}

func testTransactionOverlap(t *testing.T, db *Db) {
	err := initFooBar(db, 10, 90)
	if err != nil {
		t.Errorf("cannot initialize foo & bar: %v", err)
	}

	tx1, err := db.OpenTransaction()
	if err != nil {
		t.Errorf("cannot start 1st Tx: %v", err)
	}

	ch1to2 := make(chan int)
	ch2to1 := make(chan int)

	go func() {
		tx2, err2 := db.OpenTransaction()
		if err2 != nil {
			t.Errorf("cannot start 2nd Tx: %v", err2)
		}

		err = txUpdateFunc(tx2, 30, 70)
		if err != nil {
			t.Errorf("cannot make Tx2 update: %v", err)
		}

		<-ch1to2 // wait for Tx1 to commit

		if err = tx2.Commit(); err != nil {
			t.Errorf("cannot commit Tx2: %v", err)
		}

		close(ch2to1) // notify that Tx2 is committed
	}()

	err = txUpdateFunc(tx1, 20, 80)
	if err != nil {
		t.Errorf("cannot make Tx1 update: %v", err)
	}

	if err = tx1.Commit(); err != nil {
		t.Errorf("cannot commit Tx1: %v", err)
	}

	close(ch1to2) // notify that Tx1 is committed
	<-ch2to1      // wait for Tx2 to commit

	expNames := map[string]int{
		"foo": 30,
		"bar": 70,
	}
	expectAllNames(t, db, expNames)
}

func TestTransactionOverlap_crdb(t *testing.T) {
	runWithDatabase(t, false, testTransactionOverlap)
}

func TestTransactionOverlap_sqlite(t *testing.T) {
	runWithDatabase(t, true, testTransactionOverlap)
}

func testTransactionConflict(t *testing.T, db *Db) {
	err := initFooBar(db, 10, 90)
	if err != nil {
		t.Errorf("cannot initialize foo & bar: %v", err)
	}

	// Create 2 transactions.
	tx1, err := db.OpenTransaction()
	if err != nil {
		t.Errorf("cannot start 1st Tx: %v", err)
	}
	tx2, err := db.OpenTransaction()
	if err != nil {
		t.Errorf("cannot start 2nd Tx: %v", err)
	}

	// Both transactions get foo & bar.
	expNames := map[string]int{
		"foo": 10,
		"bar": 90,
	}
	expectAllNames(t, tx1, expNames)
	expectAllNames(t, tx2, expNames)

	// Both transactions update foo & bar and try to commit their changes.
	// Only one transaction should succeed and the other should fail.
	// Which one succeeds depends on the database implementation.
	err = txUpdateFunc(tx1, 20, 80)
	if err != nil {
		t.Errorf("cannot make Tx1 update: %v", err)
	}

	errTx1 := tx1.Commit()

	// Note: In CockroachDB the error maybe reported earlier at the
	// first conflicting Put instead of being delayed till the Commit.
	cancelTx2 := false
	if err = updateCount(tx2, "foo", 30); err != nil {
		err = tx2.ConvertError(err)
		if err != ErrTxConflict {
			t.Errorf("cannot update foo in Tx2: %v", err)
		} else {
			cancelTx2 = true
		}
	}

	errTx2 := ErrTxConflict
	if !cancelTx2 {
		if err = updateCount(tx2, "bar", 70); err != nil {
			t.Errorf("cannot update bar in Tx2: %v", err)
		}

		errTx2 = tx2.Commit()
	}

	var expFoo, expBar int
	if errTx1 == nil && errTx2 == nil {
		t.Errorf("conflict was not detected, both Tx passed")
	} else if errTx1 == nil && errTx2 == ErrTxConflict {
		expFoo, expBar = 20, 80
	} else if errTx1 == ErrTxConflict && errTx2 == nil {
		expFoo, expBar = 30, 70
	} else {
		t.Logf("both Tx failed: tx1 %v, tx2 %v", errTx1, errTx2)
	}

	tx1.Discard()
	tx2.Discard()

	// The foo & bar values should be those of the successful transaction.
	expNames = map[string]int{
		"foo": expFoo,
		"bar": expBar,
	}
	expectAllNames(t, db, expNames)
}

func TestTransactionConflict_crdb(t *testing.T) {
	runWithDatabase(t, false, testTransactionConflict)
}

// With a single SQLite connection configured, this test deadlocks because
// it is designed to force the need of concurrent progress on 2 transactions.
//func TestTransactionConflict_sqlite(t *testing.T) {
//	runWithDatabase(t, true, testTransactionConflict)
//}
