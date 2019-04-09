package models

import (
	"fmt"
	"os"
	"time"

	"github.com/go-xorm/xorm"
)


type MysqlLocks struct {
	Id                    int    `json:"id" xorm:"pk autoincr notnull "`
	HostName              string `json:"host_name" xorm:"varchar(64) not null"`
	Status                Status // 0 告警消除   1 警告
	RequestMysqlThreadId  int64
	RequestCommand        string `json:"request_command" xorm:"text"`
	BlockingMysqlThreadId int64
	BlockingCommand       string    `json:"blocking_command" xorm:"text"`
	LockIndex             string    `json:"lock_index" xorm:"varchar(256)"`
	CreateTime            time.Time `json:"create_time" xorm:"datetime notnull created"`
}



func (m *MysqlLocks)WriteLogToFile( content string,f *os.File) (err error) {
	//fd,_:=os.OpenFile(fileName,os.O_RDWR|os.O_CREATE|os.O_APPEND,0644)
	buf:=[]byte(content)
	_,err = f.Write(buf)
	return
}





type MysqlHostLists struct {
	Id       int    `json:"id" xorm:"pk autoincr notnull "`
	Host     string `json:"host" xorm:"varchar(64) not null"`
	Port     int
	User     string `json:"host" xorm:"varchar(64) not null"`
	Password string `json:"host" xorm:"varchar(64) not null"`
}

func (m *MysqlLocks) Add() (err error) {
	_, err = Db.Insert(m)
	return
}

func (m *MysqlLocks) GetLock(rID, bID int64, host string) (result []MysqlLocks, err error) {
	// Db.Query(fmt.Sprintf("select id from"))
	err = Db.Where("host_name = ? and request_mysql_thread_id = ? and blocking_mysql_thread_id = ? and status = 1", host, rID, bID).Find(&result)
	return
}

func (m *MysqlLocks) ChangeLockStatus(host string) (affectRows int64, err error) {
	affectRows, err = Db.Cols("status").Where("host_name=? and status = 1", host).Update(m)
	return
}

func (m *MysqlLocks) GetLocksByHost(host string) (result []MysqlLocks, err error) {
	err = Db.Where("host_name = ? and  status = 1", host).Find(&result)
	return
}

func (mh *MysqlHostLists) Add() (err error) {
	_, err = Db.Insert(mh)
	return
}

func (mh *MysqlHostLists) GetHosts() (hosts []MysqlHostLists, err error) {
	err = Db.Find(&hosts)
	return
}

func (m *MysqlLocks) DetecMysqlLocks(host, user, password string, port int) (resultsSlice []map[string][]byte, err error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
		user,
		password,
		host,
		port,
		"information_schema",
		"utf8",
	)
	engine, err := xorm.NewEngine("mysql", dsn)
	sql := `
		SELECT
		lw.requesting_trx_id AS request_ID,
	trx.trx_mysql_thread_id as request_mysql_ID,
		trx.trx_query AS request_command,
		lw.blocking_trx_id AS blocking_ID,
	trx1.trx_mysql_thread_id as blocking_mysql_ID,
		trx1.trx_query AS blocking_command,
		lo.lock_index AS lock_index
	FROM
		information_schema.innodb_lock_waits lw
	INNER JOIN information_schema.innodb_locks lo ON lw.requesting_trx_id = lo.lock_trx_id
	INNER JOIN information_schema.innodb_locks lo1 ON lw.blocking_trx_id = lo1.lock_trx_id
	INNER JOIN information_schema.innodb_trx trx ON lo.lock_trx_id = trx.trx_id
	INNER JOIN information_schema.innodb_trx trx1 ON lo1.lock_trx_id = trx1.trx_id
	`
	result, err := engine.Query(sql)
	engine.Close()
	return result, err

}
