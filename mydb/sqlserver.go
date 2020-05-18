package mydb

import (
	"database/sql"
	"fmt"

	config "../myconfig"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

// Connection 数据库连接
type Connection struct {
	DB      *sql.DB
	ConnStr string
	opts    config.SQLConnOpt
}

// NewConnection 数据库连接
func NewConnection() *Connection {
	var db Connection
	db.SetConnStr(config.Cfg.Sqldb)
	return &db
}

// SetConnStr 设置数据库连接字符串
func (c *Connection) SetConnStr(opts config.SQLConnOpt) {
	c.opts = opts
	if opts.Type == "mssql" {
		c.ConnStr = fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;encrypt=disable",
			opts.Host, opts.User, opts.Password, opts.Port, opts.Database)
	} else if opts.Type == "mysql" {
		c.ConnStr = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", opts.User, opts.Password, opts.Host, opts.Port, opts.Database, "utf8")
	}
}

// Connect 进行连接
func (c *Connection) Connect() error {
	if c.IsConnected() {
		return nil
	}
	c.Close()
	var err error
	c.DB, err = sql.Open(c.opts.Type, c.ConnStr)
	return err
}

// Close 关闭数据库连接
func (c *Connection) Close() bool {
	if c.IsValid() {
		return c.DB.Close() != nil
	}
	return true
}

// IsValid 数据库连接是否有效
func (c *Connection) IsValid() bool {
	return c.DB != nil
}

// IsConnected 数据库是否已连接
func (c *Connection) IsConnected() bool {
	return c.DB != nil && c.DB.Ping() != nil
}

// GetConnection 获取sql连接
func (c *Connection) GetConnection() error {
	if c.IsConnected() == false {
		return c.Connect()
	}
	return nil
}

// Exec 执行语句
func (c *Connection) Exec(sqlstr string, args ...interface{}) (sql.Result, error) {
	if err := c.GetConnection(); err != nil {
		return nil, err
	}
	return c.DB.Exec(sqlstr, args...)
}

// Query 查询语句
func (c *Connection) Query(sqlstr string, args ...interface{}) (*sql.Rows, error) {
	if err := c.GetConnection(); err != nil {
		return nil, err
	}
	return c.DB.Query(sqlstr, args...)
}

func init() {

}
