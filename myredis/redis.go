package myredis

import (
	"fmt"
	"time"

	config "../myconfig"
	"github.com/gomodule/redigo/redis"
)

// StoreData 存储的数据结构
type StoreData struct {
	Key    string
	Field  string
	Value  string
	Expire int64
}

// RedisClient Redis客户端
type RedisClient struct {
	Pool   *redis.Pool
	chanTx chan StoreData
	isexit bool
}

// NewPool 新建Redis线程池
func (r *RedisClient) NewPool(opts config.RedisConnOpt) {
	r.Pool = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port))
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", opts.Password); err != nil {
				c.Close()
				return nil, err
			}
			if _, err := c.Do("SELECT", opts.Index); err != nil {
				c.Close()
				return nil, err
			}
			return c, nil
		},
	}
}

// KEYS 检索规则匹配的key
func (r *RedisClient) KEYS(patten string) ([]string, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	return redis.Strings(conn.Do("KEYS", patten))
}

// DEL 删除key
func (r *RedisClient) DEL(key string) (int, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("DEL", key))
}

// GET 获取hash中key对应的数据
func (r *RedisClient) GET(key string) (string, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	return redis.String(conn.Do("GET", key))
}

// SET 设置key对应的数据
func (r *RedisClient) SET(key string, value string) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	return redis.Int64(conn.Do("SET", key, value))
}

// SETEX 设置key对应的数据
func (r *RedisClient) SETEX(key string, sec int, value string) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	return redis.Int64(conn.Do("SETEX", key, sec, value))
}

// EXPIRE 设置key的过期时间
func (r *RedisClient) EXPIRE(key string, sec int64) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	return redis.Int64(conn.Do("EXPIRE", key, sec))
}

// HGETALL 获取key对应的hash数据
func (r *RedisClient) HGETALL(key string) (map[string]string, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	return redis.StringMap(conn.Do("HGETALL", key))
}

// HGET 获取hash中key对应的field的数据
func (r *RedisClient) HGET(key string, field string) (string, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	return redis.String(conn.Do("HGET", key, field))
}

// HSET 设置hash中key对应的field的数据
func (r *RedisClient) HSET(key string, field string, value string) (int64, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	return redis.Int64(conn.Do("HSET", key, field, value))
}

// HMSET 设置hash中key对应的field的数据
/*func (r *RedisClient) HMSET(key string, kv map[string]string) (string, error) {
	conn := r.Pool.Get()
	defer conn.Close()
	var args []string
	args = append(args, key)
	for k, v := range kv {
		args = append(args, k)
		args = append(args, v)
		fmt.Println(k, v)
	}
	return redis.String(conn.Do("HMSET", args))
}*/

// Write 向redis中写入数据
func (r *RedisClient) Write(data StoreData) {
	r.chanTx <- data
}

// Writes 向redis中写入多组数据
func (r *RedisClient) Writes(data []StoreData) {
	for _, v := range data {
		r.chanTx <- v
	}
}

// WriteHash 向redis中写入多组数据
func (r *RedisClient) WriteHash(key string, value map[string]string) {
	for k, v := range value {
		r.chanTx <- StoreData{
			Key:    key,
			Field:  k,
			Value:  v,
			Expire: -1,
		}
	}
}

// NewClient 新建redis客户端连接
func NewClient() *RedisClient {
	var rc = RedisClient{
		chanTx: make(chan StoreData),
	}
	rc.NewPool(config.Cfg.Redis)
	return &rc
}

// Start 开始监听数据
func (r *RedisClient) Start() {
	r.isexit = false
	go r.loopRead()
}

// Close 关闭
func (r *RedisClient) Close() {
	r.isexit = true
	close(r.chanTx)
	r.Pool.Close()
}

func (r *RedisClient) loopRead() {
	for !r.isexit {
		select {
		case tx := <-r.chanTx:
			if len(tx.Key) > 0 {
				if len(tx.Field) > 0 {
					r.HSET(tx.Key, tx.Field, tx.Value)
				} else {
					r.SET(tx.Key, tx.Value)
				}
				if tx.Expire > 0 {
					r.EXPIRE(tx.Key, tx.Expire)
				}
			}
		}
	}
}

func init() {

}
