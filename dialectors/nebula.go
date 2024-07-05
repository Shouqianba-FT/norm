/**
* package: dialectors
* file: dirlector.go
* author: wuzhensheng
* create: 2021-06-23 11:52:00
* description:
**/
package dialectors

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	nebula "github.com/vesoft-inc/nebula-go/v3"
)

type (
	NebulaDialector struct {
		sPool    *nebula.SessionPool
		username string
		password string
		space    string
	}
)

var _ IDialector = new(NebulaDialector)

func NewNebulaDialector(cfg DialectorConfig) (*NebulaDialector, error) {
	cfg.LoadDefault()
	nAddresses, err := parseAddresses(cfg.Addresses)
	if err != nil {
		return &NebulaDialector{}, err
	}
	nConfig, err := nebula.NewSessionPoolConf(cfg.Username, cfg.Password, nAddresses, cfg.Space,
		nebula.WithTimeOut(cfg.Timeout),
		nebula.WithIdleTime(cfg.IdleTime),
		nebula.WithMinSize(cfg.MinConnPoolSize),
		nebula.WithMaxSize(cfg.MaxConnPoolSize))
	if err != nil {
		return &NebulaDialector{}, err
	}
	sPool, err := nebula.NewSessionPool(*nConfig, nebula.DefaultLogger{})
	if err != nil {
		return &NebulaDialector{}, errors.Wrap(err, "connect nebula fail")
	}
	return &NebulaDialector{
		sPool:    sPool,
		username: cfg.Username,
		password: cfg.Password,
		space:    cfg.Space,
	}, nil
}

// MustNewNebulaDialector 语法糖, 必须新建一个 Nebula Dialector
func MustNewNebulaDialector(cfg DialectorConfig) *NebulaDialector {
	dialector, err := NewNebulaDialector(cfg)
	if err != nil {
		panic(err)
	}
	return dialector
}

// Execute TODO 可以缓存一个 session pool.
func (d *NebulaDialector) Execute(stmt string) (*ResultSet, error) {
	result, err := d.sPool.Execute(stmt)
	if err != nil {
		return &ResultSet{}, err
	}
	if err = checkResultSet(result); err != nil {
		return &ResultSet{}, err
	}

	return &ResultSet{result}, nil
}

func (d *NebulaDialector) Close() {
	d.sPool.Close()
}

// checkResultSet 检查是否成功执行
func checkResultSet(nSet *nebula.ResultSet) error {
	if nSet.GetErrorCode() != nebula.ErrorCode_SUCCEEDED {
		return errors.New(fmt.Sprintf("code: %d, msg: %s",
			nSet.GetErrorCode(), nSet.GetErrorMsg()))
	}
	return nil
}

// parseAddresses 解析传入的 Host 格式为 nebula 需要的格式
func parseAddresses(addresses []string) ([]nebula.HostAddress, error) {
	hostAddresses := make([]nebula.HostAddress, len(addresses))
	for i, addr := range addresses {
		list := strings.Split(addr, ":")
		if len(list) < 2 {
			return []nebula.HostAddress{},
				errors.New(fmt.Sprintf("address %s invalid", addr))
		}
		port, err := strconv.ParseInt(list[1], 10, 64)
		if err != nil {
			return []nebula.HostAddress{},
				errors.New(fmt.Sprintf("address %s invalid", addr))
		}
		hostAddresses[i] = nebula.HostAddress{
			Host: list[0],
			Port: int(port),
		}
	}
	return hostAddresses, nil
}
