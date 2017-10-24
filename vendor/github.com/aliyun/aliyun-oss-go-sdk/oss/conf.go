package oss

import (
	"time"
)

// HTTPTimeout http timeout
type HTTPTimeout struct {
	ConnectTimeout   time.Duration
	ReadWriteTimeout time.Duration
	HeaderTimeout    time.Duration
	LongTimeout      time.Duration
}

// Config oss configure
type Config struct {
	Endpoint        string      // oss地址
	AccessKeyID     string      // accessId
	AccessKeySecret string      // accessKey
	RetryTimes      uint        // 失败重试次数，默认5
	UserAgent       string      // SDK名称/版本/系统信息
	IsDebug         bool        // 是否开启调试模式，默认false
	Timeout         uint        // 超时时间，默认60s
	SecurityToken   string      // STS Token
	IsCname         bool        // Endpoint是否是CNAME
	HTTPTimeout     HTTPTimeout // HTTP的超时时间设置
	IsUseProxy      bool        // 是否使用代理
	ProxyHost       string      // 代理服务器地址
	IsAuthProxy     bool        // 代理服务器是否使用用户认证
	ProxyUser       string      // 代理服务器认证用户名
	ProxyPassword   string      // 代理服务器认证密码
	IsEnableMD5     bool        // 上传数据时是否启用MD5校验
	MD5Threshold    int64       // 内存中计算MD5的上线大小，大于该值启用临时文件，单位Byte
	IsEnableCRC     bool        // 上传数据时是否启用CRC64校验
}

// 获取默认配置
func getDefaultOssConfig() *Config {
	config := Config{}

	config.Endpoint = ""
	config.AccessKeyID = ""
	config.AccessKeySecret = ""
	config.RetryTimes = 5
	config.IsDebug = false
	config.UserAgent = userAgent
	config.Timeout = 60 // seconds
	config.SecurityToken = ""
	config.IsCname = false

	config.HTTPTimeout.ConnectTimeout = time.Second * 30   // 30s
	config.HTTPTimeout.ReadWriteTimeout = time.Second * 60 // 60s
	config.HTTPTimeout.HeaderTimeout = time.Second * 60    // 60s
	config.HTTPTimeout.LongTimeout = time.Second * 300     // 300s

	config.IsUseProxy = false
	config.ProxyHost = ""
	config.IsAuthProxy = false
	config.ProxyUser = ""
	config.ProxyPassword = ""

	config.MD5Threshold = 16 * 1024 * 1024 // 16MB
	config.IsEnableMD5 = false
	config.IsEnableCRC = true

	return &config
}
