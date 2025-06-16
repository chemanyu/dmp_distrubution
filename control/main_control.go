package control

import (
	"context"
	mysqldb "dmp_distribution/common/mysql"
	"dmp_distribution/core"
	handlers "dmp_distribution/handler"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/fvbock/endless"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/spf13/pflag"
)

type (
	handlerEntity struct {
		handlerPath       string
		handlerMethodSets []string
		handler           gin.HandlerFunc
	}

	handlerBundle struct {
		rootUrl           string
		apiVer            string
		handlerEntitySets []*handlerEntity
	}
)

var (
	DefaultHandlerBundle *handlerBundle
	config               *core.Config
)

// 初始化服务  绑定IP 端口
func init() {
	cfg := pflag.StringP("config", "c", "./etc/conf.yaml", "api server config file path.")
	pflag.Parse()

	DefaultHandlerBundle = &handlerBundle{
		rootUrl:           "report",
		apiVer:            "v1",
		handlerEntitySets: []*handlerEntity{},
	}

	// 加载配置文件
	config = core.LoadConfig(*cfg)

	mysqldb.InitMysql()

}

func MainControl() {
	router := gin.New()
	if config.PPROF == "true" {
		pprof.Register(router)
	}

	// 注册中间件
	router.Use(gin.Recovery()) // Gin 的错误恢复中间件
	router.Use(gin.Logger())   // Gin 的日志中间件

	regHandlers := []handlers.Handler{}

	// 注册路由
	for _, handler := range regHandlers {
		handler.ForeachHandler(func(path string, methodSet []string, handler func(*gin.Context)) {
			DefaultHandlerBundle.handlerEntitySets = append(DefaultHandlerBundle.handlerEntitySets, &handlerEntity{
				handlerPath:       path,
				handlerMethodSets: methodSet,
				handler:           handler,
			})
		})
	}

	// 将所有注册的处理函数添加到Gin中
	for _, entity := range DefaultHandlerBundle.handlerEntitySets {
		var pathComponents []string
		pathComponents = append(pathComponents, DefaultHandlerBundle.rootUrl)
		pathComponents = append(pathComponents, DefaultHandlerBundle.apiVer)
		pathComponents = append(pathComponents, entity.handlerPath)

		fullPath := strings.Join(pathComponents, "/")
		methodSet := strings.Join(entity.handlerMethodSets, "/")

		log.Println("INFO", "MainControl handler", "path:", fullPath, "method", methodSet)

		// Gin 中的路由注册
		switch methodSet {
		case "GET":
			router.GET(fullPath, entity.handler)
		case "POST":
			router.POST(fullPath, entity.handler)
		case "PUT":
			router.PUT(fullPath, entity.handler)
		case "DELETE":
			router.DELETE(fullPath, entity.handler)
		}
	}

	// 启动http服务器
	serverAddress := config.SERVER_ADDRESS
	startServer(router, serverAddress)
}

// 开启服务
func startServer(r *gin.Engine, address string) {
	// 创建一个 HTTP 服务器实例
	srv := endless.NewServer(":"+config.SERVER_PORT, r)

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				log.Fatalf("Server failed to start: %v", err)
			}
			log.Println("Server stopped gracefully")
		}
	}()

	// graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Println("ERROR", "[startServer]server_stop", "err", err)
	}
}
