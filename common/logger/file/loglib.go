package loglib

import (
	"bufio"
	"container/list"
	kafkalib "dmp_distribution/common/logger/kafka"
	mysqldb "dmp_distribution/common/mysql"
	"dmp_distribution/core"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	jsoniter "github.com/json-iterator/go"
)

type Logger struct {
	ms_dir, ms_filname, ms_curfile, ms_topic string
	ms_dealnumfre, ms_timespan               int
	exec                                     core.WaitGroup
	ms_lock                                  sync.Mutex
	ms_logfile                               *os.File
	ms_log                                   *log.Logger
	ms_loglist                               *list.List
}

func (l *Logger) Init_Log(filemkdir, filename string) {
	l.ms_filname = filename
	l.ms_dir = filemkdir
	l.ms_loglist = list.New()
	l.ms_dealnumfre = 5000 //每次处理条目数
	l.ms_timespan = 10     //每次处理时间间隔（毫秒）
	l.exec = core.WaitGroup{}
	//kafka 初始化
	// if strings.Contains(filemkdir, "imp") {
	// 	l.ms_topic = "imp"
	// } else if strings.Contains(filemkdir, "clk") {
	// 	l.ms_topic = "clk"
	// } else if strings.Contains(filemkdir, "track") {
	// 	l.ms_topic = "track"
	// }
}

func (l *Logger) Close_Log() {
	if l.ms_logfile != nil {
		defer l.ms_logfile.Close()
	}
}

func (l *Logger) Set_DealProInfo(numfre, timespan int) {
	l.ms_dealnumfre = numfre
	l.ms_timespan = timespan
}

func (l *Logger) Start_Log() {
	t1 := time.NewTimer(time.Millisecond * time.Duration(l.ms_timespan))
	go func() {
		for {
		ENTLOOP:
			select {
			case <-t1.C:
				t1.Reset(time.Duration(l.ms_timespan) * time.Millisecond)
				var n *list.Element
				i := 0
				l.ms_lock.Lock()
				for e := l.ms_loglist.Front(); e != nil; e = n {
					if l.ms_topic != "" {
						kafkalib.Sent_Msg(e.Value.(string), l.ms_topic)
					} else {
						l.Write_Log(e.Value)
					}
					n = e.Next()
					l.ms_loglist.Remove(e)
					i++
					if i == l.ms_dealnumfre {
						l.ms_lock.Unlock()
						break ENTLOOP
					}
				}
				l.ms_lock.Unlock()

			}
		}

	}()
}

func (l *Logger) Set_LogFlags(flags int) {
	if l.ms_log != nil {
		l.ms_log.SetFlags(flags)
	}
}

func (l *Logger) Set_LogPreFix(str string) {
	if l.ms_log != nil {
		l.ms_log.SetPrefix(str)
	}
}

func (l *Logger) Write_Log(str interface{}) {
	timenow := time.Now()
	time_date := timenow.Format("20060102") //年月日
	//time_date_hour := timenow.Format("200601021504") //年月日时分
	time_date_hour := timenow.Format("2006010215") //年月日时
	// elapsedSeconds := timenow.Hour()*3600 + timenow.Minute()*60 + timenow.Second()
	// interval := strconv.Itoa((elapsedSeconds / 10) % 6)
	os.MkdirAll(l.ms_dir+time_date+"/", 0777)

	if l.ms_curfile == "" {
		l.ms_curfile = l.ms_dir + time_date + "/" + l.ms_filname + ".hour." + time_date_hour
		l.ms_logfile, _ = os.OpenFile(l.ms_curfile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		l.ms_log = log.New(l.ms_logfile, "", 0)
	} else {
		nametmp := l.ms_dir + time_date + "/" + l.ms_filname + ".hour." + time_date_hour
		if nametmp != l.ms_curfile {
			l.ms_logfile.Close()
			if !strings.Contains(l.ms_curfile, "err.log") {
				l.inputTask(l.ms_curfile)
			}
			l.ms_curfile = nametmp

			//l.inputTask(nametmp)
			l.ms_logfile, _ = os.OpenFile(l.ms_curfile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
			l.ms_log = log.New(l.ms_logfile, "", 0)
		}
	}
	l.ms_log.Println(str)
}

func (l *Logger) LogFile(str string) {
	l.ms_lock.Lock()
	l.ms_loglist.PushBack(str)
	l.ms_lock.Unlock()
}

func (l *Logger) SetJsonLogPrefix(memmap map[string]interface{}, key, data string) {
	if data != "" {
		memmap[key] = data
	}
}

func (l *Logger) LogJson(memmap map[string]interface{}) {
	str, _ := jsoniter.Marshal(memmap)
	l.LogFile(string(str))
}

func (l *Logger) LogStruct(memmap interface{}) {
	str, _ := jsoniter.Marshal(memmap)
	l.LogFile(string(str))
}

func (l *Logger) inputTask(logname string) {

	// 读取文件行数
	lineCount, err := countLines(logname)
	if err != nil {
		log.Println("Failed to count lines: " + err.Error())
		return
	}

	if lineCount <= 0 {
		return
	}
	// 读取文件大小
	fileInfo, err := os.Stat(logname)
	if err != nil {
		log.Println("Failed to get file info: " + err.Error())
		return
	}
	fileSize := fileInfo.Size()

	// 读取主机名
	hostname, err := os.Hostname()
	if err != nil {
		log.Println("Failed to get hostname: " + err.Error())
		return
	}
	t := time.Now().Unix()
	stmt, err := mysqldb.MysqlDbs.Prepare("INSERT INTO `load_checkpoint` (`host`, `file`, `size`, `numbers`, `status`, `err_count`, `create_time`, `exec_time`) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		log.Println("Failed to get Prepare error: " + err.Error())
		return
	}
	id, err := stmt.Exec(hostname, logname, fileSize, lineCount, 0, 0, t, t)
	if err != nil {
		log.Println("Failed to get stmt.Exec error: "+err.Error(), id)
		return
	}
	stmt.Close()
	// if err != nil {
	// 	log.Println("Failed to insert into task: " + err.Error())
	// }
	//result.LastInsertId()

}

// 计算文件行数
func countLines(filePath string) (int, error) {
	// _, err := os.Stat(filePath)

	// if err != nil {
	// 	if os.IsNotExist(err) {
	// 		return 0, err
	// 	}
	// }
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return lineCount, nil
}
