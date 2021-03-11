package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"net/http"
)

const (
	Stopped int32 = iota
	Running int32 = iota
)

const (
	defaultUsecase    = "devops"
	defaultDataSource = "simulator"
	configPath        = "./config/"
	binPath           = "./bin/"
	defaultConfigPath = "./config.yaml"
	defaultExeName    = "benchmark"
	defaultLogFile    = "./log.txt"
)

var (
	usesAvailable       = map[string]bool{"devops": true, "iot": true}
	dataSourceAvailable = map[string]bool{"simulator": true, "file": false}
	dbAvailable         = map[string]bool{"kmon": true, "otel": true, "influx": true, "timescale": true, "prometheus": true, "prom-pull": true}
	sharedParams        = map[string]bool{"db": true, "ds": true, "usecase": true, "workers": true, "scale": true, "timestamp-end": true}
)

type DefaultConfigFile struct {
	FileName string
	mu       sync.Mutex
}

var defaultConfigFile *DefaultConfigFile

type Benchmark struct {
	mu             sync.Mutex
	state          int32
	cmd            *exec.Cmd
	configFileName string
}

func (b *Benchmark) init() {
	b.mu.Lock()
	b.state = Stopped
	b.mu.Unlock()
}

func (b *Benchmark) reset() {
	b.mu.Lock()
	b.state = Stopped
	b.cmd = nil
	b.configFileName = ""
	b.mu.Unlock()
}

func getConfigFile(v *viper.Viper, db string, ds string) (string, error) {
	configFileName := getConfigName(db, ds)
	v.SetConfigName(configFileName)
	v.SetConfigType("yaml")
	v.AddConfigPath(configPath)
	err := v.ReadInConfig()
	if _, ok := err.(viper.ConfigFileNotFoundError); ok {
		configFileDir := configPath + strings.Join([]string{configFileName, "yaml"}, ".")
		err = createConfigFile(configFileDir, db, ds)
		err = v.ReadInConfig()
	}
	if err != nil {
		return "", errors.New(fmt.Sprintf("Can't get config file %s", configFileName))
	}

	return copyAndCreateConfigFile(configFileName, "yaml")
}

func createConfigFile(configFileName, db, ds string) error {
	defaultConfigFile.mu.Lock()
	args := []string{"config", "--target=" + db, "--data-source=" + strings.ToUpper(ds)}
	configCmd := exec.Command(binPath+defaultExeName, args...)
	_, err := configCmd.CombinedOutput()
	if err != nil {
		return err
	}
	err = os.Rename(defaultConfigPath, configFileName)
	defaultConfigFile.mu.Unlock()
	return err
}

func copyAndCreateConfigFile(configFileName string, configType string) (string, error) {
	configFileDir := configPath + strings.Join([]string{configFileName, configType}, ".")
	newConfigFileName := strings.Join([]string{configFileName, strconv.FormatInt(time.Now().Unix(), 10)}, "-") //guarantee uniqueness
	newConfigFileDir := configPath + strings.Join([]string{newConfigFileName, configType}, ".")

	// Open original file
	src, err := os.Open(configFileDir)
	if err != nil {
		log.Fatal(err)
	}
	defer src.Close()

	// Create new file
	dest, err := os.Create(newConfigFileDir)
	if err != nil {
		log.Fatal(err)
	}
	defer dest.Close()

	//This will copy
	_, err = io.Copy(dest, src)
	if err != nil {
		log.Fatal(err)
	}

	return newConfigFileDir, err
}

func getConfigName(db string, ds string) string {
	return strings.Join([]string{"config", db, strings.ToLower(ds)}, "-")
}

func runBenchmark(db, configFileName string, benchmark *Benchmark) error {

	args := []string{"load", db, "--config=" + configFileName}
	loadCmd := exec.Command(binPath+defaultExeName, args...)

	f, err := os.OpenFile(defaultLogFile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0755)
	f.WriteString(fmt.Sprintf("\nstart benchmark at time: %v\n", time.Now()))

	if err != nil {
		return errors.New("can't open or create log file")
	}

	if benchmark == nil {
		return errors.New("get global benchmark failed")
	}
	benchmark.cmd = loadCmd
	benchmark.configFileName = configFileName

	go func() {
		loadCmd.Stdout, loadCmd.Stderr = f, f
		_ = loadCmd.Run()
		_ = f.Close()
		err = os.Remove(benchmark.configFileName)
		benchmark.reset()
	}()
	return nil
}

func parseStartParams(c *gin.Context, dbSpecificMap map[string]string) (map[string]interface{}, error) {
	queryParamsMap := make(map[string]interface{})

	// parse and check
	db := c.Query("db")
	if db == "" || !dbAvailable[db] {
		return nil, errors.New("double check param: db")
	} else {
		queryParamsMap["db"] = db
	}

	//ds for data source
	ds := c.Query("ds")
	if ds == "" {
		queryParamsMap["ds"] = defaultDataSource
	} else if !dataSourceAvailable[ds] {
		return nil, errors.New("double check param: ds")
	} else {
		queryParamsMap["ds"] = ds
	}

	usecase := c.Query("usecase")
	if usecase == "" {
		usecase = defaultUsecase
	} else if !usesAvailable[usecase] {
		return nil, errors.New("double check param: usecase")
	} else {
		queryParamsMap["usecase"] = usecase
	}

	if workers := c.Query("workers"); workers != "" {
		if workersNum, err := strconv.Atoi(workers); err != nil || workersNum <= 0 {
			return nil, errors.New("double check param: workers")
		} else {
			queryParamsMap["workers"] = workers
		}
	}

	if scale := c.Query("scale"); scale != "" {
		if scaleNum, err := strconv.Atoi(scale); err != nil || scaleNum <= 0 {
			return nil, errors.New("double check param: scale")
		} else {
			queryParamsMap["scale"] = scale
		}
	}

	if timestampEnd := c.Query("timestamp-end"); timestampEnd != "" {
		queryParamsMap["timestamp-end"] = timestampEnd
	}

	//without checking
	qMap := c.Request.URL.Query()
	for k, v := range qMap {
		if _, ok := sharedParams[k]; !ok {
			queryParamsMap[k] = v[0]
		}
	}

	return queryParamsMap, nil
}

func overrideViperByQueryParams(v *viper.Viper, paramsMap map[string]interface{}) error {
	// parse viper
	dataSourceViper := v.GetStringMap("data-source")
	simulatorViper := dataSourceViper["simulator"].(map[string]interface{})
	loaderViper := v.GetStringMap("loader")
	runnerViper := loaderViper["runner"].(map[string]interface{})
	dbSpecficViper := loaderViper["db-specific"].(map[string]interface{})

	// shared configs
	if val, ok := paramsMap["usecase"]; ok {
		simulatorViper["use-case"] = val.(string)
	}
	if val, ok := paramsMap["workers"]; ok {
		runnerViper["workers"] = val.(string)
	}
	if val, ok := paramsMap["scale"]; ok {
		simulatorViper["scale"] = val.(string)
	}
	if val, ok := paramsMap["timestamp-end"]; ok {
		simulatorViper["timestamp-end"] = val.(string)
	}

	// db-specific configs
	for k, val := range paramsMap {
		if _, ok := dbSpecficViper[k]; ok {
			dbSpecficViper[k] = val
		}
	}

	// parse dbspecific params
	v.Set("data-source", dataSourceViper)
	v.Set("loader", loaderViper)

	err := v.WriteConfig()
	if err != nil {
		return errors.New("fail to change config file")
	}

	return nil
}

func startHandler(c *gin.Context) {

	b, isExist := c.Get("benchmark")
	if !isExist {
		c.JSON(http.StatusInternalServerError, "get global benchmark failed due to unknown reason")
		return
	}
	benchmark := b.(*Benchmark)

	if !atomic.CompareAndSwapInt32(&benchmark.state, Stopped, Running) { // carry out only one benchmark at the same time
		c.JSON(http.StatusServiceUnavailable, "another benchmark is running, use /stop api to shutdown first")
		return
	}

	v := viper.New()
	paramsMap, err := parseStartParams(c, nil)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	configFileName, err := getConfigFile(v, paramsMap["db"].(string), paramsMap["ds"].(string)) //config items are stored in v now
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	v.SetConfigFile(configFileName)
	err = overrideViperByQueryParams(v, paramsMap)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	err = runBenchmark(paramsMap["db"].(string), configFileName, benchmark)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, "benchmark is running successfully, check running stats on grafana or use /result api")
}

func stopHandler(c *gin.Context) {
	errInfo := ""
	b, isExist := c.Get("benchmark")
	if !isExist {
		c.JSON(http.StatusInternalServerError, "get global benchmark failed due to unknown reason")
		return
	}
	benchmark := b.(*Benchmark)
	if benchmark == nil {
		c.JSON(http.StatusInternalServerError, "get global benchmark failed due to unknown reason")
		return
	}

	err := os.Remove(benchmark.configFileName)
	if err != nil {
		errInfo += "fail to clean config file;"
	}

	if benchmark.state == Stopped {
		c.JSON(http.StatusOK, errInfo+"benchmark is stopped now, use /start api to run new benchmark")
		return
	}

	err = benchmark.cmd.Process.Kill()
	if err != nil {
		c.JSON(http.StatusInternalServerError, errInfo+"benchmark shutdown has failed, please shutdown manually;")
		return
	}

	atomic.CompareAndSwapInt32(&benchmark.state, Running, Stopped) //if failed, means already stopped
	c.JSON(http.StatusOK, "benchmark has been shutdown successfully")
}

func GetBenchmark(benchmark *Benchmark) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set("benchmark", benchmark)
		c.Next()
	}
}

func deleteLogHandler(c *gin.Context) {
	b, isExist := c.Get("benchmark")
	if !isExist {
		c.JSON(http.StatusInternalServerError, "get global benchmark failed due to unknown reason")
		return
	}
	benchmark := b.(*Benchmark)

	if benchmark.state == Running {
		c.JSON(http.StatusServiceUnavailable, "benchmark is running, use /stop api to shutdown first")
		return
	}

	// multi-thread risk
	_ = os.Remove(defaultLogFile)
	_, _ = os.Create(defaultLogFile)

	c.JSON(http.StatusServiceUnavailable, "log has been cleaned!")
	return
}

func main() {

	// gloabal vars initialize
	benchmark := new(Benchmark)
	benchmark.init()

	defaultConfigFile = new(DefaultConfigFile)

	//parse command line params
	port := flag.Int("p", 8888, "server listen at this port")
	flag.Parse()

	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery(), GetBenchmark(benchmark))

	r.GET("/start", startHandler)
	r.GET("/stop", stopHandler)
	r.StaticFS("/files", http.Dir("./"))
	r.StaticFile("/log.txt", defaultLogFile)
	r.DELETE("/log", deleteLogHandler)
	_ = r.Run(":" + strconv.Itoa(*port))
}
