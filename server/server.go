package main

import (
	"errors"
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
	defaultUsecase     = "devops"
	defaultDataSource  = "simulator"
	configPath         = "./config/"
	binPath            = "./bin/"
	defaultdConfigName = "config.yaml"
	defaultExeName     = "benchmark_macos"
	defaultLogFile     = "./log.txt"
)

var (
	usesAvailable       = map[string]bool{"devops": true, "iot": true}
	dataSourceAvailable = map[string]bool{"simulator": true, "file": false}
	dbAvailable         = map[string]bool{"kmon": true, "otel": true, "influx": true, "timescale": true, "prometheus": true, "prom-pull": true}
)

type DefaultConfigFile struct {
	FileName string
	mu       sync.Mutex
}

var defaultConfigFile *DefaultConfigFile

// need to add lock
type Benchmark struct {
	mu             sync.Mutex
	state          int32
	cmd            *exec.Cmd
	configFileName string
}

func (b Benchmark) init() {
	b.mu.Lock()
	b.state = Stopped
	b.mu.Unlock()
}

func (b Benchmark) reset() {
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
	}
	if err != nil {
		return "", errors.New(fmt.Sprintf("Can't get config file %s", configFileName))
	}

	return copyAndCreateConfigFile(configFileName, "yaml")
}

func createConfigFile(configFileName, db, ds string) error {
	// multi-thread risk ...
	defaultConfigFile.mu.Lock()
	args := []string{"config", "--target=" + db, "--data-source=" + ds}
	configCmd := exec.Command(binPath+defaultExeName, args...)
	_, err := configCmd.CombinedOutput()
	if err != nil {
		return err
	}
	err = os.Rename(defaultConfigFile.FileName, configFileName)
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
	bytesWritten, err := io.Copy(dest, src)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Bytes Written: %d\n", bytesWritten)

	return newConfigFileDir, err
}

func getConfigName(db string, ds string) string {
	return strings.Join([]string{"config", db, ds}, "-")
}

func resetBechmark(benchmark *Benchmark) {
	benchmark.reset()
}

func runBenchmark(db, configFileName string, benchmark *Benchmark) error {

	args := []string{"load", db, "--config=" + configFileName}
	loadCmd := exec.Command(binPath+defaultExeName, args...)

	f, err := os.OpenFile(defaultLogFile, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return errors.New("can't open or create log file")
	}

	if benchmark == nil {
		return errors.New("get global benchmark failed")
	}
	success := atomic.CompareAndSwapInt32(&benchmark.state, Stopped, Running)
	if !success {
		_ = f.Close()
		return errors.New("other benchmark is running; use /stop api to close the running benchmark")
	}
	benchmark.cmd = loadCmd
	benchmark.configFileName = configFileName

	go func() {
		loadCmd.Stdout, loadCmd.Stderr = f, f
		_ = loadCmd.Run()
	}()
	defer resetBechmark(benchmark)

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

	ds := c.Query("ds") //data-source
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
		//c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid param: %s", usecase)})
		return nil, errors.New("double check param: usecase")
		//return errors.New(fmt.Sprintf("invalid param: %s", usecase))
	} else {
		queryParamsMap["usecase"] = usecase
	}

	if workers := c.Query("workers"); workers != "" {
		if workersNum, err := strconv.Atoi(workers); err != nil || workersNum <= 0 {
			return nil, errors.New("double check param: workers")
			//c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid param: %s", workers)})
		} else {
			queryParamsMap["workers"] = workers
		}
	}

	if scale := c.Query("scale"); scale != "" {
		if scaleNum, err := strconv.Atoi(scale); err != nil || scaleNum <= 0 {
			return nil, errors.New("double check param: scale")
			//c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid param: %s", scale)})
		} else {
			queryParamsMap["scale"] = scale
		}
	}

	if timestampEnd := c.Query("timestamp-end"); timestampEnd != "" {
		queryParamsMap["timestamp-end"] = timestampEnd
	}

	return queryParamsMap, nil
}

func overrideViperByQueryParams(v *viper.Viper, paramsMap map[string]interface{}) error {
	// parse viper
	dataSourceViper := v.GetStringMap("data-source")
	simulatorViper := dataSourceViper["simulator"].(map[string]interface{})
	loaderViper := v.GetStringMap("loader")
	runnerViper := loaderViper["runner"].(map[string]interface{})

	if paramsMap["use-case"] != nil {
		simulatorViper["use-case"] = paramsMap["use-case"].(string)
	}
	if paramsMap["workers"] != nil {
		runnerViper["workers"] = paramsMap["workers"].(string)
	}
	if paramsMap["scale"] != nil {
		simulatorViper["scale"] = paramsMap["scale"].(string)
	}
	if paramsMap["timestamp-end"] != nil {
		simulatorViper["timestamp-end"] = paramsMap["timestamp-end"].(string)
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
	v := viper.New()
	//dbSpecificViper := v.GetStringMap("data-source")["db-specific"].(map[string]string)
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

	err = overrideViperByQueryParams(v, paramsMap)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	b, isExist := c.Get("benchmark")
	if !isExist {
		c.JSON(http.StatusInternalServerError, "get global benchmark failed due to unknown reason")
		return
	}
	benchmark := b.(*Benchmark)

	err = runBenchmark(paramsMap["db"].(string), configFileName, benchmark)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, "benchmark is running successfully, check running stats on grafana or use /result api")
}

func stopHandler(c *gin.Context) {
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

	if benchmark.state == Stopped {
		c.JSON(http.StatusOK, "benchmark is stopped now, use /start api to run new benchmark")
		return
	}

	err := benchmark.cmd.Process.Kill()
	if err != nil {
		c.JSON(http.StatusInternalServerError, "benchmark shutdown has failed, please shutdown manually")
	}
	err = os.Remove(benchmark.configFileName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, "fail to clean config file")
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

func main() {
	benchmark := new(Benchmark)
	benchmark.init()

	defaultConfigFile = new(DefaultConfigFile)

	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery(), GetBenchmark(benchmark))

	r.GET("/start", startHandler)
	r.GET("/stop", stopHandler)
	//r.GET("/result", resultHanlder)
	_ = r.Run(":8888")
}
