package main
//http://marcio.io/2015/07/cheap-mapreduce-in-go/
import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

type WalkFunc func(path string, info os.FileInfo, err error) error



type Telemetry struct {
	Request struct {
		Sender  string `json:"Sender,omitempty"`
		Trigger string `json:"Trigger,omitempty"`
	} `json:"Request,omitempty"`

	App struct {
		Program  string `json:"Program,omitempty"`
		Build    string `json:"Build,omitempty"`
		License  string `json:"License,omitempty"`
		Version  string `json:"Version,omitempty"`
	} `json:"App,omitempty"`

	Connection struct {
		Type string `json:"Type,omitempty"`
	} `json:"Connection,omitempty"`

	Region struct {
		Continent string `json:"Continent,omitempty"`
		Country   string `json:"Country,omitempty"`
	} `json:"Region,omitempty"`

	Client struct {
		OsVersion    string `json:"OsVersion,omitempty"`
		Language     string `json:"Language,omitempty"`
		Architecture string `json:"Architecture,omitempty"`
	} `json:"Client,omitempty"`
}










func enumerateFiles(dirname string) chan interface{} {
	output := make(chan interface{})
	go func() {
		filepath.Walk(dirname, func(path string, f os.FileInfo, err error) error {
			//遍历的时候跑后面的函数func
			if !f.IsDir() {
				output <- path
			}
			return nil
		})
		close(output)
	}() //直接匿名函数启动就行.
	return output
}



func enumerateJSON(filename string) chan string {
	//开一个路径放入filename中.
	output := make(chan string)
	go func() {
		file, err := os.Open(filename)
		if err != nil {
			return
		}
		defer file.Close()
		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				break
			}

			// ignore any meta comments on top of JSON file  ,就是前缀是#的都跳过.
			if strings.HasPrefix(line, "#") == true {
				continue
			}

			// add each json line to our enumeration channel
			output <- line
		}//把所有line结果都放入output这个channel中.
		close(output)
	}()
	return output
}

//type 类型重定义.
// MapperCollector is a channel that collects the output from mapper tasks
type MapperCollector chan chan interface{}  //二重channel

// MapperFunc is a function that performs the mapping part of the MapReduce job
type MapperFunc func(interface{}, chan interface{})

// ReducerFunc is a function that performs the reduce part of the MapReduce job
type ReducerFunc func(chan interface{}, chan interface{})



func mapper(filename interface{}, output chan interface{}) {
	results := map[Telemetry]int{}  // 结构体作为key也是可以的.

	// start the enumeration of each JSON lines in the file
	for line := range enumerateJSON(filename.(string)) { //go 里面的channel也是用for 来遍历的.

		// decode the telemetry JSON line
		dec := json.NewDecoder(strings.NewReader(line))
		var telemetry Telemetry

		// if line cannot be JSON decoded then skip to next one
		if err := dec.Decode(&telemetry); err == io.EOF {
			continue
		} else if err != nil {
			continue
		}

		// stores Telemetry structure in the mapper results dictionary
		previousCount, exists := results[telemetry]
		if !exists {
			results[telemetry] = 1
		} else {
			results[telemetry] = previousCount + 1
		}//只是一个count
	}

	output <- results
}


func reducer(input chan interface{}, output chan interface{}) {
	results := map[Telemetry] int{}
	for matches := range input {
		for key, value := range matches.(map[Telemetry] int) {
			_, exists := results[key]
			if !exists {
				results[key] = value
			} else {
				results[key] = results[key] + value
			}
		}
	}
	output <- results
}



func mapperDispatcher(mapper MapperFunc, input chan interface{}, collector MapperCollector) {
	for item := range input {
		taskOutput := make(chan interface{})
		go mapper(item, taskOutput)
		collector <- taskOutput
	}
	close(collector)
}

func reducerDispatcher(collector MapperCollector, reducerInput chan interface{}) {
	for output := range collector {
		reducerInput <- <-output
	}
	close(reducerInput)
}



const (
	MaxWorkers = 10
)
//为甚吗这个函数会触发,下面的函数.
func mapReduce(mapper MapperFunc, reducer ReducerFunc, input chan interface{}) interface{} {

	reducerInput := make(chan interface{})
	reducerOutput := make(chan interface{})
	mapperCollector := make(MapperCollector, MaxWorkers)
//因为下面这3个函数启动了.
	go reducer(reducerInput, reducerOutput)
	go reducerDispatcher(mapperCollector, reducerInput)
	go mapperDispatcher(mapper, input, mapperCollector) //这个函数会触发上面2个.最后的reducer跑完就会
	//返回函数.reducerOutput这个channel吐出来的对象.

	return <-reducerOutput
}


func main() {
	/*
	注意自己做的json文件里面,都不能加空格.加上空格是没法识别key ,value的.



	*/
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Println("Processing. Please wait....")

	// start the enumeration of files to be processed into a channel
	input := enumerateFiles("2.json") //返回的是chaennel,所以直接调试的时候,他还是空,因为他的值是之后才跑的.

	// this will start the map reduce work    吧函数和数据都传入进去,下面遍历这个results得到结果.
	results := mapReduce(mapper, reducer, input)

	// open output file
	f, err := os.Create("telemetry.csv")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// make a write buffer
	writer := csv.NewWriter(f)

	for telemetry, value := range results.(map[Telemetry]   int) {
//results.(map[Telemetry]   int)  表示这个对象results 里面的内容是 map[Telemetry]   int
//  interface.() 表示类型转化   其他类型直接写比如float(a) 只有interface需要加个.
		var record []string

		record = append(record, telemetry.Request.Sender)
		record = append(record, telemetry.Request.Trigger)
		record = append(record, telemetry.App.Program)
		record = append(record, telemetry.App.Build)
		record = append(record, telemetry.App.License)
		record = append(record, telemetry.App.Version)
		record = append(record, telemetry.Connection.Type)
		record = append(record, telemetry.Region.Continent)
		record = append(record, telemetry.Region.Country)
		record = append(record, telemetry.Client.OsVersion)
		record = append(record, telemetry.Client.Language)
		record = append(record, telemetry.Client.Architecture)

		// The last field of the CSV line is the aggregate count for each occurrence
		record = append(record, strconv.Itoa(value))

		writer.Write(record)
	}

	writer.Flush()

	fmt.Println("Done!")



	//scene := make(map[string]int)
	var a interface{}
	var str5 string
	a = "3432423"
	str5 = a.(string)
	fmt.Println(str5)

}








































































