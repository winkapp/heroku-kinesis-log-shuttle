package main

import (
	"flag"
	"fmt"
	"log"
	"log/syslog"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/winkapp/log-shuttle"
	"errors"
    "github.com/winkapp/log-shuttle/l2met/reader"
    "github.com/winkapp/log-shuttle/l2met/outlet"
    "github.com/winkapp/log-shuttle/l2met/metchan"
    "github.com/winkapp/log-shuttle/l2met/store"
)

var detectKinesis = regexp.MustCompile(`\Akinesis.[[:alpha:]]{2}-[[:alpha:]]{2,}-[[:digit:]]\.amazonaws\.com\z`)

// Default loggers to stdout and stderr
var (
	logger    = log.New(os.Stdout, "log-shuttle: ", log.LstdFlags | log.Lshortfile)
	errLogger = log.New(os.Stderr, "log-shuttle: ", log.LstdFlags | log.Lshortfile)

	logToSyslog bool
)

var version = "" // log-shuttle version, set with linker

//// useStdin determines if we're using the terminal's stdin or not
//func useStdin() bool {
//	return !util.IsTerminal(os.Stdin)
//}

func mapInputFormat(i string) (int, error) {
	switch i {
	case "raw":
		return shuttle.InputFormatRaw, nil
	case "rfc3164":
		return shuttle.InputFormatRFC3164, nil
	case "rfc5424":
		return shuttle.InputFormatRFC5424, nil
	}
	return 0, fmt.Errorf("Unknown input format: %s", i)
}

// determineLogsURL from the various options favoring each one in turn
func determineLogsURL(logplexURL, logsURL, cmdLineURL string) string {
	var envURL string

	if len(logplexURL) > 0 {
		log.Println("Warning: $LOGPLEX_URL is deprecated, use $LOGS_URL instead")
		envURL = logplexURL
	}

	if len(logsURL) > 0 {
		if len(logplexURL) > 0 {
			log.Println("Warning: Use of both $LOGPLEX_URL & $LOGS_URL, using $LOGS_URL instead")
		}
		envURL = logsURL
	}

	if len(cmdLineURL) > 0 {
		if len(envURL) > 0 {
			log.Println("Warning: Use of both an evnironment variable ($LOGPLEX_URL or $LOGS_URL) and -logs-url, using -logs-url option")
		}
		return cmdLineURL
	}
	return envURL
}

// parseFlags overrides the properties of the given config using the provided
// command-line flags.  Any option not overridden by a flag will be untouched.
func parseFlags(c shuttle.Config) (shuttle.Config, error) {
	var skipHeaders bool
	var statsAddr string
	var printVersion bool

	flag.BoolVar(&c.Verbose, "verbose", c.Verbose, "Enable verbose debug info.")
	flag.BoolVar(&c.SkipVerify, "skip-verify", c.SkipVerify, "Skip the verification of HTTPS server certificate.")
	flag.BoolVar(&c.UseGzip, "gzip", c.UseGzip, "POST using gzip compression.")
	flag.BoolVar(&c.Drop, "drop", c.Drop, "Drop (default) logs or backup & block stdin.")

	flag.BoolVar(&skipHeaders, "skip-headers", skipHeaders, "Skip the prepending of rfc5424 headers.")
	flag.BoolVar(&logToSyslog, "log-to-syslog", logToSyslog, "Log to syslog instead of stderr.")
	flag.BoolVar(&printVersion, "version", printVersion, "Print log-shuttle version & exit.")

	var inputFormat string

	flag.StringVar(&c.Prival, "prival", c.Prival, "The primary value of the rfc5424 header.")
	flag.StringVar(&c.Version, "syslog-version", c.Version, "The version of syslog.")
	flag.StringVar(&c.Procid, "procid", c.Procid, "The procid field for the syslog header.")
	flag.StringVar(&c.Appname, "appname", c.Appname, "The app-name field for the syslog header.")
	flag.StringVar(&c.Appname, "logplex-token", c.Appname, "Secret logplex token.")
	flag.StringVar(&c.Hostname, "hostname", c.Hostname, "The hostname field for the syslog header.")
	flag.StringVar(&c.Msgid, "msgid", c.Msgid, "The msgid field for the syslog header.")
	flag.StringVar(&c.LogsURL, "logs-url", c.LogsURL, "The receiver of the log data.")
	flag.StringVar(&c.StatsSource, "stats-source", c.StatsSource, "When emitting stats, add source=<stats-source> to the stats.")

	flag.StringVar(&inputFormat, "input-format", "raw", "raw (default), rfc3164 (syslog(3)), rfc5424.")
	flag.StringVar(&statsAddr, "stats-addr", "", "DEPRECATED, WILL BE REMOVED, HAS NO EFFECT.")

	flag.DurationVar(&c.StatsInterval, "stats-interval", c.StatsInterval, "How often to emit/reset stats.")
	flag.DurationVar(&c.WaitDuration, "wait", c.WaitDuration, "Duration to wait to flush messages to logs-url.")
	flag.DurationVar(&c.Timeout, "timeout", c.Timeout, "Duration to wait for a response from logs-url.")

	flag.IntVar(&c.MaxAttempts, "max-attempts", c.MaxAttempts, "Max number of retries.")
	var b int
	flag.IntVar(&b, "num-batchers", b, "[NO EFFECT/REMOVED] The number of batchers to run.")
	flag.IntVar(&c.NumOutlets, "num-outlets", c.NumOutlets, "The number of outlets to run.")
	flag.IntVar(&c.BatchSize, "batch-size", c.BatchSize, "Number of messages to pack into an application/logplex-1 http request.")
	var f int
	flag.IntVar(&f, "front-buff", f, "[NO EFFECT/REMOVED] Number of messages to buffer in log-shuttle's input channel.")
	flag.IntVar(&c.BackBuff, "back-buff", c.BackBuff, "Number of batches to buffer before dropping.")
	flag.IntVar(&c.MaxLineLength, "max-line-length", c.MaxLineLength, "Number of bytes that the backend allows per line.")
	flag.IntVar(&c.KinesisShards, "kinesis-shards", c.KinesisShards, "Number of unique partition keys to use per app.")


	flag.IntVar(&c.L2met_BufferSize, "buffer", c.L2met_BufferSize, "Max number of items for all internal buffers.")
	flag.IntVar(&c.L2met_Concurrency, "concurrency", c.L2met_Concurrency, "Number of running go routines for outlet or receiver.")

	flag.DurationVar(&c.L2met_FlushInterval, "flush-interval", c.L2met_FlushInterval, "Time to wait before sending data to store or outlet. Example:60s 30s 1m")
	flag.Uint64Var(&c.L2met_MaxPartitions, "partitions", c.L2met_MaxPartitions, "Number of partitions to use for outlets.")
	flag.StringVar(&c.L2met_OutletAPIToken, "outlet-token", c.L2met_OutletAPIToken, "Outlet API Token.")
	flag.DurationVar(&c.L2met_OutletInterval, "outlet-interval", c.L2met_OutletInterval, "Time to wait before outlets read buckets from the store. Example:60s 30s 1m")
	flag.IntVar(&c.L2met_OutletRetries, "outlet-retry", c.L2met_OutletRetries, "Number of attempts to outlet metrics.")
	flag.DurationVar(&c.L2met_OutletTtl, "outlet-ttl", c.L2met_OutletTtl, "Timeout set on outlet HTTP requests.")
	flag.Int64Var(&c.L2met_ReceiverDeadline, "recv-deadline", c.L2met_ReceiverDeadline, "Number of time units to pass before dropping incoming logs.")
	flag.BoolVar(&c.L2met_UseDataDogOutlet, "outlet-datadog", c.L2met_UseDataDogOutlet, "Start the DataDog outlet.")
    flag.BoolVar(&c.L2met_UseNewRelicOutlet, "outlet-newrelic", c.L2met_UseNewRelicOutlet, "Start the NewRelic outlet.")

	flag.Parse()

	if printVersion {
		fmt.Println(version)
		os.Exit(0)
	}

    logger.SetFlags(log.LstdFlags | log.Lshortfile)
    logger.SetPrefix(c.Appname + ": ")
    errLogger.SetFlags(log.LstdFlags | log.Lshortfile)
    errLogger.SetPrefix(c.Appname + ": ")
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetPrefix(c.Appname + ": ")

	if f != 0 {
		log.Println("Warning: Use of -front-buff is no longer supported. The flag has no effect and will be removed in the future.")
	}

	if b != 0 {
		log.Println("Warning: Use of -num-batchers is no longer supported. The flag has no effect and will be removed in the future.")
	}

	if statsAddr != "" {
		log.Println("Warning: Use of -stats-addr is deprecated and will be dropped in the future.")
	}

	var err error
	c.InputFormat, err = mapInputFormat(inputFormat)
	if err != nil {
		return c, err
	}

	if skipHeaders {
		log.Println("Warning: Use of -skip-headers is deprecated, use -input-format=rfc5424 instead")
		switch c.InputFormat {
		case shuttle.InputFormatRaw:
			// Massage InputFormat as that's what is used internally
			c.InputFormat = shuttle.InputFormatRFC5424
		case shuttle.InputFormatRFC5424:
			// NOOP
		default:
			return c, errors.New("Can only use -skip-headers with default input format or rfc5424")
		}
	}

	if c.Verbose == true {
		log.Println("-------------------- Config Settings --------------------")
		log.Printf("MaxLineLength:           %v\n", c.MaxLineLength)
		log.Printf("Quiet:                   %v\n", c.Quiet)
		log.Printf("Verbose:                 %v\n", c.Verbose)
		log.Printf("SkipVerify:              %v\n", c.SkipVerify)
		log.Printf("Prival:                  %v\n", c.Prival)
		log.Printf("Version:                 %v\n", c.Version)
		log.Printf("Procid:                  %v\n", c.Procid)
		log.Printf("Appname:                 %v\n", c.Appname)
		log.Printf("Hostname:                %v\n", c.Hostname)
		log.Printf("Msgid:                   %v\n", c.Msgid)
		log.Printf("LogsURL:                 %v\n", c.LogsURL)
		log.Printf("StatsSource:             %v\n", c.StatsSource)
		log.Printf("StatsInterval:           %v\n", c.StatsInterval)
		log.Printf("MaxAttempts:             %v\n", c.MaxAttempts)
		log.Printf("InputFormat:             %v\n", c.InputFormat)
		log.Printf("NumOutlets:              %v\n", c.NumOutlets)
		log.Printf("WaitDuration:            %v\n", c.WaitDuration)
		log.Printf("BatchSize:               %v\n", c.BatchSize)
		log.Printf("BackBuff:                %v\n", c.BackBuff)
		log.Printf("Timeout:                 %v\n", c.Timeout)
		log.Printf("ID:                      %v\n", c.ID)
		log.Printf("Logger:                  %v\n", c.Logger)
		log.Printf("ErrLogger:               %v\n", c.ErrLogger)
		log.Printf("FormatterFunc:           %v\n", c.FormatterFunc)
		log.Printf("Drop:                    %v\n", c.Drop)
		log.Printf("UseGzip:                 %v\n", c.UseGzip)
		log.Printf("KinesisShards:           %v\n", c.KinesisShards)
		log.Printf("L2met_BufferSize:        %v\n", c.L2met_BufferSize)
		log.Printf("L2met_Concurrency:       %v\n", c.L2met_Concurrency)
		log.Printf("L2met_FlushInterval:     %v\n", c.L2met_FlushInterval)
		log.Printf("L2met_MaxPartitions:     %v\n", c.L2met_MaxPartitions)
		log.Printf("L2met_OutletAPIToken:    %v\n", c.L2met_OutletAPIToken)
		log.Printf("L2met_OutletInterval:    %v\n", c.L2met_OutletInterval)
		log.Printf("L2met_OutletRetries:     %v\n", c.L2met_OutletRetries)
		log.Printf("L2met_OutletTtl:         %v\n", c.L2met_OutletTtl)
		log.Printf("L2met_ReceiverDeadline:  %v\n", c.L2met_ReceiverDeadline)
		log.Printf("L2met_UseDataDogOutlet:  %v\n", c.L2met_UseDataDogOutlet)
        log.Printf("L2met_UseNewRelicOutlet: %v\n", c.L2met_UseNewRelicOutlet)
		log.Println("---------------------------------------------------------")
	}

	return c, nil
}

// validateURL validates the url provided as a string.
func validateURL(u string) (*url.URL, error) {
	oURL, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("Error parsing URL: %v", err.Error())
	}

	switch oURL.Scheme {
	case "http", "https":
		// no-op these are good
	default:
		return nil, fmt.Errorf("Invalid URL scheme: %v", u)
	}

	if oURL.Host == "" {
		return nil, fmt.Errorf("No host: %v", u)
	}

	parts := strings.Split(oURL.Host, ":")

	if len(parts) > 2 {
		return nil, fmt.Errorf("Invalid host specified: %v", u)
	}

	if len(parts) == 2 {
		_, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("Invalid port specified: %v", u)
		}
	}

	return oURL, nil
}

func getConfig() (shuttle.Config, error) {
	c, err := parseFlags(shuttle.NewConfig())
	if err != nil {
		return c, err
	}

	if c.MaxAttempts < 1 {
		return c, fmt.Errorf("-max-attempts must be >= 1 (got: %v)", c.MaxAttempts)
	}

	c.LogsURL = determineLogsURL(os.Getenv("LOGPLEX_URL"), os.Getenv("LOGS_URL"), c.LogsURL)
	oURL, err := validateURL(c.LogsURL)
	if err != nil {
		return c, err
	}

	if oURL.User == nil {
		oURL.User = url.UserPassword("token", c.Appname)
	}

	c.FormatterFunc = determineOutputFormatter(oURL)

	c.LogsURL = oURL.String()

	c.ComputeHeader()

	return c, nil
}

func determineOutputFormatter(u *url.URL) shuttle.NewHTTPFormatterFunc {
	if detectKinesis.MatchString(u.Host) {
		return shuttle.NewKinesisFormatter
	}
	return shuttle.NewLogplexBatchFormatter
}

func main() {
	config, err := getConfig()
	if err != nil {
		errLogger.Fatalf("error=%q\n", err)
	}

	config.ID = version

	//if !useStdin() {
	//	errLogger.Fatalln(`error="No stdin detected."`)
	//}

   mchan := metchan.New(
			config.Verbose,
			config.Quiet,
			config.L2met_OutletAPIToken,
			config.L2met_Concurrency,
			config.L2met_BufferSize,
			config.Appname,
			config.Hostname)
    mchan.Start()

    st := store.NewMemStore()
    if !config.Quiet && config.Verbose {
        log.Println("at=initialized-mem-store")
    }
	s := shuttle.NewShuttle(config, st, mchan)

	// Setup the loggers before doing anything else
	if logToSyslog {
		s.Logger, err = syslog.NewLogger(syslog.LOG_INFO|syslog.LOG_SYSLOG, 0)
		if err != nil {
			errLogger.Fatalf(`error="Unable to setup syslog logger: %s\n"`, err)
		}
		s.ErrLogger, err = syslog.NewLogger(syslog.LOG_ERR|syslog.LOG_SYSLOG, 0)
		if err != nil {
			errLogger.Fatalf(`error="Unable to setup syslog error logger: %s\n"`, err)
		}
	} else {
		s.Logger = logger
		s.ErrLogger = errLogger
	}

	s.LoadReader(os.Stdin)

    rdr := reader.New(config, st)
    rdr.Mchan = mchan
    outlt := outlet.NewDataDogOutlet(config, rdr)
    outlt.Mchan = mchan
    outlt.Start()

	s.Launch()

	go LogFmtMetricsEmitter(s.MetricsRegistry, config.StatsSource, config.StatsInterval, s.Logger)

	// blocks until the readers all exit
	s.WaitForReadersToFinish()

	// Shutdown the shuttle.
	s.Land()
}
