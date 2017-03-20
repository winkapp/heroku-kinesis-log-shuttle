package main

import (
    "flag"
    "fmt"
    "github.com/op/go-logging"
    "log/syslog"
    "net/url"
    "os"
    "regexp"
    "strconv"
    "strings"

    "github.com/winkapp/log-shuttle"
    "github.com/winkapp/log-shuttle/l2met/metchan"
    "github.com/winkapp/log-shuttle/l2met/outlet"
    "github.com/winkapp/log-shuttle/l2met/reader"
    "github.com/winkapp/log-shuttle/l2met/store"
)

var detectKinesis = regexp.MustCompile(`\Akinesis.[[:alpha:]]{2}-[[:alpha:]]{2,}-[[:digit:]]\.amazonaws\.com\z`)

// Default loggers to stdout and stderr
var (
    log         = logging.MustGetLogger("log-shuttle")
    logToSyslog bool
)

var version = "" // log-shuttle version, set with linker

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
func determineLogsURL(logsURL, cmdLineURL string) string {
    var envURL string

    if len(logsURL) > 0 {
        envURL = logsURL
    }

    if len(cmdLineURL) > 0 {
        if len(envURL) > 0 {
            log.Warning("Use of both an evnironment variable ($LOGS_URL) and -logs-url, using -logs-url option")
        }
        return cmdLineURL
    }
    return envURL
}

// Example format string. Everything except the message has a custom color
// which is dependent on the log level. Many fields have a custom output
// formatting too, eg. the time returns the hour down to the milli second.
var format = logging.MustStringFormatter(
    `%{color}%{time:2006-01-02 15:04:05} - %{level:-7s} - %{shortfile:-20s} - %{color:reset} %{message}`,
)

type Password string

func (p Password) Redacted() interface{} {
    return logging.Redact(string(p))
}

// parseFlags overrides the properties of the given config using the provided
// command-line flags.  Any option not overridden by a flag will be untouched.
func parseFlags(c shuttle.Config) (shuttle.Config, error) {
    var printVersion bool

    flag.BoolVar(&c.Verbose, "verbose", c.Verbose, "Enable verbose debug info.")
    flag.BoolVar(&c.SkipVerify, "skip-verify", c.SkipVerify, "Skip the verification of HTTPS server certificate.")
    flag.BoolVar(&c.UseGzip, "gzip", c.UseGzip, "POST using gzip compression.")
    flag.BoolVar(&c.Drop, "drop", c.Drop, "Drop (default) logs or backup & block stdin.")
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
    flag.DurationVar(&c.StatsInterval, "stats-interval", c.StatsInterval, "How often to emit/reset stats.")
    flag.DurationVar(&c.WaitDuration, "wait", c.WaitDuration, "Duration to wait to flush messages to logs-url.")
    flag.DurationVar(&c.Timeout, "timeout", c.Timeout, "Duration to wait for a response from logs-url.")
    flag.IntVar(&c.MaxAttempts, "max-attempts", c.MaxAttempts, "Max number of retries.")
    flag.IntVar(&c.NumOutlets, "num-outlets", c.NumOutlets, "The number of outlets to run.")
    flag.IntVar(&c.BatchSize, "batch-size", c.BatchSize, "Number of messages to pack into an application/logplex-1 http request.")
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

    loggerBackend := logging.NewLogBackend(os.Stdout, "", 0)
    backendFormatter := logging.NewBackendFormatter(loggerBackend, format)
    backendLevel := logging.AddModuleLevel(backendFormatter)
    if c.Verbose == true {
        backendLevel.SetLevel(logging.DEBUG, "")
    } else if c.Quiet == true {
        backendLevel.SetLevel(logging.WARNING, "")
    } else {
        backendLevel.SetLevel(logging.INFO, "")
    }

    errLogger := logging.NewLogBackend(os.Stderr, "", 0)

    errBackendLevel := logging.AddModuleLevel(errLogger)
    errBackendLevel.SetLevel(logging.ERROR, "")

    logging.SetBackend(backendLevel, errBackendLevel)

    if printVersion {
        log.Info(version)
        os.Exit(0)
    }

    var err error
    c.InputFormat, err = mapInputFormat(inputFormat)
    if err != nil {
        return c, err
    }

    log.Debug("-------------------- Config Settings --------------------")
    log.Debugf("MaxLineLength:           %v", c.MaxLineLength)
    log.Debugf("Quiet:                   %v", c.Quiet)
    log.Debugf("Verbose:                 %v", c.Verbose)
    log.Debugf("SkipVerify:              %v", c.SkipVerify)
    log.Debugf("Prival:                  %v", c.Prival)
    log.Debugf("Version:                 %v", c.Version)
    log.Debugf("Procid:                  %v", c.Procid)
    log.Debugf("Appname:                 %v", c.Appname)
    log.Debugf("Hostname:                %v", c.Hostname)
    log.Debugf("Msgid:                   %v", c.Msgid)
    log.Debugf("LogsURL:                 %v", Password(c.LogsURL))
    log.Debugf("StatsSource:             %v", c.StatsSource)
    log.Debugf("StatsInterval:           %v", c.StatsInterval)
    log.Debugf("MaxAttempts:             %v", c.MaxAttempts)
    log.Debugf("InputFormat:             %v", c.InputFormat)
    log.Debugf("NumOutlets:              %v", c.NumOutlets)
    log.Debugf("WaitDuration:            %v", c.WaitDuration)
    log.Debugf("BatchSize:               %v", c.BatchSize)
    log.Debugf("BackBuff:                %v", c.BackBuff)
    log.Debugf("Timeout:                 %v", c.Timeout)
    log.Debugf("ID:                      %v", c.ID)
    log.Debugf("FormatterFunc:           %v", c.FormatterFunc)
    log.Debugf("Drop:                    %v", c.Drop)
    log.Debugf("UseGzip:                 %v", c.UseGzip)
    log.Debugf("KinesisShards:           %v", c.KinesisShards)
    log.Debugf("L2met_BufferSize:        %v", c.L2met_BufferSize)
    log.Debugf("L2met_Concurrency:       %v", c.L2met_Concurrency)
    log.Debugf("L2met_FlushInterval:     %v", c.L2met_FlushInterval)
    log.Debugf("L2met_MaxPartitions:     %v", c.L2met_MaxPartitions)
    log.Debugf("L2met_OutletAPIToken:    %v", Password(c.L2met_OutletAPIToken))
    log.Debugf("L2met_OutletInterval:    %v", c.L2met_OutletInterval)
    log.Debugf("L2met_OutletRetries:     %v", c.L2met_OutletRetries)
    log.Debugf("L2met_OutletTtl:         %v", c.L2met_OutletTtl)
    log.Debugf("L2met_ReceiverDeadline:  %v", c.L2met_ReceiverDeadline)
    log.Debugf("L2met_UseDataDogOutlet:  %v", c.L2met_UseDataDogOutlet)
    log.Debugf("L2met_UseNewRelicOutlet: %v", c.L2met_UseNewRelicOutlet)
    log.Debug("---------------------------------------------------------")

    return c, nil
}

// validateURL validates the url provided as a string.
func validateURL(u string) (*url.URL, error) {
    oURL, err := url.Parse(u)
    if err != nil {
        return nil, fmt.Errorf("Error parsing URL: %s", err.Error())
    }

    switch oURL.Scheme {
    case "http", "https":
        // no-op these are good
    default:
        return nil, fmt.Errorf("Invalid URL scheme: %s", u)
    }

    if oURL.Host == "" {
        return nil, fmt.Errorf("No host: %s", u)
    }

    parts := strings.Split(oURL.Host, ":")

    if len(parts) > 2 {
        return nil, fmt.Errorf("Invalid host specified: %s", u)
    }

    if len(parts) == 2 {
        _, err := strconv.Atoi(parts[1])
        if err != nil {
            return nil, fmt.Errorf("Invalid port specified: %s", u)
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
        return c, fmt.Errorf("-max-attempts must be >= 1 (got: %d)", c.MaxAttempts)
    }

    c.LogsURL = determineLogsURL(os.Getenv("LOGS_URL"), c.LogsURL)
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
        log.Fatalf("error=%q\n", err)
    }

    config.ID = version

    //if !useStdin() {
    //    errLogger.Fatalln(`error="No stdin detected."`)
    //}

    mchan := metchan.New(
        config.L2met_OutletAPIToken,
        config.L2met_Concurrency,
        config.L2met_BufferSize,
        config.Appname,
        config.Hostname)
    mchan.Start()

    st := store.NewMemStore()

    log.Debug("at=initialized-mem-store")

    s := shuttle.NewShuttle(config, st, mchan)

    // Setup the loggers before doing anything else
    if logToSyslog {
        s.Logger, err = syslog.NewLogger(syslog.LOG_INFO|syslog.LOG_SYSLOG, 0)
        if err != nil {
            log.Fatalf(`error="Unable to setup syslog logger: %s\n"`, err)
        }
        s.ErrLogger, err = syslog.NewLogger(syslog.LOG_ERR|syslog.LOG_SYSLOG, 0)
        if err != nil {
            log.Fatalf(`error="Unable to setup syslog error logger: %s\n"`, err)
        }
    } else {
        //s.Logger = logger
        //s.ErrLogger = errLogger
    }

    s.LoadReader(os.Stdin)

    rdr := reader.New(config, st)
    rdr.Mchan = mchan
    outlt := outlet.NewDataDogOutlet(config, rdr)
    outlt.Mchan = mchan
    outlt.Start()

    s.Launch()

    go LogFmtMetricsEmitter(s.MetricsRegistry, config.StatsSource, config.StatsInterval)

    // blocks until the readers all exit
    s.WaitForReadersToFinish()

    // Shutdown the shuttle.
    s.Land()
}
