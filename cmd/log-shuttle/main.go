package main

import (
    "flag"
    "fmt"
    "github.com/op/go-logging"
    "log/syslog"
    "net"
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
    log = logging.MustGetLogger("log-shuttle")
    logToSyslog bool
)

var version = "1.0" // log-shuttle version, set with linker

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

func unmapInputFormat(i int) string {
    switch i {
        case shuttle.InputFormatRFC3164:
            return "rfc3164"
        case shuttle.InputFormatRFC5424:
            return "rfc5424"
        default: //shuttle.InputFormatRaw
            return "raw"
    }
}

func mapProtocol(p string) (shuttle.Protocol, error) {
    switch strings.ToLower(p) {
        case "udp":
            return shuttle.UDP, nil
        case "tcp":
            return shuttle.TCP, nil
    }
    return 0, fmt.Errorf("Unknown protocol: %s", p)
}

func unmapProtocol(p shuttle.Protocol) (string) {
    switch p {
        case shuttle.UDP:
            return "udp"
        default:
            return "tcp"
    }
}

// determineLogsURL from the various options favoring each one in turn
//func determineLogsURL(logsURL, cmdLineURL string) string {
//    var envURL string
//
//    if len(logsURL) > 0 {
//        envURL = logsURL
//    }
//
//    if len(cmdLineURL) > 0 {
//        if len(envURL) > 0 {
//            log.Warning("Use of both an evnironment variable ($LOGS_URL) and -logs-url, using -logs-url option")
//        }
//        return cmdLineURL
//    }
//    return envURL
//}

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
    var inputFormat string
    var listenProtocol string

    flag.StringVar(&c.Appname, "appname", c.Appname, "The app-name field (Can also be given as env var: APP_NAME)")
    flag.StringVar(&c.Appname, "a", c.Appname, "The app-name field (shorthand)")
    flag.IntVar(&c.BatchSize, "batch-size", c.BatchSize, "Number of messages to pack into an application/logplex-1 http request (Can also be given as env var: BATCH_SIZE)")
    flag.IntVar(&c.BatchSize, "b", c.BatchSize, "Number of messages to pack into an http request (shorthand)")
    flag.BoolVar(&c.UseGzip, "gzip", c.UseGzip, "POST using gzip compression")
    flag.BoolVar(&c.UseGzip, "g", c.UseGzip, "POST using gzip compression (shorthand)")
    flag.StringVar(&inputFormat, "input-format", "raw", "raw (default), rfc3164 (syslog(3)), rfc5424 (Can also be given as env var: INPUT_FORMAT)")
    flag.StringVar(&inputFormat, "i", "raw", "raw (default), rfc3164 (syslog(3)), rfc5424 (shorthand)")
    flag.IntVar(&c.KinesisShards, "kinesis-shards", c.KinesisShards, "Number of unique partition keys to use per app (Can also be given as env var: KINESIS_SHARDS)")
    flag.IntVar(&c.KinesisShards, "k", c.KinesisShards, "Number of unique partition keys to use per app (shorthand)")
    flag.StringVar(&c.LogsURL, "logs-url", c.LogsURL, "The receiver of the log data (Can also be given as env var: LOGS_URL)")
    flag.StringVar(&c.LogsURL, "l", c.LogsURL, "The receiver of the log data (shorthand)")
    flag.IntVar(&c.MaxLineLength, "max-line-length", c.MaxLineLength, "Number of bytes that the backend allows per line (Can also be given as env var: MAX_LINE_LENGTH)")
    flag.IntVar(&c.MaxLineLength, "m", c.MaxLineLength, "Number of bytes that the backend allows per line (shorthand)")
    flag.StringVar(&listenProtocol, "protocol", "tcp", "Listener protocol (tcp/udp)")
    flag.StringVar(&listenProtocol, "r", "tcp", "Listener protocol (tcp/udp) (shorthand)")
    flag.IntVar(&c.Port, "port", c.Port, "Listener Port")
    flag.IntVar(&c.Port, "p", c.Port, "Listener Port (shorthand)")
    flag.BoolVar(&c.Server, "server", c.Server, "Run a socker server instead of reading from stdin")
    flag.BoolVar(&c.Server, "s", c.Server, "Run a socker server instead of reading from stdin (shorthand)")
    flag.StringVar(&c.L2met_Tags, "tags", c.L2met_Tags, "(l2met) Additional tags to add to all metrics (comma-separated: environment:staging,clustertype:kubernetes) (Can also be given as env var: TAGS)")
    flag.StringVar(&c.L2met_Tags, "t", c.L2met_Tags, "(l2met) Additional tags to add to all metrics (shorthand)")
    flag.BoolVar(&c.Verbose, "verbose", c.Verbose, "Enable verbose debug info (Can also be given as env var: LOG_LEVEL)")
    flag.BoolVar(&c.Verbose, "v", c.Verbose, "Enable verbose debug info (shorthand)")
    flag.BoolVar(&printVersion, "version", printVersion, "Print log-shuttle version & exit")
    flag.BoolVar(&printVersion, "V", printVersion, "Print log-shuttle version & exit (shorthand)")

    flag.BoolVar(&c.SkipVerify, "skip-verify", c.SkipVerify, "Skip the verification of HTTPS server certificate")
    flag.BoolVar(&c.Drop, "drop", c.Drop, "Drop (default) logs or backup & block stdin")
    flag.BoolVar(&logToSyslog, "log-to-syslog", logToSyslog, "Log to syslog instead of stderr")
    flag.StringVar(&c.Prival, "prival", c.Prival, "The primary value of the rfc5424 header")
    flag.StringVar(&c.Version, "syslog-version", c.Version, "The version of syslog")
    flag.StringVar(&c.Procid, "procid", c.Procid, "The procid field for the syslog header")
    flag.StringVar(&c.Hostname, "hostname", c.Hostname, "The hostname field for the syslog header.")
    flag.StringVar(&c.Msgid, "msgid", c.Msgid, "The msgid field for the syslog header.")
    flag.StringVar(&c.StatsSource, "stats-source", c.StatsSource, "When emitting stats, add source=<stats-source> to the stats.")
    flag.DurationVar(&c.StatsInterval, "stats-interval", c.StatsInterval, "How often to emit/reset stats.")
    flag.DurationVar(&c.WaitDuration, "wait", c.WaitDuration, "Duration to wait to flush messages to logs-url.")
    flag.DurationVar(&c.Timeout, "timeout", c.Timeout, "Duration to wait for a response from logs-url.")
    flag.IntVar(&c.MaxAttempts, "max-attempts", c.MaxAttempts, "Max number of retries.")
    flag.IntVar(&c.NumOutlets, "num-outlets", c.NumOutlets, "The number of outlets to run.")
    flag.IntVar(&c.BackBuff, "back-buff", c.BackBuff, "Number of batches to buffer before dropping.")

    flag.IntVar(&c.L2met_BufferSize, "buffer", c.L2met_BufferSize, "(l2met) Max number of items for all internal buffers.")
    flag.IntVar(&c.L2met_Concurrency, "concurrency", c.L2met_Concurrency, "(l2met) Number of running go routines for outlet or receiver.")
    flag.DurationVar(&c.L2met_FlushInterval, "flush-interval", c.L2met_FlushInterval, "(l2met) Time to wait before sending data to store or outlet. Example:60s 30s 1m")
    flag.BoolVar(&c.L2met_UseDataDogOutlet, "outlet-datadog", c.L2met_UseDataDogOutlet, "(l2met) Start the DataDog outlet.")
    flag.DurationVar(&c.L2met_OutletInterval, "outlet-interval", c.L2met_OutletInterval, "(l2met) Time to wait before outlets read buckets from the store. Example:60s 30s 1m")
    //flag.BoolVar(&c.L2met_UseNewRelicOutlet, "outlet-newrelic", c.L2met_UseNewRelicOutlet, "(l2met) Start the NewRelic outlet.")
    flag.IntVar(&c.L2met_OutletRetries, "outlet-retry", c.L2met_OutletRetries, "(l2met) Number of attempts to outlet metrics.")
    flag.StringVar(&c.L2met_OutletAPIToken, "outlet-token", c.L2met_OutletAPIToken, "(l2met) Outlet API Token. (Can also be given as env var: OUTLET_TOKEN)")
    flag.DurationVar(&c.L2met_OutletTtl, "outlet-ttl", c.L2met_OutletTtl, "(l2met) Timeout set on outlet HTTP requests.")
    flag.Uint64Var(&c.L2met_MaxPartitions, "partitions", c.L2met_MaxPartitions, "(l2met) Number of partitions to use for outlets.")
    flag.Int64Var(&c.L2met_ReceiverDeadline, "recv-deadline", c.L2met_ReceiverDeadline, "(l2met) Number of time units to pass before dropping incoming logs.")
    flag.Parse()

    if printVersion {
        log.Info(version)
        os.Exit(0)
    }

    var err error
    c.InputFormat, err = mapInputFormat(envConfigStrings(inputFormat, "raw", "INPUT_FORMAT"))
    if err != nil {
        return c, err
    }

    if c.MaxAttempts < 1 {
        return c, fmt.Errorf("-max-attempts must be >= 1 (got: %d)", c.MaxAttempts)
    }

    if c.Server {
        c.Protocol, err = mapProtocol(listenProtocol)
        if err != nil {
            return c, err
        }
    } /* else {
        if !useStdin() {
            errLogger.Fatalln(`error="No stdin detected."`)
        }
    } */

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

func setupLoggers(c shuttle.Config) logging.Level {
    loggerBackend := logging.NewLogBackend(os.Stdout, "", 0)
    backendFormatter := logging.NewBackendFormatter(loggerBackend, format)
    backendLevel := logging.AddModuleLevel(backendFormatter)
    var level logging.Level
    if c.Verbose == true {
        level = logging.DEBUG
    } else if c.Quiet == true {
        level = logging.WARNING
    } else {
        lvl, exists := os.LookupEnv("LOG_LEVEL")
        if !exists {
            level = logging.INFO
        } else {
            var err error
            level, err = logging.LogLevel(strings.ToUpper(lvl))
            if err != nil {
                level = logging.INFO
            }
        }

    }
    backendLevel.SetLevel(level, "")
    errLogger := logging.NewLogBackend(os.Stderr, "", 0)
    errBackendLevel := logging.AddModuleLevel(errLogger)
    errBackendLevel.SetLevel(logging.ERROR, "")
    logging.SetBackend(backendLevel, errBackendLevel)
    return level
}

func envConfigStrings(configVal, defaultVal, envVarName string) (string) {
    if configVal == defaultVal {
        if envVal, exists := os.LookupEnv(envVarName); exists {
            return envVal
        }
        return defaultVal
    }
    return configVal
}

func envConfigInts(configVal, defaultVal int, envVarName string) (int) {
    if configVal == defaultVal {
        if envVal, exists := os.LookupEnv(envVarName); exists {
            if envValInt, err := strconv.Atoi(envVal); err == nil {
                return envValInt
            } else {
                log.Debugf("Error converting value of ENV %q to int (got: %q) - %v", envVarName, envVal, err)
                log.Warningf("Could not convert value of ENV %q to int (got: %q).  Using default value: %q", envVarName, envVal, defaultVal)
                return defaultVal
            }
        }
        return defaultVal
    }
    return configVal
}

func getConfig() (shuttle.Config, error) {
    c, err := parseFlags(shuttle.NewConfig())
    if err != nil {
        return c, err
    }

    logLevel := setupLoggers(c)

    c.Appname = envConfigStrings(c.Appname, shuttle.DefaultAppName, "APP_NAME")
    c.BatchSize = envConfigInts(c.BatchSize, shuttle.DefaultBatchSize, "BATCH_SIZE")
    c.InputFormat = envConfigInts(c.InputFormat, shuttle.DefaultInputFormat, "INPUT_FORMAT")
    c.KinesisShards = envConfigInts(c.KinesisShards, shuttle.DefaultKinesisShards, "KINESIS_SHARDS")
    c.LogsURL = envConfigStrings(c.LogsURL, shuttle.DefaultLogsURL, "LOGS_URL")
    c.MaxLineLength = envConfigInts(c.MaxLineLength, shuttle.DefaultMaxLineLength, "MAX_LINE_LENGTH")
    c.L2met_OutletAPIToken = envConfigStrings(c.L2met_OutletAPIToken, shuttle.Default_l2met_OutletAPIToken, "OUTLET_TOKEN")
    c.L2met_Tags = envConfigStrings(c.L2met_Tags, shuttle.Default_l2met_Tags, "TAGS")
    c.BatchSize = envConfigInts(c.BatchSize, shuttle.DefaultBatchSize, "BATCH_SIZE")

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

    c.ID = version

    log.Debug("-------------------- Config Settings --------------------")
    log.Debugf("Quiet:                   %t", c.Quiet)
    log.Debugf("Verbose:                 %t", c.Verbose)
    log.Debugf("LogLevel:                %s", logLevel.String())
    log.Debugf("SkipVerify:              %t", c.SkipVerify)
    log.Debugf("Prival:                  %s", c.Prival)
    log.Debugf("Version:                 %s", c.Version)
    log.Debugf("Procid:                  %s", c.Procid)
    log.Debugf("Appname:                 %s", c.Appname)
    log.Debugf("Hostname:                %s", c.Hostname)
    log.Debugf("MaxLineLength:           %d", c.MaxLineLength)
    log.Debugf("Msgid:                   %s", c.Msgid)
    log.Debugf("LogsURL:                 %s", Password(c.LogsURL))
    log.Debugf("StatsSource:             %s", c.StatsSource)
    log.Debugf("StatsInterval:           %v", c.StatsInterval)
    log.Debugf("MaxAttempts:             %d", c.MaxAttempts)
    log.Debugf("InputFormat:             %s", unmapInputFormat(c.InputFormat))
    log.Debugf("NumOutlets:              %d", c.NumOutlets)
    log.Debugf("WaitDuration:            %v", c.WaitDuration)
    log.Debugf("BatchSize:               %d", c.BatchSize)
    log.Debugf("BackBuff:                %d", c.BackBuff)
    log.Debugf("Timeout:                 %v", c.Timeout)
    log.Debugf("ID:                      %s", c.ID)
    log.Debugf("FormatterFunc:           %T", c.FormatterFunc)
    log.Debugf("Drop:                    %t", c.Drop)
    log.Debugf("UseGzip:                 %t", c.UseGzip)
    log.Debugf("KinesisShards:           %d", c.KinesisShards)
    log.Debugf("Server:                  %t", c.Server)
    log.Debugf("Protocol:                %s", unmapProtocol(c.Protocol))
    log.Debugf("Port:                    %d", c.Port)
    log.Debugf("L2met_BufferSize:        %d", c.L2met_BufferSize)
    log.Debugf("L2met_Concurrency:       %d", c.L2met_Concurrency)
    log.Debugf("L2met_FlushInterval:     %v", c.L2met_FlushInterval)
    log.Debugf("L2met_MaxPartitions:     %d", c.L2met_MaxPartitions)
    log.Debugf("L2met_OutletAPIToken:    %s", Password(c.L2met_OutletAPIToken))
    log.Debugf("L2met_OutletInterval:    %v", c.L2met_OutletInterval)
    log.Debugf("L2met_OutletRetries:     %d", c.L2met_OutletRetries)
    log.Debugf("L2met_OutletTtl:         %v", c.L2met_OutletTtl)
    log.Debugf("L2met_ReceiverDeadline:  %d", c.L2met_ReceiverDeadline)
    log.Debugf("L2met_Tags:              %s", c.L2met_Tags)
    log.Debugf("L2met_UseDataDogOutlet:  %t", c.L2met_UseDataDogOutlet)
    log.Debugf("L2met_UseNewRelicOutlet: %t", c.L2met_UseNewRelicOutlet)
    log.Debug("---------------------------------------------------------")

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

    var tags = []string{}
    if len(config.L2met_Tags) > 0 {
        tags = append(tags, config.L2met_Tags)
    }
    if len(config.Appname) > 0 {
        tags = append(tags, "app:" + config.Appname)
    }

    mchan := metchan.New(
        config.L2met_OutletAPIToken,
        config.L2met_Concurrency,
        config.L2met_BufferSize,
        config.Appname,
        config.Hostname,
        strings.Join(tags, ","))
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

    if config.Server {
        if config.Protocol == shuttle.UDP {
            // UDP socket server
            addr := net.UDPAddr{
                Port: config.Port,
                IP: net.ParseIP("127.0.0.1"),
            }
            conn, err := net.ListenUDP("udp", &addr)
            if err != nil {
                log.Panicf("UDP Listen Error: %v", err)
            }

            s.LoadReader(conn)
        } else {
            // TCP socket server
            // listen on all interfaces
            server, err := net.Listen("tcp", ":" + strconv.Itoa(config.Port))
            if err != nil {
                log.Panicf("TCP Listen Error: %v", err)
            }

            // accept connection on port
            conn, err := server.Accept()
            if err != nil {
                log.Panicf("TCP Accept Error: %v", err)
            }
            s.TCPServer = server
            s.LoadReader(conn)
        }
    } else {
        s.LoadReader(os.Stdin)
    }

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
