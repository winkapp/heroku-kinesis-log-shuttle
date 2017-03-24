package shuttle

import (
    "fmt"
    "log"
    "time"
    "os"
)

// Input format constants.
// TODO: ensure these are really used properly
const (
    InputFormatRaw = iota
    InputFormatRFC3164
    InputFormatRFC5424
)

// Default option values
const (
    DefaultMaxLineLength            = 10000
    DefaultInputFormat              = InputFormatRaw
    DefaultBackBuff                 = 50
    DefaultTimeout                  = 5 * time.Second
    DefaultWaitDuration             = 250 * time.Millisecond
    DefaultMaxAttempts              = 3
    DefaultStatsInterval            = 0 * time.Second
    DefaultStatsSource              = ""
    DefaultQuiet                    = false
    DefaultVerbose                  = false
    DefaultSkipVerify               = false
    DefaultPriVal                   = "190"
    DefaultVersion                  = "1"
    DefaultProcID                   = "shuttle"
    DefaultAppName                  = "token"
    DefaultHostname                 = "shuttle"
    DefaultMsgID                    = "- -"
    DefaultLogsURL                  = ""
    DefaultNumOutlets               = 4
    DefaultBatchSize                = 500
    DefaultID                       = ""
    DefaultDrop                     = true
    DefaultUseGzip                  = false
    DefaultKinesisShards            = 1

    // l2met
    Default_l2met_BufferSize        = 1024
    Default_l2met_Concurrency       = 10

    Default_l2met_FlushInterval     = time.Second
    Default_l2met_MaxPartitions     = uint64(1)
    Default_l2met_OutletAPIToken    = ""
    Default_l2met_OutletInterval    = time.Second
    Default_l2met_OutletRetries     = 2
    Default_l2met_OutletTtl         = time.Second*2
    Default_l2met_ReceiverDeadline  = 2
    Default_l2met_Tags              = ""
    Default_l2met_UseDataDogOutlet  = true
    Default_l2met_UseNewRelicOutlet = false
)

const (
    errDrop errType = iota
    errLost
)

// Defaults that can't be constants
var (
    DefaultFormatterFunc = NewLogplexBatchFormatter
)

type errType int

type errData struct {
    count int
    since time.Time
    eType errType
}

// Config holds the various config options for a shuttle
type Config struct {
    MaxLineLength                       int
    BackBuff                            int
    BatchSize                           int
    NumOutlets                          int
    InputFormat                         int
    MaxAttempts                         int
    KinesisShards                       int
    LogsURL                             string
    Prival                              string
    Version                             string
    Procid                              string
    Hostname                            string
    Appname                             string
    Msgid                               string
    StatsSource                         string
    SkipVerify                          bool
    Quiet                               bool
    Verbose                             bool
    UseGzip                             bool
    Drop                                bool
    WaitDuration                        time.Duration
    Timeout                             time.Duration
    StatsInterval                       time.Duration
    lengthPrefixedSyslogFrameHeaderSize int
    syslogFrameHeaderFormat             string
    ID                                  string
    FormatterFunc                       NewHTTPFormatterFunc

    // l2Met
    L2met_BufferSize                    int
    L2met_Concurrency                   int
    L2met_FlushInterval                 time.Duration
    L2met_MaxPartitions                 uint64
    L2met_OutletAPIToken                string
    L2met_OutletInterval                time.Duration
    L2met_OutletRetries                 int
    L2met_OutletTtl                     time.Duration
    L2met_ReceiverDeadline              int64
    L2met_Tags                          string
    L2met_UseDataDogOutlet              bool
    L2met_UseNewRelicOutlet             bool

    // Loggers
    Logger                              *log.Logger
    ErrLogger                           *log.Logger
}

// NewConfig returns a newly created Config, filled in with defaults
func NewConfig() Config {
    host, err := os.Hostname()
    if err != nil {
        host = DefaultHostname
    }

    shuttleConfig := Config{
        MaxLineLength:           DefaultMaxLineLength,
        Quiet:                   DefaultQuiet,
        Verbose:                 DefaultVerbose,
        SkipVerify:              DefaultSkipVerify,
        Prival:                  DefaultPriVal,
        Version:                 DefaultVersion,
        Procid:                  DefaultProcID,
        Appname:                 DefaultAppName,
        Hostname:                host,
        Msgid:                   DefaultMsgID,
        LogsURL:                 DefaultLogsURL,
        StatsSource:             DefaultStatsSource,
        StatsInterval:           time.Duration(DefaultStatsInterval),
        MaxAttempts:             DefaultMaxAttempts,
        InputFormat:             DefaultInputFormat,
        NumOutlets:              DefaultNumOutlets,
        WaitDuration:            time.Duration(DefaultWaitDuration),
        BatchSize:               DefaultBatchSize,
        BackBuff:                DefaultBackBuff,
        Timeout:                 time.Duration(DefaultTimeout),
        ID:                      DefaultID,
        Logger:                  discardLogger,
        ErrLogger:               discardLogger,
        FormatterFunc:           DefaultFormatterFunc,
        Drop:                    DefaultDrop,
        UseGzip:                 DefaultUseGzip,
        KinesisShards:           DefaultKinesisShards,

        // l2met
        L2met_BufferSize:        Default_l2met_BufferSize,
        L2met_Concurrency:       Default_l2met_Concurrency,
        L2met_FlushInterval:     time.Duration(Default_l2met_FlushInterval),
        L2met_MaxPartitions:     Default_l2met_MaxPartitions,
        L2met_OutletAPIToken:    Default_l2met_OutletAPIToken,
        L2met_OutletInterval:    time.Duration(Default_l2met_OutletInterval),
        L2met_OutletRetries:     Default_l2met_OutletRetries,
        L2met_OutletTtl:         time.Duration(Default_l2met_OutletTtl),
        L2met_Tags:              Default_l2met_Tags,
        L2met_ReceiverDeadline:  Default_l2met_ReceiverDeadline,
        L2met_UseDataDogOutlet:  Default_l2met_UseDataDogOutlet,
        L2met_UseNewRelicOutlet: Default_l2met_UseNewRelicOutlet,
    }

    shuttleConfig.ComputeHeader()

    return shuttleConfig
}

// ComputeHeader computes the syslogFrameHeaderFormat once so we don't have to
// do that for every formatter itteration
// Should be called after setting up the rest of the config or if the config
// changes
func (c *Config) ComputeHeader() {
    // This is here to pre-compute this so other's don't have to later
    c.lengthPrefixedSyslogFrameHeaderSize = len(c.Prival) + len(c.Version) + len(LogplexBatchTimeFormat) +
        len(c.Hostname) + len(c.Appname) + len(c.Procid) + len(c.Msgid) + 8 // spaces, < & >

    c.syslogFrameHeaderFormat = fmt.Sprintf("%s <%s>%s %s %s %s %s %s ",
        "%d",
        c.Prival,
        c.Version,
        "%s", // The time should be put here
        c.Hostname,
        c.Appname,
        c.Procid,
        c.Msgid)
}
