
-- create tables hits and hits_tmp

DROP TABLE IF EXISTS hits_tmp;

CREATE TABLE IF NOT EXISTS hits_tmp
(
    WatchID                    text,         -- UInt64 *   #
    JavaEnable                 smallint,     -- UInt8
    Title                      text,         -- String *
    GoodEvent                  smallint,     -- Int16
    EventTime                  timestamp,    -- DateTime *
    EventDate                  Date,         -- Date *
    CounterID                  bigint,       -- UInt32 *
    ClientIP                   bigint,       -- UInt32 *
    ClientIP6                  text,         -- FixedString(16)
    RegionID                   bigint,       -- UInt32 *
    UserID                     text,         -- UInt64 *   #
    CounterClass               smallint,     -- Int8
    OS                         smallint,     -- UInt8
    UserAgent                  smallint,     -- UInt8
    URL                        text,         -- String *
    Referer                    text,         -- String *
    URLDomain                  text,         -- String
    RefererDomain              text,         -- String
    Refresh                    smallint,     -- UInt8 *
    IsRobot                    smallint,     -- UInt8
    RefererCategories          text,         -- Array(UInt16)
    URLCategories              text,         -- Array(UInt16)
    URLRegions                 text,         -- Array(UInt32)
    RefererRegions             text,         -- Array(UInt32)
    ResolutionWidth            int,          -- UInt16 *
    ResolutionHeight           int,          -- UInt16
    ResolutionDepth            smallint,     -- UInt8
    FlashMajor                 smallint,     -- UInt8
    FlashMinor                 smallint,     -- UInt8
    FlashMinor2                text,         -- String
    NetMajor                   smallint,     -- UInt8
    NetMinor                   smallint,     -- UInt8
    UserAgentMajor             int,          -- UInt16
    UserAgentMinor             text,         -- FixedString(2)
    CookieEnable               smallint,     -- UInt8
    JavascriptEnable           smallint,     -- UInt8
    IsMobile                   smallint,     -- UInt8
    MobilePhone                smallint,     -- UInt8  *
    MobilePhoneModel           text,         -- String *
    Params                     text,         -- String
    IPNetworkID                bigint,       -- UInt32
    TraficSourceID             smallint,     -- Int8   *
    SearchEngineID             int,          -- UInt16 *
    SearchPhrase               text,         -- String *
    AdvEngineID                smallint,     -- UInt8  *
    IsArtifical                smallint,     -- UInt8
    WindowClientWidth          int,          -- UInt16 *
    WindowClientHeight         int,          -- UInt16 *
    ClientTimeZone             smallint,     -- Int16
    ClientEventTime            timestamp,    -- DateTime
    SilverlightVersion1        smallint,     -- UInt8
    SilverlightVersion2        smallint,     -- UInt8
    SilverlightVersion3        bigint,       -- UInt32
    SilverlightVersion4        int,          -- UInt16
    PageCharset                text,         -- String
    CodeVersion                bigint,       -- UInt32
    IsLink                     smallint,     -- UInt8 *
    IsDownload                 smallint,     -- UInt8 *
    IsNotBounce                smallint,     -- UInt8
    FUniqID                    bigint,       -- UInt64    #
    HID                        bigint,       -- UInt32
    IsOldCounter               smallint,     -- UInt8
    IsEvent                    smallint,     -- UInt8
    IsParameter                smallint,     -- UInt8
    DontCountHits              smallint,     -- UInt8 *
    WithHash                   smallint,     -- UInt8
    HitColor                   text,         -- FixedString(1)
    UTCEventTime               timestamp,    -- DateTime
    Age                        smallint,     -- UInt8
    Sex                        smallint,     -- UInt8
    Income                     smallint,     -- UInt8
    Interests                  int,          -- UInt16
    Robotness                  smallint,     -- UInt8
    GeneralInterests           text,         -- Array(UInt16)
    RemoteIP                   bigint,       -- UInt32
    RemoteIP6                  text,         -- FixedString(16)
    WindowName                 int,          -- Int32
    OpenerName                 int,          -- Int32
    HistoryLength              smallint,     -- Int16
    BrowserLanguage            text,         -- FixedString(2)
    BrowserCountry             text,         -- FixedString(2)
    SocialNetwork              text,         -- String
    SocialAction               text,         -- String
    HTTPError                  int,          -- UInt16
    SendTiming                 int,          -- Int32
    DNSTiming                  int,          -- Int32
    ConnectTiming              int,          -- Int32
    ResponseStartTiming        int,          -- Int32
    ResponseEndTiming          int,          -- Int32
    FetchTiming                int,          -- Int32
    RedirectTiming             int,          -- Int32
    DOMInteractiveTiming       int,          -- Int32
    DOMContentLoadedTiming     int,          -- Int32
    DOMCompleteTiming          int,          -- Int32
    LoadEventStartTiming       int,          -- Int32
    LoadEventEndTiming         int,          -- Int32
    NSToDOMContentLoadedTiming int,          -- Int32
    FirstPaintTiming           int,          -- Int32
    RedirectCount              smallint,     -- Int8
    SocialSourceNetworkID      smallint,     -- UInt8
    SocialSourcePage           text,         -- String
    ParamPrice                 bigint,       -- Int64
    ParamOrderID               text,         -- String
    ParamCurrency              text,         -- FixedString(3)
    ParamCurrencyID            int,          -- UInt16
    GoalsReached               text,         -- Array(UInt32)
    OpenstatServiceName        text,         -- String
    OpenstatCampaignID         text,         -- String
    OpenstatAdID               text,         -- String
    OpenstatSourceID           text,         -- String
    UTMSource                  text,         -- String
    UTMMedium                  text,         -- String
    UTMCampaign                text,         -- String
    UTMContent                 text,         -- String
    UTMTerm                    text,         -- String
    FromTag                    text,         -- String
    HasGCLID                   smallint,     -- UInt8
    RefererHash                text,         -- UInt64     #
    URLHash                    text,         -- Uint64 *   #
    CLID                       bigint,       -- Uint32
    YCLID                      text,         -- UInt64     #
    ShareService               text,         -- String
    ShareURL                   text,         -- String
    ShareTitle                 text,         -- String
--     ParsedParams Nested(
         Key1 text, -- String
         Key2 text, -- String
         Key3 text, -- String
         Key4 text, -- String
         Key5 text, -- String
         ValueDouble text, -- Float64
--         ValueDouble Float64),
    IslandID   text,       -- FixedString(16)
    RequestNum bigint,     -- UInt32
    RequestTry smallint    -- UInt8
)
with(appendonly=true, orientation=column)
distributed by (userid);


DROP TABLE IF EXISTS hits;
CREATE TABLE IF NOT EXISTS hits
(
    WatchID                    bigint,       -- UInt64 *   #
    JavaEnable                 smallint,     -- UInt8
    Title                      text,         -- String *
    GoodEvent                  smallint,     -- Int16
    EventTime                  timestamp,    -- DateTime *
    EventDate                  Date,         -- Date *
    CounterID                  bigint,       -- UInt32 *
    ClientIP                   bigint,       -- UInt32 *
    ClientIP6                  text,         -- FixedString(16)
    RegionID                   bigint,       -- UInt32 *
    UserID                     bigint,       -- UInt64 *   #
    CounterClass               smallint,     -- Int8
    OS                         smallint,     -- UInt8
    UserAgent                  smallint,     -- UInt8
    URL                        text,         -- String *
    Referer                    text,         -- String *
    URLDomain                  text,         -- String
    RefererDomain              text,         -- String
    Refresh                    smallint,     -- UInt8 *
    IsRobot                    smallint,     -- UInt8
    RefererCategories          text,         -- Array(UInt16)
    URLCategories              text,         -- Array(UInt16)
    URLRegions                 text,         -- Array(UInt32)
    RefererRegions             text,         -- Array(UInt32)
    ResolutionWidth            int,          -- UInt16 *
    ResolutionHeight           int,          -- UInt16
    ResolutionDepth            smallint,     -- UInt8
    FlashMajor                 smallint,     -- UInt8
    FlashMinor                 smallint,     -- UInt8
    FlashMinor2                text,         -- String
    NetMajor                   smallint,     -- UInt8
    NetMinor                   smallint,     -- UInt8
    UserAgentMajor             int,          -- UInt16
    UserAgentMinor             text,         -- FixedString(2)
    CookieEnable               smallint,     -- UInt8
    JavascriptEnable           smallint,     -- UInt8
    IsMobile                   smallint,     -- UInt8
    MobilePhone                smallint,     -- UInt8  *
    MobilePhoneModel           text,         -- String *
    Params                     text,         -- String
    IPNetworkID                bigint,       -- UInt32
    TraficSourceID             smallint,     -- Int8   *
    SearchEngineID             int,          -- UInt16 *
    SearchPhrase               text,         -- String *
    AdvEngineID                smallint,     -- UInt8  *
    IsArtifical                smallint,     -- UInt8
    WindowClientWidth          int,          -- UInt16 *
    WindowClientHeight         int,          -- UInt16 *
    ClientTimeZone             smallint,     -- Int16
    ClientEventTime            timestamp,    -- DateTime
    SilverlightVersion1        smallint,     -- UInt8
    SilverlightVersion2        smallint,     -- UInt8
    SilverlightVersion3        bigint,       -- UInt32
    SilverlightVersion4        int,          -- UInt16
    PageCharset                text,         -- String
    CodeVersion                bigint,       -- UInt32
    IsLink                     smallint,     -- UInt8 *
    IsDownload                 smallint,     -- UInt8 *
    IsNotBounce                smallint,     -- UInt8
    FUniqID                    bigint,       -- UInt64    #
    HID                        bigint,       -- UInt32
    IsOldCounter               smallint,     -- UInt8
    IsEvent                    smallint,     -- UInt8
    IsParameter                smallint,     -- UInt8
    DontCountHits              smallint,     -- UInt8 *
    WithHash                   smallint,     -- UInt8
    HitColor                   text,         -- FixedString(1)
    UTCEventTime               timestamp,    -- DateTime
    Age                        smallint,     -- UInt8
    Sex                        smallint,     -- UInt8
    Income                     smallint,     -- UInt8
    Interests                  int,          -- UInt16
    Robotness                  smallint,     -- UInt8
    GeneralInterests           text,         -- Array(UInt16)
    RemoteIP                   bigint,       -- UInt32
    RemoteIP6                  text,         -- FixedString(16)
    WindowName                 int,          -- Int32
    OpenerName                 int,          -- Int32
    HistoryLength              smallint,     -- Int16
    BrowserLanguage            text,         -- FixedString(2)
    BrowserCountry             text,         -- FixedString(2)
    SocialNetwork              text,         -- String
    SocialAction               text,         -- String
    HTTPError                  int,          -- UInt16
    SendTiming                 int,          -- Int32
    DNSTiming                  int,          -- Int32
    ConnectTiming              int,          -- Int32
    ResponseStartTiming        int,          -- Int32
    ResponseEndTiming          int,          -- Int32
    FetchTiming                int,          -- Int32
    RedirectTiming             int,          -- Int32
    DOMInteractiveTiming       int,          -- Int32
    DOMContentLoadedTiming     int,          -- Int32
    DOMCompleteTiming          int,          -- Int32
    LoadEventStartTiming       int,          -- Int32
    LoadEventEndTiming         int,          -- Int32
    NSToDOMContentLoadedTiming int,          -- Int32
    FirstPaintTiming           int,          -- Int32
    RedirectCount              smallint,     -- Int8
    SocialSourceNetworkID      smallint,     -- UInt8
    SocialSourcePage           text,         -- String
    ParamPrice                 bigint,       -- Int64
    ParamOrderID               text,         -- String
    ParamCurrency              text,         -- FixedString(3)
    ParamCurrencyID            int,          -- UInt16
    GoalsReached               text,         -- Array(UInt32)
    OpenstatServiceName        text,         -- String
    OpenstatCampaignID         text,         -- String
    OpenstatAdID               text,         -- String
    OpenstatSourceID           text,         -- String
    UTMSource                  text,         -- String
    UTMMedium                  text,         -- String
    UTMCampaign                text,         -- String
    UTMContent                 text,         -- String
    UTMTerm                    text,         -- String
    FromTag                    text,         -- String
    HasGCLID                   smallint,     -- UInt8
    RefererHash                bigint,       -- UInt64     #
    URLHash                    bigint,       -- Uint64 *   #
    CLID                       bigint,       -- Uint32
    YCLID                      bigint,       -- UInt64     #
    ShareService               text,         -- String
    ShareURL                   text,         -- String
    ShareTitle                 text,         -- String
--     ParsedParams Nested(
         Key1 text, -- String
         Key2 text, -- String
         Key3 text, -- String
         Key4 text, -- String
         Key5 text, -- String
         ValueDouble text, -- Float64
--         ValueDouble Float64),
    IslandID   text,       -- FixedString(16)
    RequestNum bigint,     -- UInt32
    RequestTry smallint    -- UInt8
)
with(appendonly=true, orientation=column)
distributed by (userid);

