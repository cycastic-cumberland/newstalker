using NewstalkerPostgresETL;
using PostgresDriver;

namespace NewstalkerCore;

internal static class NewstalkerSqlScripts
{
    public const string CreateCoreTables = @"
create table if not exists stalker_logs
(
    id        bigserial
        constraint stalker_logs_pk
            primary key,
    timestamp bigint,
    header    varchar(128),
    message   varchar(256),
    log_type  integer,
    metadata  text
);

create table if not exists news_outlets
(
    base_urls varchar(64) not null
        constraint new_outlets_pk
            primary key,
    name      varchar(32)
);

create index if not exists new_outlets_base_urls_index
    on news_outlets (base_urls);

create table if not exists scrape_results
(
    url           varchar(400) not null
        constraint scrape_result_pk
            primary key,
    outlet_url    varchar(64)
        constraint scrape_result_news_outlets_base_urls_fk
            references news_outlets,
    language      varchar(10),
    title         varchar(200),
    author        varchar(100),
    time_posted   timestamp with time zone,
    original_text text,
    word_count    integer
);

create index if not exists scrape_result_url_index
    on scrape_results (url);

create table if not exists article_tags
(
    tag varchar(100) not null
        constraint article_tags_pk
            primary key
);

create index if not exists article_tags_tag_index
    on article_tags (tag);

create table if not exists tags_used
(
    article_url varchar(400) not null
        constraint tags_used_scrape_results_url_fk
            references scrape_results,
    tag         varchar(100) not null
        constraint tags_used_article_tags_tag_fk
            references article_tags,
    constraint tags_used_pk
        primary key (article_url, tag)
);

create index if not exists tags_used_article_url_index
    on tags_used (article_url);

create index if not exists tags_used_tag_index
    on tags_used (tag);

create table if not exists summarization_results
(
    article_url     varchar(400) not null
        constraint summarization_results_pk
            primary key
        constraint summarization_results_scrape_results_url_fk
            references scrape_results,
    summarized_text text
);

create table if not exists unique_keywords
(
    keyword varchar(100) not null
        constraint unique_keywords_pk
            primary key
);

create table if not exists extracted_keywords
(
    article_url varchar(255) not null
        constraint extracted_keywords_scrape_results_url_fk
            references scrape_results,
    keyword     varchar(100) not null
        constraint extracted_keywords_unique_keywords_keyword_fk
            references unique_keywords,
    relevancy   double precision,
    constraint extracted_keywords_pk
        primary key (article_url, keyword)
);

create table if not exists scrape_sessions
(
    id               serial
        constraint scrape_sessions_pk
            primary key,
    time_initialized timestamp with time zone,
    time_end         timestamp with time zone,
    is_finished      boolean
);
";
    public const string CreateAdministrativeTables = @"
create table if not exists api_keys
(
    api_key char(32) not null
        constraint api_keys_pk
            primary key,
    permission integer
);
";
}

public static class Initializer
{
    private struct OutletStruct
    {
        public string Url;
        public string OutletName;
    }

    public static async Task InitializeCoreTables(PostgresProvider db)
    {
        await db.TryExecute(NewstalkerSqlScripts.CreateCoreTables);
    }
    public static async Task InitializeAdministrativeTables(PostgresProvider db)
    {
        await db.TryExecute(NewstalkerSqlScripts.CreateAdministrativeTables);
    }
    public static async Task InitializeOutlets(PostgresProvider db)
    {
        try
        {
            await db.OpenTransaction(async t =>
            {
                var transaction = t.GetRawTransaction();
                await db.TryExecute("INSERT INTO news_outlets VALUES " +
                                    "(@url, 'Tuổi trẻ') ON CONFLICT DO NOTHING;",
                    new { url = TuoiTreOutlet.BaseUrl }, transaction);
                await db.TryExecute("INSERT INTO news_outlets VALUES " +
                                    "(@url, 'Thanh niên') ON CONFLICT DO NOTHING;",
                    new { url = ThanhNienOutlet.BaseUrl }, transaction);
            });
        }
        catch (Exception)
        {
            // Ignored
        }
    }

    public static async Task<IEnumerable<(string url, string outletName)>> QueryOutletInfo(PostgresProvider db)
    {
        try
        {
            return from val in await db.TryMappedQuery<OutletStruct>(
                    "SELECT base_urls AS Url, name AS OutletName FROM news_outlets;")
                select (val.Url, val.OutletName);
        }
        catch (Exception)
        {
            return ArraySegment<(string url, string outletName)>.Empty;
        }
    }
}