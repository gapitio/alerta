
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'history' AND typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())) THEN
        CREATE TYPE history AS (
            id text,
            event text,
            severity text,
            status text,
            value text,
            text text,
            type text,
            update_time timestamp without time zone,
            "user" text,
            timeout integer
        );
    ELSE
        BEGIN
            ALTER TYPE history ADD ATTRIBUTE "user" text CASCADE;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column "user" already exists in history type.';
        END;
        BEGIN
            ALTER TYPE history ADD ATTRIBUTE timeout integer CASCADE;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column "timeout" already exists in history type.';
        END;
    END IF;
END$$;


CREATE TABLE IF NOT EXISTS alerts (
    id text PRIMARY KEY,
    resource text NOT NULL,
    event text NOT NULL,
    environment text,
    severity text,
    correlate text[],
    status text,
    service text[],
    "group" text,
    value text,
    text text,
    tags text[],
    attributes jsonb,
    origin text,
    type text,
    create_time timestamp without time zone,
    timeout integer,
    raw_data text,
    customer text,
    duplicate_count integer,
    repeat boolean,
    previous_severity text,
    trend_indication text,
    receive_time timestamp without time zone,
    last_receive_id text,
    last_receive_time timestamp without time zone,
    history history[]
);

ALTER TABLE alerts ADD COLUMN IF NOT EXISTS update_time timestamp without time zone;


CREATE TABLE IF NOT EXISTS notes (
    id text PRIMARY KEY,
    text text,
    "user" text,
    attributes jsonb,
    type text NOT NULL,
    create_time timestamp without time zone NOT NULL,
    update_time timestamp without time zone,
    alert text,
    customer text
);


CREATE TABLE IF NOT EXISTS blackouts (
    id text PRIMARY KEY,
    priority integer NOT NULL,
    environment text NOT NULL,
    service text[],
    resource text,
    event text,
    "group" text,
    tags text[],
    customer text,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone NOT NULL,
    duration integer
);

ALTER TABLE blackouts
ADD COLUMN IF NOT EXISTS "user" text,
ADD COLUMN IF NOT EXISTS create_time timestamp without time zone,
ADD COLUMN IF NOT EXISTS text text,
ADD COLUMN IF NOT EXISTS origin text;

DROP TABLE IF EXISTS twilio_rules;


CREATE TABLE IF NOT EXISTS notification_channels (
    id text PRIMARY KEY,
    type text NOT NULL,
    api_token text not null,
    api_sid text,
    sender text not null,
    customer text
);

DO $$
BEGIN
    ALTER TABLE notification_channels ADD COLUMN "host" text;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "host" already exists in notification_channels.';
END$$;

DO $$
BEGIN
    ALTER TABLE notification_channels ADD COLUMN "platform_id" text;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "platform_id" already exists in notification_channels.';
END$$;

DO $$
BEGIN
    ALTER TABLE notification_channels ADD COLUMN "platform_partner_id" text;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "platform_partner_id" already exists in notification_channels.';
END$$;

DO $$
BEGIN
    ALTER TABLE notification_channels ADD COLUMN "verify" text;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "verify" already exists in notification_channels.';
END$$;

DO $$
BEGIN
    ALTER TABLE notification_channels ADD COLUMN "bearer" text;
    ALTER TABLE notification_channels ADD COLUMN "bearer_timeout" timestamp without time zone;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "bearer" and "bearer_timeout" already exists in notification_channels.';
END$$;


DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'severity_advanced') AND NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'notification_triggers') THEN
        CREATE TYPE severity_advanced AS (
            "from_" text[],
            "to" text[]
        );
    END IF;
END$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'advanced_tags') THEN
        CREATE TYPE advanced_tags AS (
            "all" text[],
            "any" text[]
        );
    END IF;
END$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'notification_triggers') THEN
        ALTER TYPE severity_advanced RENAME TO notification_triggers;
        ALTER TYPE notification_triggers RENAME ATTRIBUTE "from_" TO from_severity;
        ALTER TYPE notification_triggers RENAME ATTRIBUTE "to" TO to_severity;
        ALTER TYPE notification_triggers ADD ATTRIBUTE "status" text[];
        ALTER TYPE notification_triggers ADD ATTRIBUTE "text" text;
    END IF;
END$$;

CREATE TABLE IF NOT EXISTS escalation_rules (
    id text PRIMARY KEY,
    priority integer NOT NULL,
    environment text NOT NULL,
    "time" interval,
    service text[],
    resource text,
    event text,
    "group" text,
    tags text[],
    customer text,
    "user" text,
    create_time timestamp without time zone,
    start_time time without time zone,
    end_time time without time zone,
    days text[],
    severity text[],
    use_advanced_severity boolean,
    advanced_severity notification_triggers[],
    active boolean
);

DO $$
BEGIN
    ALTER TABLE escalation_rules ADD COLUMN tags_a advanced_tags[];
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "tags_a" already exists in escalation_rules.';
END$$;
DO $$
BEGIN
    UPDATE escalation_rules SET tags_a = array[(tags, '{}')::advanced_tags]::advanced_tags[] where pg_typeof(tags) != pg_typeof(tags_a);
    UPDATE escalation_rules set tags_a = tags::advanced_tags[] where pg_typeof(tags) = pg_typeof(tags_a);
END$$;
DO $$
BEGIN
    ALTER table escalation_rules DROP COLUMN "tags";
    ALTER table escalation_rules RENAME COLUMN tags_a to tags;
END$$;

DO $$
BEGIN
    ALTER TABLE escalation_rules ADD COLUMN excluded_tags advanced_tags[];
    UPDATE escalation_rules set excluded_tags = '{"({},{})"}';
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "excluded_tags" already exists in notification_rules.';
END$$;

DO $$
BEGIN
    ALTER TABLE escalation_rules ADD COLUMN triggers notification_triggers[];
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "triggers" already exists in escalation_rules.';
END$$;

DO $$
BEGIN
    UPDATE escalation_rules set triggers = ARRAY_APPEND(triggers, ('{}',severity,'{}', null)::notification_triggers) where severity != '{}';
    ALTER table escalation_rules DROP COLUMN severity;
EXCEPTION
    WHEN undefined_column THEN RAISE NOTICE 'column "severity" have already been dropped from notification_rules.';
END$$;


CREATE TABLE IF NOT EXISTS delayed_notifications (
    id text PRIMARY KEY,
    alert_id text,
    notification_rule_id text,
    delay_time timestamp without time zone
);

CREATE TABLE IF NOT EXISTS notification_rules (
    id text PRIMARY KEY,
    priority integer NOT NULL,
    environment text NOT NULL,
    service text[],
    resource text,
    event text,
    "group" text,
    tags text[],
    customer text,
    "user" text,
    create_time timestamp without time zone,
    start_time time without time zone,
    end_time time without time zone,
    days text[],
    receivers text[],
    severity text[],
    text text,
    channel_id text not null,
    FOREIGN key (channel_id) references notification_channels(id)
);
DO $$
BEGIN
    ALTER TABLE notification_rules ADD COLUMN tags_a advanced_tags[];
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "tags_a" already exists in notification_rules.';
END$$;
DO $$
BEGIN
    UPDATE notification_rules SET tags_a = array[(tags, '{}')::advanced_tags]::advanced_tags[] where pg_typeof(tags) != pg_typeof(tags_a);
    UPDATE notification_rules set tags_a = tags::advanced_tags[] where pg_typeof(tags) = pg_typeof(tags_a);
END$$;
DO $$
BEGIN
    ALTER table notification_rules DROP COLUMN "tags";
    ALTER table notification_rules RENAME COLUMN tags_a to tags;
END$$;


DO $$
BEGIN
    ALTER TABLE notification_rules ADD COLUMN delay_time interval;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "delay_time" already exists in notification_rules.';
END$$;
DO $$
BEGIN
    ALTER TABLE notification_rules ADD COLUMN use_oncall boolean;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "use_on_call" already exists in notification_rules.';
END$$;
DO $$
BEGIN
    ALTER TABLE notification_rules ADD COLUMN advanced_severity notification_triggers[];
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "advanced_severity" already exists in notification_rules.';
END$$;

DO $$
BEGIN
    UPDATE notification_rules SET advanced_severity = '{}' WHERE use_advanced_severity = FALSE;
EXCEPTION
    WHEN undefined_column THEN RAISE NOTICE 'column use_advanced_severity do no longer exist in notification_rules';
END$$;

DO $$
BEGIN
    ALTER TABLE notification_rules RENAME COLUMN advanced_severity to triggers;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "triggers" already exists in notification_rules.';
END$$;

DO $$
BEGIN
    UPDATE notification_rules set triggers = ARRAY_APPEND(triggers, ('{}',severity,'{}', null)::notification_triggers) where severity != '{}' and "status" = '{}' AND NOT use_advanced_severity;
EXCEPTION
    WHEN undefined_column THEN RAISE NOTICE 'column "severity" have already been dropped from notification_rules.';
END$$;

DO $$
BEGIN
    ALTER TABLE notification_rules ADD COLUMN excluded_tags text[];
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "excluded_tags" already exists in notification_rules.';
END$$;

DO $$
BEGIN
    ALTER TABLE notification_rules ADD COLUMN excluded_tags_a advanced_tags[];
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "excluded_tags_a" already exists in notification_rules.';
END$$;
DO $$
BEGIN
    UPDATE notification_rules SET excluded_tags_a = array[(excluded_tags, '{}')::advanced_tags]::advanced_tags[] where pg_typeof(excluded_tags) != pg_typeof(excluded_tags_a);
    UPDATE notification_rules set excluded_tags_a = excluded_tags::advanced_tags[] where pg_typeof(excluded_tags) = pg_typeof(excluded_tags_a);
END$$;
DO $$
BEGIN
    ALTER table notification_rules DROP COLUMN "excluded_tags";
    ALTER table notification_rules RENAME COLUMN excluded_tags_a to excluded_tags;
END$$;

DO $$
BEGIN
    ALTER TABLE notification_rules ADD COLUMN status text[];
    UPDATE notification_rules SET status = '{}';
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "status" already exists in notification_rules.';
END$$;

DO $$
BEGIN
    UPDATE notification_rules set triggers = ARRAY_APPEND(triggers, ('{}','{}',status, null)::notification_triggers) where "status" != '{}' and severity = '{}' and not use_advanced_severity;
    UPDATE notification_rules set triggers = ARRAY_APPEND(triggers, ('{}', severity, status, null)::notification_triggers) where "status" != '{}' and severity != '{}' and not use_advanced_severity;
    UPDATE notification_rules as re set triggers = n.f from (SELECT "id", array_agg((tr.from_severity, tr.to_severity, b.status, tr.text)::notification_triggers) as f from notification_rules as b, unnest(triggers) as tr GROUP BY id) as n where "status" != '{}' and use_advanced_severity and n.id = re.id;
    ALTER table notification_rules DROP COLUMN "status";
    ALTER table notification_rules DROP COLUMN severity;
EXCEPTION
    WHEN undefined_column THEN RAISE NOTICE 'column "status" and severity have already been dropped from notification_rules.';
END$$;

DO $$
BEGIN
    ALTER table notification_rules DROP COLUMN advanced_severity;
    ALTER table notification_rules DROP COLUMN use_advanced_severity;
EXCEPTION
    WHEN undefined_column THEN RAISE NOTICE 'column advanced_severity and use_advanced_severity have already been dropped from notification_rules.';
END$$;

DO $$
BEGIN
    ALTER TABLE notification_rules ADD COLUMN active boolean;
    UPDATE notification_rules SET active = true;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "active" already exists in notification_rules.';
END$$;

DO $$
BEGIN
    ALTER TABLE notification_rules ADD COLUMN reactivate timestamp without time zone;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "reactivate" already exists in notification_rules.';
END$$;

DO $$
BEGIN
    ALTER TABLE notification_rules ADD COLUMN name text UNIQUE;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "name" already exists in notification_rules.';
END$$;

DO $$
BEGIN
    ALTER TABLE notification_rules ADD COLUMN user_ids text[];
    ALTER TABLE notification_rules ADD COLUMN group_ids text[];
    UPDATE notification_rules SET user_ids = '{}';
    UPDATE notification_rules SET group_ids = '{}';
    ALTER TABLE notification_rules ALTER COLUMN user_ids SET NOT NULL;
    ALTER TABLE notification_rules ALTER COLUMN group_ids SET NOT NULL;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "user_ids" and "gruop_ids" already exists in notification_rules.';
END$$;

DO $$
BEGIN
    UPDATE public.notification_rules SET resource=NULL WHERE resource='';
    UPDATE public.notification_rules SET event=NULL WHERE event='';
    UPDATE public.notification_rules SET "group"=NULL WHERE "group"='';
    UPDATE notification_rules SET excluded_tags='{}' WHERE excluded_tags IS NULL;
    UPDATE notification_rules set triggers = ARRAY[('{}', '{}', '{}', null)::notification_triggers] WHERE triggers = '{}';

END$$;

CREATE TABLE IF NOT EXISTS notification_rules_history (
    id SERIAL PRIMARY KEY,
    rule_id TEXT REFERENCES notification_rules(id) ON DELETE CASCADE,
    "user" TEXT,
    "type" TEXT,
    create_time timestamp without time zone,
    rule_data JSON
);

CREATE INDEX IF NOT EXISTS nrhi
ON notification_rules_history(rule_id);

CREATE TABLE IF NOT EXISTS on_calls(
    id text PRIMARY KEY,
    customer text,
    "user" text,
    user_ids text[] NOT NULL,
    group_ids text[] NOT NULL,
    "start_date" date,
    end_date date,
    start_time time without time zone,
    end_time time without time zone,
	repeat_type text,
	repeat_days text[] CONSTRAINT repeat_days_check CHECK (repeat_days IS NULL or repeat_type = 'list' ),
	repeat_weeks integer[] CONSTRAINT repeat_weeks_check CHECK (repeat_weeks IS NULL or repeat_type = 'list' ),
	repeat_months text[] CONSTRAINT repeat_months_check CHECK (repeat_months IS NULL or repeat_type = 'list' ),
    CONSTRAINT check_user_length CHECK (cardinality(user_ids) > 0 OR cardinality(group_ids) > 0)
);


CREATE TABLE IF NOT EXISTS notification_groups(
    id text PRIMARY KEY,
    name text UNIQUE NOT NULL,
    users text[]
);


ALTER TABLE notification_groups ADD COLUMN IF NOT EXISTS phone_numbers text[];
ALTER TABLE notification_groups ADD COLUMN IF NOT EXISTS mails text[];

UPDATE notification_groups SET phone_numbers='{}' WHERE phone_numbers IS NULL;
UPDATE notification_groups SET mails='{}' WHERE mails IS NULL;

CREATE TABLE IF NOT EXISTS notification_history(
    id text PRIMARY KEY,
    sent bool,
    message text,
    channel text,
    rule text,
    alert text,
    receiver text,
    sender text,
    sent_time timestamp without time zone,
    confirmed boolean,
    confirmed_time timestamp without time zone,
    error text
);


CREATE TABLE IF NOT EXISTS customers (
    id text PRIMARY KEY,
    match text NOT NULL,
    customer text
);

ALTER TABLE customers DROP CONSTRAINT IF EXISTS customers_match_key;


CREATE TABLE IF NOT EXISTS heartbeats (
    id text PRIMARY KEY,
    origin text NOT NULL,
    tags text[],
    type text,
    create_time timestamp without time zone,
    timeout integer,
    receive_time timestamp without time zone,
    customer text
);

ALTER TABLE heartbeats ADD COLUMN IF NOT EXISTS attributes jsonb;


CREATE TABLE IF NOT EXISTS keys (
    id text PRIMARY KEY,
    key text UNIQUE NOT NULL,
    "user" text NOT NULL,
    scopes text[],
    text text,
    expire_time timestamp without time zone,
    count integer,
    last_used_time timestamp without time zone,
    customer text
);


CREATE TABLE IF NOT EXISTS metrics (
    "group" text NOT NULL,
    name text NOT NULL,
    title text,
    description text,
    value integer,
    count integer,
    total_time integer,
    type text NOT NULL,
    CONSTRAINT metrics_pkey PRIMARY KEY ("group", name, type)
);
ALTER TABLE metrics ALTER COLUMN total_time TYPE BIGINT;
ALTER TABLE metrics ALTER COLUMN count TYPE BIGINT;


CREATE TABLE IF NOT EXISTS perms (
    id text PRIMARY KEY,
    match text UNIQUE NOT NULL,
    scopes text[]
);


CREATE TABLE IF NOT EXISTS users (
    id text PRIMARY KEY,
    name text,
    email text UNIQUE,
    password text NOT NULL,
    status text,
    roles text[],
    attributes jsonb,
    create_time timestamp without time zone NOT NULL,
    last_login timestamp without time zone,
    text text,
    update_time timestamp without time zone,
    email_verified boolean,
    hash text
);
ALTER TABLE users ALTER COLUMN email DROP NOT NULL;

DO $$
BEGIN
    ALTER TABLE users ADD COLUMN login text UNIQUE;
    UPDATE users SET login = email;
    ALTER TABLE users ALTER COLUMN login SET NOT NULL;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "login" already exists in users.';
END$$;

DO $$
BEGIN
    ALTER TABLE users ADD COLUMN phone_number text;
    ALTER TABLE users ADD COLUMN country text;
EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column "phone_number" already exists in users.';
END$$;

CREATE TABLE IF NOT EXISTS notification_sends(
    id TEXT PRIMARY KEY,
    user_name TEXT,
    user_email TEXT REFERENCES users(email) ON DELETE CASCADE,
    group_name text REFERENCES notification_groups(name) ON DELETE CASCADE,
    mail BOOLEAN,
    sms BOOLEAN
);

CREATE TABLE IF NOT EXISTS groups (
    id text PRIMARY KEY,
    name text UNIQUE NOT NULL,
    users text[],
    text text,
    tags text[],
    attributes jsonb,
    update_time timestamp without time zone
);


CREATE UNIQUE INDEX IF NOT EXISTS env_res_evt_cust_key ON alerts USING btree (environment, resource, event, (COALESCE(customer, ''::text)));


CREATE UNIQUE INDEX IF NOT EXISTS org_cust_key ON heartbeats USING btree (origin, (COALESCE(customer, ''::text)));
