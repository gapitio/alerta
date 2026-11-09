# Changelog

This repository is a fork of [alerta/alerta](https://github.com/alerta/alerta),
based on upstream release 9.0.3. The sections below list the changes made in the
Gapit fork, by fork release tag. Releases v3.0.0, v3.1.4 and v3.2.2 were tagged
from side branches; their changes also appear in the mainline releases around them.
Build, CI, style and test-only commits are not listed.

The upstream Alerta changelog follows after the fork releases.

## v4.1.2 (2026-09-25)

### Feat

- Add default sort on id to keep the same order when sorting keys have duplicates (#180)

## v4.1.1 (2026-09-24)

### Feat

- Add support for using API-keys for twilio channels (#179)

## v4.1.0 (2026-09-08)

### Feat

- Add config parameter for oidc ssl verify (#177)
- Add subject field for emails (#176)
- Change notification_sends to use user_id instead of user email (#174)
- Add support for multiple values for attribute columns (#172)
- Only allow alert severities from alarm model (#171)
- Add more sorting for notificationrules (#169)
- Add scopes for alert actions (#168)
- Add close action to alerta_isa_18_2 alarm model (#164)

### Fix

- Auto update old noitification scopes in notification keys (#178)
- Remove lookup of user if email is none in notification-rule (#175)
- Return 404 when receiving unknown id (#173)
- Do not allow negative action timeout (#170)
- Add environment and customer as none wilcards (#167)
- Add wildcards for filter count (#166)
- Add correct count of attributes in filter tab count (#165)
- Rename environments to environment in filtertab count (#163)

## v4.0.1 (2026-04-27)

No user-facing changes (build/CI only).

## v4.0.0 (2026-04-27)

### Feat

- Add filter tabs for alert and history views (#162)
- Remove repeat-type and use and function for all fields (#161)
- Update all fields of duplicate and correlated alerts (#151)
- Add oidc scopes (#159)
- Change timeout to shelved/acked timeouts (#156)
- Remove group, repeat, trendindication and type from alerts (#154)
- Remove unused functions of alarm model (#155)
- Remove correlate of events (#153)
- Update datetime.utcnow to datetime.now(UTC) (#152)

### Fix

- Remove temporary columns when restructuring in schema (#160)
- Remove drop of groups in notification rules (#158)
- Remove drop of unused columns (#157)

## v3.2.2 (2026-04-27)

### Feat

- Remove repeat-type and use and function for all fields

## v3.2.1 (2026-02-02)

No user-facing changes (build/CI only).

## v3.2.0 (2026-02-02)

### Feat

- Add alert_id to keep track of which alert a history belongs to (#150)
- Add bulk action, bulk post alerts, and bulk delete of alerts (#148)
- Add export of manual setup (#147)
- Add config for hiding pages in navigation bar (#144)

### Fix

- Add h"." in history filter for columns appearing in alert and history (#149)
- Select only distinct notification groups (#146)
- Remove notifications without any receivers (#145)

## v3.1.4 (2025-12-04)

### Fix

- Select only distinct notification groups

## v3.1.3 (2025-11-17)

### Fix

- Update email of notification rules/groups when updating user email (#143)

## v3.1.2 (2025-11-12)

### Fix

- Get user_email instead of relying on ID to be correct (#142)

## v3.1.1 (2025-11-10)

No user-facing changes (build/CI only).

## v3.1.0 (2025-11-10)

### Feat

- Change \n with <br> in sendgrid messages (#139)
- Add better filters (#137)
- Heartbeats (#136)
- Add new tagging structure for alerts (#135)
- Add views for getting alerts that will trigger notification from notification rule (#133)

### Fix

- Change getUserById to getUserByEmail in oncall users (#141)
- Change database update query to use usersEmail
- Change users to emails users in backend (#140)
- Remove admin:users from notification rules, groups and oncalls (#138)
- Remove group ids when deleting notification group (#134)

## v3.0.0 (2025-10-31)

### Feat

- Initial 3.0.0 release
- Heartbeats (#136)
- Add new tagging structure for alerts (#135)
- Add views for getting alerts that will trigger notification from notification rule (#133)

### Fix

- Remove group ids when deleting notification group (#134)

### Refactor

- Reverse default sort of severity column

## v2.2.3 (2025-05-27)

### Fix

- Remove null status from alerts table when starting alerta (#132)

## v2.2.2 (2025-05-12)

No user-facing changes (build/CI only).

## v2.2.1 (2025-05-12)

### Fix

- Add update bearer in test notification channel (#131)

## v2.2.0 (2025-05-06)

### Feat

- Add not(!) function for service, tags, roles, scopes and tag filters (#130)

### Fix

- Add query to show alerts history based on environment filter (#128)
- Add auto update of notification_sends when user email or notification group is changed

### Refactor

- Add filter search for each alerts column (#129)
- Add Heartbeats to default of ALLOWED_ENVIRONMENTS setting

## v2.1.0 (2025-03-14)

### Feat

- Add notification rule history (#127)
- Add notification send for sending custom sms and mail to users and notification groups (#125)
- Add notes plugin that removes notes if alarm received normal severity
- Add advanced tags to escalation rules
- Add filter for sent status in notification history
- Add and/all and or/any function for excluded_tags in notification rules
- Add and/all and or/any function for tags in notification rules

### Fix

- Add status check for alert.status when picking trigger text
- Notification plugin chooses correct trigger text for status and severity triggers
- Add email/phone number from users in notification test (#124)
- Add status checks for plugins and alert model
- Add time check for escalation rules
- Change logic of excluded tags to have or logic between entries in the list
- Add and function between all and any fields of groups excluded tags for notification rules
- Add support for adding tags in list instead of using advanced tags
- Make email addresses lower case in notification rule plugin to remove duplicate email addresses
- Add all phone numbers and emails from notification groups
- Add name for subqueries
- Move update of excluded tags to after add of excluded tags
- Add notification history for smtp
- Log error instead of sending requests to channels whose authentication keys fail to decrypt

### Refactor

- Remove print of receivers in notification rule plugin
- Replace sendgrid package with simple requests
- Replace twilio package with simple requests

## v2.0.3 (2024-11-05)

### Fix

- Remove + 'd' from mylink tokens

## v2.0.2 (2024-11-04)

### Feat

- Add phone_numbers and email to notification groups

### Fix

- Add , at end of each line for update of notification_groups
- Initialize notification_groups phone_numbers and mails as empty arrays
- Add error handling with info log for searching
- Set defualt status first time receiveing with normal severity

## v2.0.1 (2024-10-07)

### Fix

- Update mylink bearer 10 min before timeout

## v2.0.0 (2024-09-26)

### Feat

- Add text field for each advaned severity trigger
- Add csv export/download for report of top n offenders, standing and flapping

### Fix

- Remove empty triggers
- Add all checks for advanced_severities to same clause
- Add update of rules with status and use_advanced_severity
- Add check for use_advanced_severity when transfering status and severity
- Remove advanced severity columns
- Add trigger for status and severity
- Make trigger attributes optional when sending in new notification rule with triggers
- Add reset av triggers to own clause to not fail rename of advanced_severity
- Remove advanced_severity when not used in upgrade of db
- Check for null values in triggers when evaluating active notification rules

### Refactor

- Rename advancedseverity to triggers

## v1.6.2 (2024-07-02)

No user-facing changes (build/CI only).

## v1.6.1 (2024-07-02)

### Feat

- Add gapit version to config view
- Add ldap_email_edit to config view
- Add setting for ldap email edit

### Fix

- Remove untested link_mobility notification type

## v1.6.0 (2024-06-12)

### Feat

- Add notification delay

### Fix

- Return count of notification history instead of count of notification group
- Add brackets for on_users set
- Remove null variables from excluded_tags

## v1.5.1 (2024-05-15)

### Feat

- Add excluded tags to notification rules

### Fix

- Show enforced delete scope of alerts
- Change handling of actions in alerta ISA 18 2 alarm model

## v1.5.0 (2024-04-22)

### Feat

- Add notification history

### Fix

- Add notification history to schema

## v1.4.3 (2024-04-16)

### Fix

- Check status without ignoring active

## v1.4.2 (2024-04-16)

### Feat

- Add mylink as notification channel

## v1.4.1 (2024-04-16)

### Feat

- Add searchable fields to notification rules

## v1.4.0 (2024-04-16)

### Feat

- Add reactivate date for notification rules
- Add status as field for notificaon rules
- Add custom name for notification rules
- Seperate notification groups from user contol

### Fix

- Remove empty strings in NR to not make them useless

## v1.3.2 (2024-04-16)

### Feat

- Add better history pagination
- Add setting for max history page size
- Add escalation rules
- Add escalation of alarms
- Add combination of alerta and isa alarmModel
- Add testing of notification channel
- Add user, group in notification rule plugin
- Add users, groups, mute to notification rule
- Add link mobility xml
- Add postgres support for link mobility sms
- Add advanced severity notification rules
- Add smtp notification channel
- Add oncall functionality
- Add support for notification rules in mongodb
- Integrate notification rules and channels
- Add method to change sms/call text in api
- Integrate Twilio rules (model, plugin and API)

### Fix

- Add default inform severity
- Add open and unack actions to alert_isa_18_2
- Add views import for ISA_18_2 alarm model
- Update mongodb backend
- Twilio call missing fernet
- Add mongodb support for getting users
- Make copy of xml instead of mutating
- Add correct return of active state
- Add ssl verify option for link_mobility
- Update of advanced severity
- Add error logging on link_mobility_xml
- Add logging of link_mobility_xml
- Fix wrong host parameter in link_mobility_xml
- Fix wrong Authorization header
- Docker build and running through image
- Serialize platform id correctly
- Fix error when advanced severity is null
- Add encrypted channel login info
- Error in schema.sql
- Remove channel type constrain
- Set max twilio sms length to 1600
- Message support for array and dict selection
- Fix problem with finding active twilio rules

### Refactor

- Rename twilio_rules to notification_rules
- Remove unused code

# Upstream Alerta

## v9.0.1 (2023-06-22)

### Feat

- Add support for ACTION_UNACK to ISA 18.2 alarm model (#1715)
- add db schema (#1839)

### Fix

- `token_endpoint_auth_methods_supported` is optional (#1856)

## v9.0.0 (2023-03-17)

### Feat

- Add support for AuthProxy auth (#1657)
- show/hide API server version info (#1821)
- **plugin**: add alertname in labels when annotations.alertname (#1801)
- Add userAgent to audit log (#1656)

### Fix

- Bulk operations did not work properly (#1825)
- Replace FLASK_ENV with FLASK_DEBUG (#1824)
- Change ORDER BY no-op for Postgres 15 (#1820)
- **security**: do not expose exception errors to end users (#1811)
- **auth**: auth bypass via registering when AUTH_PROVIDER != basic (#1782)
- **plugin**: fixes an issue if the last plugin in the order (#1798)
- Do not 400 error if content type not application/json (#1756)
- Keycloak url base (#1683)
- Audit log should handle empty body on http delete (#1655)

### Refactor

- **db**: Use Psql 9.6 syntax for ADD COLUMN (#1653)

### Perf

- Add load test workflow

## v8.7.0 (2021-12-06)

## v8.6.5 (2021-12-05)

### Feat

- Support all OpenID client_secret_* token endpoint auth methods (#1641)

### Fix

- Switch docker build from alpine to debian buster
- Empty blackout values should be null (#1643)
- Allow unack when status set to ack in plugin (#1642)
- Do not hardcode inform severity (#1640)
- Housekeeping config variable precedence (#1639)
- **deps**: PyMongo 4.0 is currently not supported (#1636)

## v8.6.4 (2021-12-01)

### Feat

- Optionally print warnings if db create fails (#1627)
- Use GitHub teams for role lookup (#1625)

### Fix

- Customer lookup failed when no user email (#1629)
- special handling of sorting by custom attributes (#1628)

## v8.6.3 (2021-11-25)

## v8.6.2 (2021-11-21)

## v8.6.1 (2021-11-20)

### Feat

- add uptime stat to prometheus metrics (#1546)

## v8.6.0 (2021-05-20)

### Refactor

- convert formatted strings to f-strings (more) (#1522)
- convert tests to use f-strings (#1521)
- convert formatted strings to f-strings (#1520)

### Feat

- add escalate severity custom action plugin (#1519)
- Add support for user-defined API key (#1518)
- log dismissing notes to alert history (#1507)
- add default blackout duration to config endpoint (#1506)
- support readonly users (#1505)

### Fix

- change housekeeping delete threshold to seconds (#1508)
- config setting default environment for webhooks (#1510)
- read LDAP_BIND_PASSWORD from environment variable (#1509)

## v8.5.0 (2021-04-18)

### Fix

- **grafana**: ensure tags work on multi timeseries alert (#1450) (#1490)
- add related id to note reponse (#1499)
- **deps**: bump lxml from 4.6.2 to 4.6.3 (#1486)

### Refactor

- create "query builders" for all data models (#1446)

### Feat

- support custom top10 report sizes (#1498)
- add alert origin to blackout options (#1497)
- add support for custom auth scopes (#1479)

## v8.4.1 (2021-02-28)

### Fix

- **deps**: bump multiple package dependencies (#1470)

## v8.4.0 (2021-02-27)

### Fix

- **auth**: HMAC was falling thru to catchall if auth required (#1470)
- **tests**: enforce authentication for forwarder test (#1471)
- **deps**: update PyJWT to v2.0.0 (#1441)
- request filter fixes in logging.py (#1442)

### Refactor

- use more enums and class properties (#1444)

### Feat

- **plugin,webhook**: Add support for custom error responses (#1466)
- add pagination support to collection responses (#1443)
- **plugin**: add timeout policy plugin to enforce ack and shelve timeouts (#1410)

## v8.3.3 (2021-01-06)

### Fix

- move root logger config key back to top level (#1438)

## v8.3.2 (2021-01-02)

### Fix

- log level not set correctly if DEBUG enabled (#1437)
- do not override envvar config for GitLab and Keycloak (#1431)

## v8.3.1 (2020-12-13)

### Fix

- **search**: use phrase token when searching arrays (#1426)
- **tests**: only temporarily modify the os.environ (#1423)
- **build**: run tests against correct branch
- read built-in plugin config from env vars (#1422)

### Refactor

- **config**: simplify config settings from env vars (#1424)

## v8.3.0 (2020-12-12)

### Feat

- add allowed environments to config endpoint (#1421)
- add colors for Ack and Shelved statuses (#1420)
- add syslog logging output format (#1417)

### Fix

- remove redundant logging messages
- use string enumerated types where possible (#1419)
- add "X-Request-ID" as CORS header (#1418)
- add requestId to error responses (#1415)
- **webhook**: set alert status from action, not directly (#1409)
- **mongodb**: optional query for raw_data and history (#1408)
- **postgres**: optional query for raw_data and history (#1407)
- set alert heartbeat timeout from alert timeout

### Perf

- **db**: Do not query for rawData or history if not required
