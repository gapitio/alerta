from datetime import UTC, datetime

BUILD_NUMBER = 'DEV'
BUILD_DATE = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
BUILD_VCS_NUMBER = 'HEAD'
