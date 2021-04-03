set -e  # exit on error

SOURCE_DEFINITION_URL="https://raw.githubusercontent.com/airbytehq/airbyte/master/airbyte-config/init/src/main/resources/seed/source_definitions.yaml"

# pull sources
curl $SOURCE_DEFINITION_URL > saas/airbyte/sources.yaml

# pull all checks
cd saas/airbyte/definitions_sync/
go run .
cd -