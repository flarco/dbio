set -e

source /__/envkey/dbio/development && ENVKEY=$ENVKEY /__/bin/envkey-source -f --dot-env > .env

set -a && . ./.env && bash scripts/test.sh