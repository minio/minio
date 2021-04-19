#!/bin/bash -e
#
#

test_run_dir="$MINT_RUN_CORE_DIR/aws-sdk-php"
$WGET --output-document=- https://getcomposer.org/installer | php -- --install-dir="$test_run_dir"
php "$test_run_dir/composer.phar" --working-dir="$test_run_dir" install
