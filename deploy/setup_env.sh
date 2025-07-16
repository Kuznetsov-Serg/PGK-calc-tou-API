#!/bin/bash

cp ./deploy/.env.copy .env

PROJECT_VERSION=$(grep version pyproject.toml | head -1 | sed s'/"//g' | awk '{print $3}')
export PROJECT_VERSION

PROJECT_NAME=$(grep name pyproject.toml | head -1 | sed s'/"//g' | awk '{print $3}')
export PROJECT_NAME

sed -i "s/PROJECT_VERSION=.*/PROJECT_VERSION=$PROJECT_VERSION/" .env
sed -i "s/PROJECT_NAME=.*/PROJECT_NAME=$PROJECT_NAME/" .env

sed -i "s/PROJECT_ENVIRONMENT=.*/PROJECT_ENVIRONMENT=$JOB_PROJECT_ENVIRONMENT/" .env

sed -i "s/DB_HOST=.*/DB_HOST=$DB_HOST/" .env
sed -i "s/DB_USER=.*/DB_USER=$DB_USER/" .env
sed -i "s/DB_PASSWORD=.*/DB_PASSWORD=$DB_PASS/" .env
sed -i "s/DB_NAME=.*/DB_NAME=$DB_NAME/" .env

sed -i "s/SENTRY=.*/SENTRY=$SENTRY/" .env
sed -i "s#SENTRY_DSN=.*#SENTRY_DSN=$SENTRY_DSN#" .env # warn: DSN contains //, change delimiter

sed -i "s/REDIS=.*/REDIS=$BOT_NAME/" .env
sed -i "s#REDIS_HOST=.*#REDIS_HOST=$REDIS_HOST#" .env
