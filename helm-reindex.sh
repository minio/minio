#!/bin/bash

helm package helm/minio -d helm-releases/

helm repo index --merge index.yaml --url https://charts.min.io .
