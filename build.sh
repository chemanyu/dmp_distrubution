#!/bin/bash

program_version="v2023.05.18"
compiler_version=$(go version)
build_time=$(date)
author=$(whoami)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-X 'main.ProgramVersion=$program_version' -X 'main.CompileVersion=$compiler_version' -X 'main.BuildTime=$build_time' -X 'main.Author=$author'"
